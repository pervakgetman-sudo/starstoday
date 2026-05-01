import os
import logging
import sqlite3
import json
import asyncio
from datetime import datetime, date
from typing import Dict, Any, Optional
from contextlib import contextmanager

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from dateutil.parser import parse
import aiohttp
import pytz

# ---------- КОНФИГУРАЦИЯ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ----------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен в переменных окружения")

# Для веб-хука нужно знать PUBLIC_URL
PUBLIC_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://your-app.onrender.com")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{PUBLIC_URL}{WEBHOOK_PATH}"

# Настройка базы данных (Render дает временное дисковое пространство)
DB_PATH = os.environ.get("DATABASE_PATH", "/tmp/astro_bot.db")

# Часовой пояс для рассылки (можно менять)
TIMEZONE = pytz.timezone(os.environ.get("TIMEZONE", "Europe/Moscow"))

# Настройка логирования для Render
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------- БАЗА ДАННЫХ (с контекстным менеджером) ----------
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT,
                birth_date TEXT,
                birth_time TEXT,
                birth_city TEXT,
                birth_lat REAL,
                birth_lon REAL,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                forecast_date TEXT,
                forecast_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, forecast_date)
            )
        """)
        # Индексы для скорости
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON daily_forecasts(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_date ON daily_forecasts(forecast_date)")
    logger.info("База данных инициализирована")

# ---------- FSM СОСТОЯНИЯ ----------
class RegisterStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_birth_date = State()
    waiting_for_birth_time = State()
    waiting_for_birth_city = State()

# ---------- КЛАВИАТУРЫ ----------
main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔮 Прогноз на сегодня")],
        [KeyboardButton(text="📊 Моя натальная карта")],
        [KeyboardButton(text="⚙️ Редактировать данные"), KeyboardButton(text="❌ Отключить рассылку")]
    ],
    resize_keyboard=True
)

# ---------- ГЕОКОДИНГ (через Nominatim - бесплатно) ----------
async def geocode_city(city_name: str) -> tuple:
    """Получение координат по названию города через OpenStreetMap"""
    async with aiohttp.ClientSession() as session:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": city_name,
            "format": "json",
            "limit": 1
        }
        headers = {
            "User-Agent": "AstroBot/1.0 (your-email@example.com)"  # Замените на свой email
        }
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        return float(data[0]['lat']), float(data[0]['lon'])
        except Exception as e:
            logger.error(f"Ошибка геокодинга: {e}")
    # Координаты по умолчанию (Гринвич)
    return 51.5074, -0.1278

# ---------- АСТРОЛОГИЧЕСКАЯ ЛОГИКА (улучшенная заглушка) ----------
def calculate_natal_chart(birth_date: str, birth_time: str, lat: float, lon: float) -> Dict[str, Any]:
    """
    Расчет натальной карты.
    В продакшене замените на pyswisseph или внешний API
    """
    # Имитация расчета на основе даты (для демонстрации)
    from hashlib import md5
    seed = f"{birth_date}{birth_time}{lat}{lon}".encode()
    hash_val = int(md5(seed).hexdigest()[:8], 16)
    
    signs = ["Овен", "Телец", "Близнецы", "Рак", "Лев", "Дева", 
             "Весы", "Скорпион", "Стрелец", "Козерог", "Водолей", "Рыбы"]
    
    planets = ["Солнце", "Луна", "Меркурий", "Венера", "Марс", 
               "Юпитер", "Сатурн", "Уран", "Нептун", "Плутон"]
    
    chart = {}
    for i, planet in enumerate(planets):
        sign_idx = (hash_val + i * 7) % 12
        degree = (hash_val + i * 13) % 30
        chart[planet] = f"{degree}° {signs[sign_idx]}"
    
    # Асцендент
    asc_idx = (hash_val + 31) % 12
    asc_degree = (hash_val + 17) % 30
    chart["Асцендент"] = f"{asc_degree}° {signs[asc_idx]}"
    
    return chart

def get_daily_horoscope(natal: Dict[str, Any], forecast_date: str) -> str:
    """
    Персональный прогноз на основе натальной карты и текущей даты
    """
    # В реальном проекте здесь вычисляются транзиты
    today = datetime.now()
    weekday = today.strftime("%A")
    
    # Используем натальное Солнце для персонализации
    sun_pos = natal.get("Солнце", "0° Знак")
    
    templates = [
        f"✨ Астрологический прогноз на {forecast_date}:\n\n"
        f"Ваше Солнце в {sun_pos} сегодня формирует вдохновляющий аспект. "
        f"День благоприятен для творчества и самовыражения.",
        
        f"🌙 Прогноз на {forecast_date}:\n\n"
        f"Луна сегодня активирует ваш дом карьеры. "
        f"Обратите внимание на профессиональные возможности.",
        
        f"⭐️ Персональный гороскоп на {forecast_date}:\n\n"
        f"Венера в гармонии с вашей натальной картой. "
        f"Удачный день для отношений и финансов."
    ]
    
    import hashlib
    seed = f"{forecast_date}{natal.get('Солнце', '')}"
    idx = int(hashlib.md5(seed.encode()).hexdigest()[:2], 16) % len(templates)
    
    return templates[idx]

def get_natal_text(natal: Dict[str, Any]) -> str:
    """Формирование текста натальной карты"""
    text = "🌟 **Ваша натальная карта**\n\n"
    for planet, pos in list(natal.items())[:5]:
        text += f"• {planet}: {pos}\n"
    text += "\n...\n"
    text += "\n_Для точного расчета требуется профессиональный астрологический движок._"
    return text

# ---------- РАБОТА С БАЗОЙ ДАННЫХ (CRUD) ----------
def save_user(user_id: int, name: str, birth_date: str, birth_time: str, 
              city: str, lat: float, lon: float):
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO users 
            (user_id, full_name, birth_date, birth_time, birth_city, birth_lat, birth_lon, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        """, (user_id, name, birth_date, birth_time, city, lat, lon))

def get_user(user_id: int) -> Optional[Dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return dict(row) if row else None

def get_all_active_users() -> list:
    with get_db() as conn:
        rows = conn.execute("SELECT user_id FROM users WHERE is_active = 1").fetchall()
        return [row['user_id'] for row in rows]

def save_forecast(user_id: int, forecast_date: str, text: str):
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO daily_forecasts (user_id, forecast_date, forecast_text)
            VALUES (?, ?, ?)
        """, (user_id, forecast_date, text))

def get_cached_forecast(user_id: int, forecast_date: str) -> Optional[str]:
    with get_db() as conn:
        row = conn.execute(
            "SELECT forecast_text FROM daily_forecasts WHERE user_id = ? AND forecast_date = ?",
            (user_id, forecast_date)
        ).fetchone()
        return row['forecast_text'] if row else None

def deactivate_user(user_id: int):
    with get_db() as conn:
        conn.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (user_id,))

def activate_user(user_id: int):
    with get_db() as conn:
        conn.execute("UPDATE users SET is_active = 1 WHERE user_id = ?", (user_id,))

# ---------- ОБРАБОТЧИКИ КОМАНД ----------
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if user:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔮 Получить прогноз", callback_data="forecast_today")],
                [InlineKeyboardButton(text="📊 Натальная карта", callback_data="show_natal")],
                [InlineKeyboardButton(text="❌ Отключить рассылку", callback_data="disable_notifications")]
            ]
        )
        await message.answer(
            f"🌟 С возвращением, {user['full_name']}!\n"
            f"Ваши данные сохранены. Выберите действие:",
            reply_markup=keyboard
        )
    else:
        await message.answer(
            "🌟 Добро пожаловать в **Персональный Астрологический Бот**!\n\n"
            "Я создаю уникальные прогнозы на основе вашей натальной карты.\n\n"
            "Давайте начнем регистрацию. Как вас зовут?",
            parse_mode=ParseMode.MARKDOWN
        )
        await state.set_state(RegisterStates.waiting_for_name)

@dp.message(RegisterStates.waiting_for_name)
async def process_name(message: types.Message, state: FSMContext):
    if len(message.text) > 50:
        await message.answer("Имя слишком длинное. Пожалуйста, введите имя короче:")
        return
    await state.update_data(name=message.text)
    await message.answer("📅 Укажите дату рождения в формате **ГГГГ-ММ-ДД**\nПример: 1990-05-15")
    await state.set_state(RegisterStates.waiting_for_birth_date)

@dp.message(RegisterStates.waiting_for_birth_date)
async def process_birth_date(message: types.Message, state: FSMContext):
    try:
        parse(message.text)
        # Проверка, что дата не в будущем
        birth_date_obj = parse(message.text).date()
        if birth_date_obj > date.today():
            await message.answer("Дата рождения не может быть в будущем. Введите корректную дату:")
            return
        await state.update_data(birth_date=message.text)
        await message.answer("⏰ Укажите время рождения в формате **ЧЧ:ММ**\nПример: 14:30")
        await state.set_state(RegisterStates.waiting_for_birth_time)
    except:
        await message.answer("❌ Неверный формат. Используйте ГГГГ-ММ-ДД\nПример: 1990-05-15")

@dp.message(RegisterStates.waiting_for_birth_time)
async def process_birth_time(message: types.Message, state: FSMContext):
    parts = message.text.split(":")
    if len(parts) == 2:
        try:
            hour = int(parts[0])
            minute = int(parts[1])
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                await state.update_data(birth_time=f"{hour:02d}:{minute:02d}:00")
                await message.answer("🌍 Укажите **город рождения**\nПример: Москва, Лондон, Нью-Йорк")
                await state.set_state(RegisterStates.waiting_for_birth_city)
                return
        except:
            pass
    await message.answer("❌ Неверный формат. Используйте ЧЧ:ММ (например, 14:30)")

@dp.message(RegisterStates.waiting_for_birth_city)
async def process_birth_city(message: types.Message, state: FSMContext):
    city = message.text.strip()
    if len(city) < 2:
        await message.answer("Пожалуйста, введите реальное название города:")
        return
    
    # Показываем индикатор загрузки
    status_msg = await message.answer("🔄 Определяю координаты города...")
    
    lat, lon = await geocode_city(city)
    user_data = await state.get_data()
    user_id = message.from_user.id
    
    save_user(
        user_id=user_id,
        name=user_data["name"],
        birth_date=user_data["birth_date"],
        birth_time=user_data["birth_time"],
        city=city,
        lat=lat,
        lon=lon
    )
    
    await status_msg.delete()
    await message.answer(
        f"✅ **Регистрация завершена, {user_data['name']}!**\n\n"
        f"📍 Город: {city}\n"
        f"📅 Дата: {user_data['birth_date']}\n"
        f"⏰ Время: {user_data['birth_time']}\n\n"
        f"Теперь вы можете получать **персональные астрологические прогнозы** каждый день!\n\n"
        f"Используйте кнопки для управления:",
        reply_markup=main_keyboard,
        parse_mode=ParseMode.MARKDOWN
    )
    await state.clear()

@dp.message(F.text == "🔮 Прогноз на сегодня")
async def daily_forecast_button(message: types.Message):
    await send_daily_forecast(message.from_user.id, message)

async def send_daily_forecast(user_id: int, message_obj: Optional[types.Message] = None):
    user = get_user(user_id)
    if not user:
        if message_obj:
            await message_obj.answer("❌ Сначала зарегистрируйтесь: /start")
        return
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Проверяем кеш
    cached = get_cached_forecast(user_id, today_str)
    if cached:
        if message_obj:
            await message_obj.answer(cached)
        return
    
    # Генерируем новый прогноз
    natal = calculate_natal_chart(user['birth_date'], user['birth_time'], 
                                  user['birth_lat'], user['birth_lon'])
    forecast_text = get_daily_horoscope(natal, today_str)
    save_forecast(user_id, today_str, forecast_text)
    
    if message_obj:
        await message_obj.answer(forecast_text)

@dp.message(F.text == "📊 Моя натальная карта")
async def show_natal_button(message: types.Message):
    user = get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Сначала зарегистрируйтесь: /start")
        return
    
    natal = calculate_natal_chart(user['birth_date'], user['birth_time'],
                                  user['birth_lat'], user['birth_lon'])
    text = get_natal_text(natal)
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

@dp.message(F.text == "⚙️ Редактировать данные")
async def edit_data_button(message: types.Message, state: FSMContext):
    await message.answer("Давайте обновим ваши данные. Как вас зовут?")
    await state.set_state(RegisterStates.waiting_for_name)

@dp.message(F.text == "❌ Отключить рассылку")
async def disable_notifications_button(message: types.Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("❌ Вы не зарегистрированы")
        return
    
    deactivate_user(user_id)
    await message.answer(
        "🔕 Ежедневная рассылка прогнозов отключена.\n"
        "Вы по-прежнему можете получать прогноз по кнопке.\n\n"
        "Чтобы включить рассылку снова, отправьте /start"
    )

@dp.callback_query()
async def handle_callback(callback: types.CallbackQuery):
    if callback.data == "forecast_today":
        await send_daily_forecast(callback.from_user.id, callback.message)
        await callback.answer()
    elif callback.data == "show_natal":
        user = get_user(callback.from_user.id)
        if user:
            natal = calculate_natal_chart(user['birth_date'], user['birth_time'],
                                          user['birth_lat'], user['birth_lon'])
            text = get_natal_text(natal)
            await callback.message.answer(text, parse_mode=ParseMode.MARKDOWN)
        else:
            await callback.message.answer("❌ Сначала зарегистрируйтесь: /start")
        await callback.answer()
    elif callback.data == "disable_notifications":
        deactivate_user(callback.from_user.id)
        await callback.message.answer(
            "🔕 Рассылка отключена. Для включения отправьте /start"
        )
        await callback.answer()

# ---------- ФОНОВАЯ ЗАДАЧА ДЛЯ ЕЖЕДНЕВНОЙ РАССЫЛКИ ----------
async def scheduled_forecasts(bot: Bot):
    """Запускается каждый день в указанное время"""
    while True:
        now = datetime.now(TIMEZONE)
        # Запускаем в 8:00 утра по местному времени
        if now.hour == 8 and now.minute == 0:
            logger.info("Запуск ежедневной рассылки прогнозов")
            today_str = now.strftime("%Y-%m-%d")
            users = get_all_active_users()
            
            for user_id in users:
                try:
                    user = get_user(user_id)
                    if user:
                        natal = calculate_natal_chart(user['birth_date'], user['birth_time'],
                                                      user['birth_lat'], user['birth_lon'])
                        forecast_text = get_daily_horoscope(natal, today_str)
                        save_forecast(user_id, today_str, forecast_text)
                        await bot.send_message(
                            user_id,
                            f"🌟 **Ваш ежедневный прогноз на {today_str}** 🌟\n\n{forecast_text}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        await asyncio.sleep(0.5)  # Защита от flood
                except Exception as e:
                    logger.error(f"Ошибка рассылки для {user_id}: {e}")
            
            # Ждем 60 секунд, чтобы не запустить рассылку несколько раз
            await asyncio.sleep(60)
        await asyncio.sleep(30)

# ---------- НАСТРОЙКА ВЕБ-ХУКОВ ----------
async def on_startup(bot: Bot):
    """Действия при запуске бота"""
    await bot.set_webhook(WEBHOOK_URL)
    logger.info(f"Webhook установлен: {WEBHOOK_URL}")
    
    # Запускаем фоновую задачу для рассылки
    asyncio.create_task(scheduled_forecasts(bot))

async def on_shutdown(bot: Bot):
    """Действия при остановке"""
    await bot.delete_webhook()
    logger.info("Webhook удален")

def main():
    """Точка входа для Render"""
    # Инициализируем базу данных
    init_db()
    
    # Создаем бота и диспетчер
    bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
    
    # Настраиваем приложение aiohttp
    app = web.Application()
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    
    # Обработчики событий
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    # Запускаем сервер
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()
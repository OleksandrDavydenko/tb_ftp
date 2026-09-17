"""
Святкова кампанія «FTP × 15»: інтерактивне привітання з 15-річчям компанії.

Користувач отримує перший екран (гіфка) і гортає історію кнопками — кожен
крок редагує те саме повідомлення, щоб не засмічувати чат. На останньому
екрані кнопка «🎉 Привітати FTP» додає привітання в лічильник (таблиця
ftp15_greetings, один голос на telegram_id).

Розсилка: send_message_to_users() — вставляється в планувальник, як update*.py.
Кнопки: handle_ftp15_callback() — підключено в handle_callback_query (telegrambot.py).

ТЕСТУВАННЯ: поки TEST_MODE=True розсилка йде лише на TEST_TELEGRAM_IDS.
Щоб увімкнути для всіх активних — постав TEST_MODE = False.
"""

import os
import logging
import asyncio
from pathlib import Path

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaAnimation, InputMediaPhoto
from telegram.error import BadRequest

from db import get_active_users, get_db_connection
from utils.blocking import run_blocking

TEST_MODE = True
TEST_TELEGRAM_IDS = [203148640, 142311296]

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# callback_data: "ftp15:step:<номер кроку>" та "ftp15:greet"
CALLBACK_PREFIX = "ftp15"

START_BUTTON_TEXT = "👀 Подивитися"
NEXT_BUTTON_TEXT = "Далі →"
BACK_BUTTON_TEXT = "← Назад"
GREET_BUTTON_TEXT = "🎉 Привітати FTP"

# Пауза між отримувачами: ліміт Telegram ≈30 повідомлень/с
SEND_PAUSE_SECONDS = 0.05

IMAGES_DIR = Path(__file__).resolve().parent / "images"

STEPS = {
    1: {
        "file": "ftp_celebration.gif",
        "caption": (
            "🎂 <b>Нам 15!</b>\n\n"
            "15 років FTP — це не просто дата. Це тисячі робочих днів, рішень, "
            "зустрічей, цифр, дзвінків і повідомлень.\n\n"
            "І ми подумали: а що, як подивитися на один рік життя компанії очима нашого бота?\n\n"
            "Натискайте — покажемо 👇"
        ),
    },
    2: {
        "file": "screen_2_actions.png",
        "caption": (
            "Почнемо з простого.\n"
            "За один рік бот отримав…\n\n"
            "<b>39 660 дій</b>\n\n"
            "Відкривали функції, перевіряли інформацію, шукали потрібне, поверталися знову. "
            "Це все — робоче життя одного бота."
        ),
    },
    3: {
        "file": "screen_3_users.png",
        "caption": (
            "<b>Ці цифри — не про бота. Вони про людей.</b>\n\n"
            "<b>166</b> активних користувачів, із них <b>164</b> реально взаємодіяли "
            "з ботом протягом року.\n\n"
            "Це вже не «тестовий бот». Це інструмент, яким реально користуються в роботі."
        ),
    },
    4: {
        "file": "screen_4_functions.png",
        "caption": (
            "Найчастіше шукали зовсім не щось екзотичне:\n\n"
            "Оклад — <b>2 291</b>\n"
            "Відомість бонусів — <b>1 823</b>\n"
            "Курс валют — <b>1 578</b>\n"
            "Залишки відпусток — <b>1 276</b>\n"
            "Бонуси — <b>1 203</b>\n\n"
            "Коли потрібна конкретна відповідь — хочеться отримати її одразу."
        ),
    },
    5: {
        "file": "screen_5_salary.png",
        "caption": (
            "А потім ми знайшли цікаву закономірність.\n\n"
            "У бота є <b>зарплатний сезон</b> 😄\n\n"
            "12–15 числа — <b>≈31%</b> усіх дій із зарплатними функціями.\n\n"
            "Саме тоді настає справжній ажіотаж."
        ),
    },
    6: {
        "file": "screen_6_night.png",
        "caption": (
            "Але є дещо цікавіше.\n\n"
            "Хтось користується ботом навіть уночі:\n"
            "00:00 – 05:59 → <b>1 657 дій</b>.\n\n"
            "Бот не має робочого часу. Він працює <b>24/7</b>. "
            "А максимум за один день — <b>800 дій</b>."
        ),
    },
    7: {
        "file": "screen_7_ai.png",
        "caption": (
            "І так, за останній рік бот став трохи розумнішим 🤖\n\n"
            "<b>370</b> запитів до ШІ, і <b>132</b> користувачі вже скористалися ШІ-асистентом.\n\n"
            "Бот поступово рухається від «де це знайти?» до «допоможи мені розібратися»."
        ),
    },
    8: {
        "file": "screen_8_recap.png",
        "caption": (
            "<b>Один рік:</b>\n"
            "39 660 дій · 164 активних користувачі · 360 активних днів · 24/7 доступність.\n\n"
            "Але вся ця статистика — лише маленький фрагмент значно більшої історії.\n\n"
            "<b>FTP вже 15 років.</b> І найцікавіше те, що ця історія продовжується щодня."
        ),
    },
    9: {
        "file": "screen_9_final.png",
        "caption": (
            "❤️ Дякуємо, що ти — частина цієї історії.\n\n"
            "15 років FTP — це не просто 15 років на календарі. Це люди. Команди. Робота. "
            "Рішення. І тисячі маленьких дій, з яких щодня складається щось велике.\n\n"
            "🎂 <b>З 15-річчям, FTP!</b>"
        ),
    },
}

FIRST_STEP = min(STEPS)
LAST_STEP = max(STEPS)

# Після першого завантаження файлу Telegram повертає file_id — далі шлемо
# за ним, без повторного аплоуду картинки кожному користувачу
_file_ids: dict[int, str] = {}


def _is_animation(step: int) -> bool:
    return STEPS[step]["file"].lower().endswith(".gif")


def _media_source(step: int):
    return _file_ids.get(step) or IMAGES_DIR / STEPS[step]["file"]


def _remember_file_id(step: int, message) -> None:
    # edit_message_media повертає True замість Message для inline-повідомлень
    if not hasattr(message, "photo"):
        return
    if message.animation:
        _file_ids[step] = message.animation.file_id
    elif message.photo:
        _file_ids[step] = message.photo[-1].file_id


def build_keyboard(step: int) -> InlineKeyboardMarkup:
    row = []
    if step > FIRST_STEP:
        row.append(InlineKeyboardButton(BACK_BUTTON_TEXT, callback_data=f"{CALLBACK_PREFIX}:step:{step - 1}"))
    if step < LAST_STEP:
        text = START_BUTTON_TEXT if step == FIRST_STEP else NEXT_BUTTON_TEXT
        row.append(InlineKeyboardButton(text, callback_data=f"{CALLBACK_PREFIX}:step:{step + 1}"))
    else:
        row.append(InlineKeyboardButton(GREET_BUTTON_TEXT, callback_data=f"{CALLBACK_PREFIX}:greet"))
    return InlineKeyboardMarkup([row])


async def send_step(target_bot: Bot, chat_id: int, step: int):
    """Надсилає крок новим повідомленням."""
    caption = STEPS[step]["caption"]
    if _is_animation(step):
        message = await target_bot.send_animation(
            chat_id=chat_id, animation=_media_source(step), caption=caption,
            parse_mode='HTML', reply_markup=build_keyboard(step)
        )
    else:
        message = await target_bot.send_photo(
            chat_id=chat_id, photo=_media_source(step), caption=caption,
            parse_mode='HTML', reply_markup=build_keyboard(step)
        )
    _remember_file_id(step, message)
    return message


# ──────────────────────────────────────────────────────────────────────────────
# Розсилка
# ──────────────────────────────────────────────────────────────────────────────

def send_message_to_users():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_send_message_to_users())

async def async_send_message_to_users():
    """ Відправляє перший екран кампанії «FTP × 15» (тестовим або всім активним користувачам). """
    # Без будь-якої картинки історія обірветься посередині — краще не слати нічого
    missing = [data["file"] for data in STEPS.values() if not (IMAGES_DIR / data["file"]).is_file()]
    if missing:
        logging.error(f"❌ FTP15: розсилку скасовано, бракує файлів у {IMAGES_DIR}: {', '.join(missing)}")
        return

    if TEST_MODE:
        users = [{'telegram_id': telegram_id, 'employee_name': 'тест'} for telegram_id in TEST_TELEGRAM_IDS]
    else:
        users = get_active_users()

    for user in users:
        telegram_id = user.get('telegram_id')
        employee_name = user.get('employee_name')
        if telegram_id:
            try:
                await send_step(bot, telegram_id, FIRST_STEP)
                logging.info(f"✅ FTP15: повідомлення відправлено: {employee_name} (Telegram ID: {telegram_id})")
            except Exception as e:
                logging.error(f"❌ FTP15: помилка при відправці повідомлення {employee_name}: {e}")
            await asyncio.sleep(SEND_PAUSE_SECONDS)
        else:
            logging.warning(f"⚠️ FTP15: відсутній Telegram ID для користувача: {employee_name}")


# ──────────────────────────────────────────────────────────────────────────────
# Кнопки
# ──────────────────────────────────────────────────────────────────────────────

async def handle_ftp15_callback(update, context, value: str) -> None:
    """value — частина callback_data після "ftp15:" ("step:N" або "greet")."""
    if value == "greet":
        await _greet(update, context)
        return

    action, _, raw_step = value.partition(":")
    if action != "step" or not raw_step.isdigit() or int(raw_step) not in STEPS:
        logging.warning(f"[ftp15] невідомий callback: {value}")
        return
    step = int(raw_step)

    query = update.callback_query
    media_class = InputMediaAnimation if _is_animation(step) else InputMediaPhoto
    media = media_class(media=_media_source(step), caption=STEPS[step]["caption"], parse_mode='HTML')
    try:
        message = await query.edit_message_media(media=media, reply_markup=build_keyboard(step))
        _remember_file_id(step, message)
    except BadRequest as e:
        # Подвійне натискання тієї самої кнопки — повідомлення вже на цьому кроці
        if "not modified" in str(e).lower():
            return
        logging.warning(f"[ftp15] не вдалося перегорнути на крок {step}: {e} — надсилаємо новим повідомленням")
        _file_ids.pop(step, None)
        await send_step(context.bot, update.effective_chat.id, step)


async def _greet(update, context) -> None:
    query = update.callback_query
    user = update.effective_user
    chat_id = update.effective_chat.id
    employee_name = context.user_data.get('employee_name') or user.full_name

    try:
        is_new, total = await run_blocking(add_ftp15_greeting, user.id, employee_name)
    except Exception as e:
        # Кнопку не прибираємо — хай можна буде натиснути ще раз
        logging.error(f"❌ FTP15: не вдалося зберегти привітання {employee_name}: {e}")
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ Не вдалося зберегти привітання. Спробуй, будь ласка, ще раз за хвилину."
        )
        return

    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except BadRequest as e:
        logging.warning(f"[ftp15] не вдалося прибрати кнопки: {e}")

    if is_new:
        logging.info(f"🎉 FTP15: нове привітання від {employee_name} (усього {total})")
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🎉 Дякуємо! Твоє привітання вже з нами ❤️\n\nПривітань уже: <b>{total}</b>",
            parse_mode='HTML'
        )
        # Одиночний емодзі Telegram показує анімованим — це й буде конфеті
        await context.bot.send_message(chat_id=chat_id, text="🎉")
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Твоє привітання вже враховане ❤️\n\nПривітань уже: <b>{total}</b>",
            parse_mode='HTML'
        )


# ──────────────────────────────────────────────────────────────────────────────
# Лічильник привітань
# ──────────────────────────────────────────────────────────────────────────────

def add_ftp15_greeting(telegram_id: int, employee_name: str) -> tuple[bool, int]:
    """Зараховує привітання (одне на telegram_id). Повертає (чи нове, скільки всього)."""
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS ftp15_greetings (
                telegram_id   BIGINT PRIMARY KEY,
                employee_name TEXT,
                greeted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            cursor.execute(
                "INSERT INTO ftp15_greetings (telegram_id, employee_name) VALUES (%s, %s) "
                "ON CONFLICT (telegram_id) DO NOTHING",
                (telegram_id, employee_name)
            )
            is_new = cursor.rowcount == 1
            cursor.execute("SELECT COUNT(*) FROM ftp15_greetings")
            total = cursor.fetchone()[0]
        return is_new, total
    finally:
        conn.close()

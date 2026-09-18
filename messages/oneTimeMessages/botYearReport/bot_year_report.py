"""
Звіт про перший рік роботи бота: 1 вересня боту виповнився рік у компанії.

Користувач отримує перший екран і гортає статистику кнопками — кожен крок
редагує те саме повідомлення, щоб не засмічувати чат.

Розсилка: send_message_to_users() — вставляється в планувальник, як update*.py.
Кнопки: handle_ftp15_callback() — підключено в handle_callback_query (telegrambot.py).

ТЕСТУВАННЯ: поки TEST_MODE=True розсилка йде лише на TEST_TELEGRAM_IDS.
Щоб увімкнути для всіх активних — постав TEST_MODE = False.
"""

import os
import logging
import asyncio
from pathlib import Path

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.error import BadRequest

from db import get_active_users

TEST_MODE = True
TEST_TELEGRAM_IDS = [203148640] #225659191

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# callback_data: "ftp15:step:<номер кроку>"
CALLBACK_PREFIX = "ftp15"

NEXT_BUTTON_TEXT = "Далі →"
BACK_BUTTON_TEXT = "← Назад"

# Пауза між отримувачами: ліміт Telegram ≈30 повідомлень/с
SEND_PAUSE_SECONDS = 0.05

IMAGES_DIR = Path(__file__).resolve().parent / "images"

STEPS = {
    # Уся статистика вже намальована на самих картинках — дублювати її підписом
    # не треба. Підписи лишаються тільки там, де несуть НОВИЙ зміст: крок 1
    # (привід і нагадування функцій) і крок 7 (куди писати ідеї).
    1: {
        "file": "screen_2_actions.png",
        "caption": (
            "🎉 <b>Боту — рік!</b>\n\n"
            "1 вересня виповнився рік, як бот працює в компанії. Зібрали короткий "
            "звіт — що з ним відбувалося за цей час.\n\n"
            "Нагадаємо, чим він може бути корисним просто зараз:\n\n"
            "💼 /salary — розрахунковий лист, оклад і бонуси\n"
            "📊 /analytics — аналітика по угодах і прибутку\n"
            "📉 /debt — дебіторська заборгованість\n"
            "🧾 /hr — відпустки та відпрацьовані дні\n"
            "💱 Курс валют — оновлюється щодня\n"
            "🤖 Просто напишіть запитання в чат — відповість AI"
        ),
    },
    2: {"file": "screen_3_users.png", "caption": None},
    3: {"file": "screen_4_functions.png", "caption": None},
    4: {"file": "screen_5_salary.png", "caption": None},
    5: {"file": "screen_6_night.png", "caption": None},
    6: {"file": "screen_7_ai.png", "caption": None},
    7: {
        "file": "screen_8_recap.png",
        "caption": (
            "💡 Є ідея, як зробити бота зручнішим? Напишіть на <b>od@ftpua.com</b> — "
            "за ідею, яку візьмемо в роботу, діє винагорода <b>500 грн</b>.\n\n"
            "Деталі — у розділі «ℹ️ Інформація» → «💡 Нові ідеї»."
        ),
    },
}

FIRST_STEP = min(STEPS)
LAST_STEP = max(STEPS)

# Після першого завантаження файлу Telegram повертає file_id — далі шлемо
# за ним, без повторного аплоуду картинки кожному користувачу
_file_ids: dict[int, str] = {}


def _media_source(step: int):
    return _file_ids.get(step) or IMAGES_DIR / STEPS[step]["file"]


def _open_local_file(step: int):
    """Відкриває картинку кроку в бінарному режимі.

    ВАЖЛИВО: для InputMediaPhoto (edit_message_media) не можна передавати шлях
    (str/Path) так, як це без проблем роблять send_photo/send_animation. PTB для
    InputMedia* завжди викликає parse_file_input з local_mode=True (бо не знає
    реальних налаштувань бота) — а це означає, що звичайний Path перетворюється
    на URI виду "file:///...". Такий URI приймає лише локально розгорнутий
    Bot API сервер (--local), а не api.telegram.org, тому Telegram у відповідь
    на editMessageMedia повертав 400 "Invalid file http url specified:
    unsupported url protocol". Відкритий файловий об'єкт PTB натомість загортає
    в InputFile і аплоадить мультипартом — так само, як у send_step().
    """
    return open(IMAGES_DIR / STEPS[step]["file"], "rb")


def _remember_file_id(step: int, message) -> None:
    # edit_message_media повертає True замість Message для inline-повідомлень
    if not hasattr(message, "photo"):
        return
    if message.photo:
        _file_ids[step] = message.photo[-1].file_id


def build_keyboard(step: int) -> InlineKeyboardMarkup:
    row = []
    if step > FIRST_STEP:
        row.append(InlineKeyboardButton(BACK_BUTTON_TEXT, callback_data=f"{CALLBACK_PREFIX}:step:{step - 1}"))
    if step < LAST_STEP:
        row.append(InlineKeyboardButton(NEXT_BUTTON_TEXT, callback_data=f"{CALLBACK_PREFIX}:step:{step + 1}"))
    return InlineKeyboardMarkup([row])


async def send_step(target_bot: Bot, chat_id: int, step: int, keyboard: InlineKeyboardMarkup | None = None):
    """Надсилає крок новим повідомленням."""
    message = await target_bot.send_photo(
        chat_id=chat_id, photo=_media_source(step), caption=STEPS[step]["caption"],
        parse_mode='HTML', reply_markup=keyboard if keyboard is not None else build_keyboard(step)
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
    """ Відправляє перший екран звіту (тестовим або всім активним користувачам). """
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
    """value — частина callback_data після "ftp15:" (формат "step:N")."""
    action, _, raw_step = value.partition(":")
    if action != "step" or not raw_step.isdigit() or int(raw_step) not in STEPS:
        logging.warning(f"[ftp15] невідомий callback: {value}")
        return
    step = int(raw_step)

    query = update.callback_query
    keyboard = build_keyboard(step)

    cached_file_id = _file_ids.get(step)
    file_handle = None if cached_file_id else _open_local_file(step)
    try:
        media = InputMediaPhoto(
            media=cached_file_id or file_handle, caption=STEPS[step]["caption"], parse_mode='HTML'
        )
        try:
            message = await query.edit_message_media(media=media, reply_markup=keyboard)
            _remember_file_id(step, message)
        except BadRequest as e:
            # Подвійне натискання тієї самої кнопки — повідомлення вже на цьому кроці
            if "not modified" in str(e).lower():
                return
            logging.warning(f"[ftp15] не вдалося перегорнути на крок {step}: {e} — надсилаємо новим повідомленням")
            _file_ids.pop(step, None)
            await send_step(context.bot, update.effective_chat.id, step, keyboard=keyboard)
    finally:
        if file_handle:
            file_handle.close()

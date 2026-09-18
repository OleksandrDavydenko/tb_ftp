"""
Звіт про перший рік роботи FTPFinanceBot: 1 вересня йому виповнився рік у компанії.

Користувач отримує перший екран і гортає статистику кнопками — кожен крок
редагує те саме повідомлення, щоб не засмічувати чат.

Розсилка: send_message_to_users() — вставляється в планувальник, як update*.py.
Кнопки: handle_bot_year_callback() — підключено в handle_callback_query
(telegrambot.py); цей імпорт має бути активним завжди, бо кнопки на вже
надісланих повідомленнях можуть натиснути будь-коли після розсилки.

ТЕСТУВАННЯ: поки TEST_MODE=True розсилка йде лише на TEST_TELEGRAM_IDS.
Щоб увімкнути для всіх активних — постав TEST_MODE = False.
"""

import os
import logging
import asyncio
from pathlib import Path

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaAnimation, InputMediaPhoto
from telegram.error import BadRequest

from db import get_active_users

TEST_MODE = True
TEST_TELEGRAM_IDS = [203148640] #225659191

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# callback_data: "botyear:step:<номер кроку>"
CALLBACK_PREFIX = "botyear"

START_BUTTON_TEXT = "📊 Дивитися звіт"
NEXT_BUTTON_TEXT = "Далі →"
BACK_BUTTON_TEXT = "← Назад"

# Пауза між отримувачами: ліміт Telegram ≈30 повідомлень/с
SEND_PAUSE_SECONDS = 0.05

IMAGES_DIR = Path(__file__).resolve().parent / "images"

STEPS = {
    # Уся статистика вже намальована на самих картинках — дублювати її підписом
    # не треба. Підписи лишаються тільки там, де несуть НОВИЙ зміст: крок 1
    # (привід і річниця бота), крок 2 (нагадування функцій) і крок 8
    # (куди писати ідеї). Гіфка з ребрендингом йде в кінці останнім слайдом.
    1: {"file": "screen_2_intro.png",         
        "caption": (
            "🎉 <b>FTPFinanceBot — рік у роботі!</b>\n\n"
            "1 вересня виповнився рік, як FTPFinanceBot працює в компанії. "
            "Ми підбиваємо підсумки цього чудового року і радіємо новому етапу розвитку. "
            "Зібрали короткий звіт — що змінилося, чого досягли і що чекає попереду."

        ),},
    2: {
        "file": "screen_3_actions.png",
        "caption": (
            "Нагадаємо, чим FTPFinanceBot може бути корисним просто зараз:\n\n"
            "💼 /salary — розрахунковий лист, оклад і бонуси\n"
            "📊 /analytics — аналітика по угодах і прибутку\n"
            "📉 /debt — дебіторська заборгованість\n"
            "🧾 /hr — відпустки та відпрацьовані дні\n"
            "💱 Курс валют — оновлюється щодня\n"
            "🤖 Просто напишіть запитання в чат — відповість AI"
        ),
    },
    3: {"file": "screen_4_users.png", "caption": None},
    4: {"file": "screen_5_functions.png", "caption": None},
    5: {"file": "screen_6_salary.png", "caption": None},
    6: {"file": "screen_7_night.png", "caption": None},
    7: {"file": "screen_8_ai.png", "caption": None},
    8: {
        "file": "screen_9_recap.png",
        "caption": (
            "💡 Є ідея, як зробити FTPFinanceBot зручнішим? Напишіть на <b>od@ftpua.com</b> — "
            "за ідею, яку візьмемо в роботу, діє винагорода <b>500 грн</b>.\n\n"
            "Деталі — у розділі «ℹ️ Інформація» → «💡 Нові ідеї»."
        ),
    },
    9: {
        "file": "screen_1_emblem.gif",
        "caption": (
            "З цієї нагоди ми зробили ребрендинг — оновили візуальну айдентику бота "
            "на знак річниці, розвитку і нового етапу в житті продукту.\n\n"
            "Дякуємо всім, хто користується FTPFinanceBot, надсилає ідеї, підтримує та допомагає "
            "робити його ще кориснішим і зручнішим. Тут і далі — більше інструментів, більше швидкості "
            "і ще більше цінності для команди."
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


def _open_local_file(step: int):
    """Відкриває картинку кроку в бінарному режимі.

    ВАЖЛИВО: для InputMediaPhoto/InputMediaAnimation (edit_message_media) не можна передавати шлях
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
    return InlineKeyboardMarkup([row])


async def send_step(target_bot: Bot, chat_id: int, step: int, keyboard: InlineKeyboardMarkup | None = None):
    """Надсилає крок новим повідомленням."""
    caption = STEPS[step]["caption"]
    reply_markup = keyboard if keyboard is not None else build_keyboard(step)
    if _is_animation(step):
        message = await target_bot.send_animation(
            chat_id=chat_id, animation=_media_source(step), caption=caption,
            parse_mode='HTML', reply_markup=reply_markup
        )
    else:
        message = await target_bot.send_photo(
            chat_id=chat_id, photo=_media_source(step), caption=caption,
            parse_mode='HTML', reply_markup=reply_markup
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
        logging.error(f"❌ Звіт про рік бота: розсилку скасовано, бракує файлів у {IMAGES_DIR}: {', '.join(missing)}")
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
                logging.info(f"✅ Звіт про рік бота: повідомлення відправлено: {employee_name} (Telegram ID: {telegram_id})")
            except Exception as e:
                logging.error(f"❌ Звіт про рік бота: помилка при відправці повідомлення {employee_name}: {e}")
            await asyncio.sleep(SEND_PAUSE_SECONDS)
        else:
            logging.warning(f"⚠️ Звіт про рік бота: відсутній Telegram ID для користувача: {employee_name}")


# ──────────────────────────────────────────────────────────────────────────────
# Кнопки
# ──────────────────────────────────────────────────────────────────────────────

async def handle_bot_year_callback(update, context, value: str) -> None:
    """value — частина callback_data після "botyear:" (формат "step:N")."""
    action, _, raw_step = value.partition(":")
    if action != "step" or not raw_step.isdigit() or int(raw_step) not in STEPS:
        logging.warning(f"[botyear] невідомий callback: {value}")
        return
    step = int(raw_step)

    query = update.callback_query
    keyboard = build_keyboard(step)
    media_class = InputMediaAnimation if _is_animation(step) else InputMediaPhoto

    cached_file_id = _file_ids.get(step)
    file_handle = None if cached_file_id else _open_local_file(step)
    try:
        media = media_class(
            media=cached_file_id or file_handle, caption=STEPS[step]["caption"], parse_mode='HTML'
        )
        try:
            message = await query.edit_message_media(media=media, reply_markup=keyboard)
            _remember_file_id(step, message)
        except BadRequest as e:
            # Подвійне натискання тієї самої кнопки — повідомлення вже на цьому кроці
            if "not modified" in str(e).lower():
                return
            logging.warning(f"[botyear] не вдалося перегорнути на крок {step}: {e} — надсилаємо новим повідомленням")
            _file_ids.pop(step, None)
            await send_step(context.bot, update.effective_chat.id, step, keyboard=keyboard)
    finally:
        if file_handle:
            file_handle.close()

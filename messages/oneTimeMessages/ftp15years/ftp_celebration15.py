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
TEST_TELEGRAM_IDS = [203148640, 225659191]

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
    # Текст із розділу 4 ТЗ уже намальований на самих картинках (заголовок,
    # цифри, підпис) — дублювати його підписом під фото не треба, інфографіки
    # досить. Підписи лишаються там, де несуть НОВИЙ зміст, якого немає на
    # картинці: крок 1 (на гіфці взагалі немає тексту), крок 2 (короткий
    # список функцій — не статистика, тож картинку не дублює), крок 9
    # (слово фінансового відділу) і крок 10 (тепле привітання та побажання —
    # доповнює емоційний фінал, а не повторює текст із самої картинки).
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
            "Нагадаємо, чим бот може бути корисним просто зараз:\n\n"
            "💼 /salary — розрахунковий лист, оклад і бонуси\n"
            "📊 /analytics — аналітика по угодах і прибутку\n"
            "📉 /debt — дебіторська заборгованість\n"
            "🧾 /hr — відпустки та відпрацьовані дні\n"
            "💱 Курс валют — оновлюється щодня\n"
            "🤖 Просто напишіть запитання в чат — відповість AI"
        ),
    },
    3: {"file": "screen_3_users.png", "caption": None},
    4: {"file": "screen_4_functions.png", "caption": None},
    5: {"file": "screen_5_salary.png", "caption": None},
    6: {"file": "screen_6_night.png", "caption": None},
    7: {"file": "screen_7_ai.png", "caption": None},
    8: {"file": "screen_8_recap.png", "caption": None},
    # Передостанній кадр: від статистики бота переходимо до планів компанії.
    # Без картинки — лише текст ("file": None), тож це звичайне текстове
    # повідомлення, а не підпис під фото.
    9: {
        "file": None,
        "caption": (
            "💼 <b>Кілька слів від фінансового відділу</b>\n\n"
            "За 15 років FTP виросла в потужну компанію — і попереду в нас багато нових проєктів. "
            "Ось що вже відбувається:\n\n"
            "📊 <b>Бюджетування.</b> З 2027 року запускаємо бюджетування всіх витрат підприємства — "
            "для повного контролю над видатками.\n"
            "🔍 <b>Аудит і МСФЗ.</b> Проходимо аудит і змінюємо облік, щоб управлінська звітність "
            "відповідала міжнародним стандартам.\n"
            "📈 <b>Аналітика.</b> Розробляємо сервіси на базі OLAP: фінансові показники стануть "
            "максимально прозорими — з чітким розмежуванням прав доступу до закритої інформації.\n\n"
            "Маємо чіткий план і впевнено рухаємося вперед 🚀\n\n"
            "🙏 Дякуємо за продуктивну співпрацю з нашим відділом, достовірні дані в облікових "
            "програмах і вашу довіру.\n\n"
            "Бажаємо професійного зростання й великих досягнень у команді людей, які по-справжньому "
            "люблять свою справу. Наша робота важлива не лише для FTP, а й для України 🇺🇦 — особливо зараз."
        ),
    },
    10: {
        "file": "screen_9_final.png",
        "caption": (
            "Хай наступні роки принесуть FTP ще більше сміливих ідей, вдалих угод "
            "і надійних людей поруч.\n\n"
            "Нехай компанія й далі впевнено росте, надихає та рухається вперед — "
            "разом із кожним із вас. 🚀💙\n\n"
            "💡 І окремо дякуємо за кожну вашу ідею: за розвиток бота відповідає "
            "кожен із нас, тож ми завжди раді новим пропозиціям."
        ),
    },
}

FIRST_STEP = min(STEPS)
LAST_STEP = max(STEPS)

# Після першого завантаження файлу Telegram повертає file_id — далі шлемо
# за ним, без повторного аплоуду картинки кожному користувачу
_file_ids: dict[int, str] = {}


def _is_text(step: int) -> bool:
    return STEPS[step]["file"] is None


def _is_animation(step: int) -> bool:
    return not _is_text(step) and STEPS[step]["file"].lower().endswith(".gif")


def _media_source(step: int):
    return _file_ids.get(step) or IMAGES_DIR / STEPS[step]["file"]


def _open_local_file(step: int):
    """Відкриває картинку кроку в бінарному режимі.

    ВАЖЛИВО: для InputMediaPhoto/InputMediaAnimation (edit_message_media)
    не можна передавати шлях (str/Path) як send_photo/send_animation роблять
    без проблем. PTB для InputMedia* завжди викликає parse_file_input
    з local_mode=True (бо не знає реальних налаштувань бота) — а це означає,
    що звичайний Path перетворюється на URI виду "file:///...". Такий URI
    приймає лише локально розгорнутий Bot API сервер (--local), а не
    api.telegram.org, тому Telegram у відповідь на editMessageMedia повертав
    400 "Invalid file http url specified: unsupported url protocol". Відкритий
    файловий об'єкт PTB натомість загортає в InputFile і аплоадить мультипартом
    — так само, як це вже коректно працює в send_step().
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
    else:
        row.append(InlineKeyboardButton(GREET_BUTTON_TEXT, callback_data=f"{CALLBACK_PREFIX}:greet"))
    return InlineKeyboardMarkup([row])


def _post_greet_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура для того, хто вже привітав: кнопку «Привітати» знімаємо
    (голос уже враховано), але «← Назад» лишаємо — інакше з фінального
    екрана нікуди не гортається."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(BACK_BUTTON_TEXT, callback_data=f"{CALLBACK_PREFIX}:step:{LAST_STEP - 1}")
    ]])


async def _keyboard_for(step: int, telegram_id: int) -> InlineKeyboardMarkup:
    """Клавіатура кроку з урахуванням того, чи людина вже привітала.

    Перевірка в БД потрібна лише на останньому кроці — щоб той, хто вже
    натискав «Привітати FTP», не бачив цю кнопку знову, якщо повернеться
    сюди через «← Назад» → «Далі →». На решті кроків жодного запиту немає.
    """
    if step == LAST_STEP and await run_blocking(has_greeted, telegram_id):
        return _post_greet_keyboard()
    return build_keyboard(step)


async def send_step(target_bot: Bot, chat_id: int, step: int, keyboard: InlineKeyboardMarkup | None = None):
    """Надсилає крок новим повідомленням."""
    caption = STEPS[step]["caption"]
    reply_markup = keyboard if keyboard is not None else build_keyboard(step)
    if _is_text(step):
        return await target_bot.send_message(
            chat_id=chat_id, text=caption, parse_mode='HTML', reply_markup=reply_markup
        )
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
    """ Відправляє перший екран кампанії «FTP × 15» (тестовим або всім активним користувачам). """
    # Без будь-якої картинки історія обірветься посередині — краще не слати нічого
    missing = [
        data["file"] for data in STEPS.values()
        if data["file"] and not (IMAGES_DIR / data["file"]).is_file()
    ]
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
    keyboard = await _keyboard_for(step, update.effective_user.id)

    # Telegram не вміє редагуванням перетворити фото на текст і навпаки.
    # Тож на межі «фото ↔ текст» видаляємо поточне повідомлення і шлемо
    # крок новим — у чаті все одно лишається одне повідомлення.
    current = query.message
    current_is_text = current is not None and not (current.photo or current.animation)
    if current is not None and current_is_text != _is_text(step):
        try:
            await current.delete()
        except BadRequest as e:
            logging.warning(f"[ftp15] не вдалося видалити попередній крок: {e}")
        await send_step(context.bot, update.effective_chat.id, step, keyboard=keyboard)
        return

    if _is_text(step):
        try:
            await query.edit_message_text(text=STEPS[step]["caption"], parse_mode='HTML', reply_markup=keyboard)
        except BadRequest as e:
            if "not modified" not in str(e).lower():
                logging.warning(f"[ftp15] не вдалося перегорнути на крок {step}: {e} — надсилаємо новим повідомленням")
                await send_step(context.bot, update.effective_chat.id, step, keyboard=keyboard)
        return

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
            logging.warning(f"[ftp15] не вдалося перегорнути на крок {step}: {e} — надсилаємо новим повідомленням")
            _file_ids.pop(step, None)
            await send_step(context.bot, update.effective_chat.id, step, keyboard=keyboard)
    finally:
        if file_handle:
            file_handle.close()


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
        await query.edit_message_reply_markup(reply_markup=_post_greet_keyboard())
    except BadRequest as e:
        logging.warning(f"[ftp15] не вдалося оновити кнопки: {e}")

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

def _ensure_greetings_table(cursor) -> None:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ftp15_greetings (
        telegram_id   BIGINT PRIMARY KEY,
        employee_name TEXT,
        greeted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)


def has_greeted(telegram_id: int) -> bool:
    """Чи ця людина вже натискала «Привітати FTP» (щоб на фінальному кроці не показувати кнопку вдруге)."""
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cursor:
            _ensure_greetings_table(cursor)
            cursor.execute("SELECT 1 FROM ftp15_greetings WHERE telegram_id = %s", (telegram_id,))
            return cursor.fetchone() is not None
    finally:
        conn.close()


def add_ftp15_greeting(telegram_id: int, employee_name: str) -> tuple[bool, int]:
    """Зараховує привітання (одне на telegram_id). Повертає (чи нове, скільки всього)."""
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cursor:
            _ensure_greetings_table(cursor)
            # DO UPDATE замість DO NOTHING: кожне натискання оновлює час на
            # останній клік (а не лишається зі значенням першого), тож у таблиці
            # завжди видно, коли людина тиснула востаннє. Це не змінює кількість
            # у лічильнику — telegram_id все одно PRIMARY KEY, один рядок на
            # людину незалежно від кількості кліків. Тризнак "xmax = 0" —
            # стандартний спосіб у Postgres відрізнити, чи це був справжній
            # INSERT (нова людина), чи спрацював ON CONFLICT (повторний клік).
            cursor.execute(
                "INSERT INTO ftp15_greetings (telegram_id, employee_name) VALUES (%s, %s) "
                "ON CONFLICT (telegram_id) DO UPDATE SET "
                "employee_name = EXCLUDED.employee_name, greeted_at = CURRENT_TIMESTAMP "
                "RETURNING (xmax = 0) AS is_new",
                (telegram_id, employee_name)
            )
            is_new = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM ftp15_greetings")
            total = cursor.fetchone()[0]
        return is_new, total
    finally:
        conn.close()

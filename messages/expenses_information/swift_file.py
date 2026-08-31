"""
Видача файлів SWIFT по платіжному дорученню з Google Drive.

Кнопка «Отримати файли» додається до повідомлення про появу SWIFT
(messages/expenses_information/check_swift_payments.py) — і тільки до нього:
для звичайного списання коштів файлів ще немає.

У папці на Drive ім'я кожного файлу починається з номера документа
(`CR108920__Confirmation_of_Transfer_4366....pdf`), тож по натисканню кнопки
шукаємо всі файли з таким номером і надсилаємо їх користувачу.

Це банківські підтвердження, тому в чаті вони не залишаються: одразу після
відправки бот попереджає окремим повідомленням і через DELETE_AFTER_SECONDS
сам їх видаляє.
"""

import asyncio
import logging
import os
from io import BytesIO

from messages.expenses_information.payment_tablepart import (
    MAX_CALLBACK_DATA_BYTES,
    _fmt_text,
    _safe_payment_number,
)
from utils.google_drive import download_file, list_folder_files

# callback_data має вигляд "swiftfile:<номер платіжки>"
CALLBACK_PREFIX = "swiftfile"
BUTTON_TEXT = "📎 Отримати файли"

FOLDER_ID = os.getenv("SWIFT_DRIVE_FOLDER_ID", "1RW6X5tt0LPHvTrL-t8gXktkUc1JwWF8S")

# Через скільки секунд прибираємо надіслані файли з чату
DELETE_AFTER_SECONDS = 180

# Ліміт Telegram на sendDocument — 50 МБ; лишаємо запас
MAX_FILE_SIZE = 45 * 1024 * 1024

# Рідні формати Google (Docs/Sheets) не можна тягнути через alt=media
GOOGLE_NATIVE_MIME_PREFIX = "application/vnd.google-apps."


def _matches_document(file_name: str, payment_number: str) -> bool:
    """
    Чи належить файл цьому документу.

    Drive-запит `name contains` дає підрядок, і цього замало: за номером
    `CR10892` він поверне ще й файли `CR108920` та `CR108921`. Тому вимагаємо,
    щоб ім'я саме починалося з номера, а наступний символ був роздільником.
    """
    name = (file_name or "").strip().lower()
    number = (payment_number or "").strip().lower()
    if not name or not number or not name.startswith(number):
        return False

    rest = name[len(number):]
    return not rest or not rest[0].isalnum()


def build_swift_file_button(doc_number) -> dict | None:
    """
    Одна inline-кнопка для сирого JSON клавіатури Bot API (розсилка йде через
    requests, а не через PTB). None — якщо номер небезпечний або не влазить
    у callback_data.
    """
    safe_number = _safe_payment_number(doc_number)
    if not safe_number:
        return None

    callback_data = f"{CALLBACK_PREFIX}:{safe_number}"
    if len(callback_data.encode("utf-8")) > MAX_CALLBACK_DATA_BYTES:
        logging.warning(f"🚫 Файл SWIFT: callback_data задовга для {safe_number}")
        return None

    return {"text": BUTTON_TEXT, "callback_data": callback_data}


def build_swift_file_keyboard(doc_number) -> dict | None:
    """Клавіатура з однією кнопкою — якщо файл треба показати окремо."""
    button = build_swift_file_button(doc_number)
    return {"inline_keyboard": [[button]]} if button else None


# ---------------------------
# GOOGLE DRIVE
# ---------------------------
def fetch_swift_files(payment_number) -> list[tuple[str, bytes]] | None:
    """
    Файли документа як список пар (ім'я, вміст).

    Повертає:
      * list[tuple] — знайдені файли (може бути порожнім, якщо їх ще немає);
      * None        — помилка (немає доступу до Drive, поганий номер).
    """
    safe_number = _safe_payment_number(payment_number)
    if not safe_number:
        logging.warning(f"🚫 Файли SWIFT: некоректний номер документа {payment_number!r}")
        return None

    found = list_folder_files(FOLDER_ID, name_contains=safe_number)
    if found is None:
        return None

    files: list[tuple[str, bytes]] = []
    for item in found:
        name = item.get("name") or ""
        if not _matches_document(name, safe_number):
            continue

        if str(item.get("mimeType", "")).startswith(GOOGLE_NATIVE_MIME_PREFIX):
            logging.warning(f"⏭ Файли SWIFT: пропускаю рідний формат Google — {name}")
            continue

        try:
            size = int(item.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        if size > MAX_FILE_SIZE:
            logging.warning(f"⏭ Файли SWIFT: {name} завеликий ({size} байт)")
            continue

        content = download_file(item["id"])
        if content is None:
            logging.error(f"❌ Файли SWIFT: не вдалося завантажити {name}")
            continue

        files.append((name, content))

    logging.info(f"📎 Файли SWIFT {safe_number}: готово до відправки {len(files)} файл(ів)")
    return files


# ---------------------------
# ФОРМАТУВАННЯ
# ---------------------------
def format_caption(payment_number, count: int) -> str:
    return f"📎 Файли до документа № <b>{_fmt_text(payment_number)}</b> ({count} шт.)"


def format_notice_message() -> str:
    minutes = DELETE_AFTER_SECONDS // 60
    return (
        f"🕒 Файли будуть автоматично видалені через {minutes} хв.\n"
        "Збережіть їх, якщо потрібні."
    )


def format_deleted_message(payment_number) -> str:
    return f"🗑 Файли до документа № <b>{_fmt_text(payment_number)}</b> видалено."


def format_partial_message(payment_number) -> str:
    return (
        f"⚠️ Не всі файли до документа № <b>{_fmt_text(payment_number)}</b> вдалося надіслати.\n"
        "Натисніть кнопку ще раз, щоб отримати повний набір."
    )


def format_empty_message(payment_number) -> str:
    return (
        f"ℹ️ До документа № <b>{_fmt_text(payment_number)}</b> файлів на диску ще немає.\n\n"
        "Підтвердження банку з'являються не одразу — спробуйте трохи згодом."
    )


def format_error_message(payment_number) -> str:
    return (
        f"⚠️ Не вдалося отримати файли до документа № <b>{_fmt_text(payment_number)}</b>.\n"
        "Сервіс зберігання зараз недоступний — спробуйте, будь ласка, за кілька хвилин."
    )


# ---------------------------
# АВТОВИДАЛЕННЯ
# ---------------------------
async def _delete_sent_files(context) -> None:
    """Прибирає надіслані файли; попередження перетворює на позначку про видалення."""
    data = context.job.data
    chat_id = data["chat_id"]
    file_message_ids = data["file_message_ids"]
    notice_message_id = data["notice_message_id"]
    payment_number = data["payment_number"]

    try:
        await context.bot.delete_messages(chat_id=chat_id, message_ids=file_message_ids)
    except Exception:
        # Пакетне видалення не пройшло (напр. частину вже прибрав користувач) —
        # добиваємо поштучно, щоб не лишити чужі документи в чаті.
        for message_id in file_message_ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            except Exception:
                logging.warning(f"⚠️ Файли SWIFT: не вдалося видалити повідомлення {message_id}")

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=notice_message_id,
            text=format_deleted_message(payment_number),
            parse_mode="HTML",
        )
    except Exception:
        logging.warning("⚠️ Файли SWIFT: не вдалося оновити повідомлення про видалення")


# ---------------------------
# ХЕНДЛЕР
# ---------------------------
async def show_swift_file(update, context, payment_number: str) -> None:
    """Відповідає на натискання кнопки окремими повідомленнями (кнопка лишається)."""
    query = update.callback_query
    if not query or not query.message:
        return

    chat_id = query.message.chat_id
    wait_msg = await query.message.reply_text(
        f"⏳ Шукаю файли до документа № <b>{_fmt_text(payment_number)}</b>…",
        parse_mode="HTML",
    )

    try:
        loop = asyncio.get_running_loop()
        files = await loop.run_in_executor(None, fetch_swift_files, payment_number)
    finally:
        try:
            await context.bot.delete_message(chat_id, wait_msg.message_id)
        except Exception:
            pass

    if files is None:
        await query.message.reply_text(format_error_message(payment_number), parse_mode="HTML")
        return

    if not files:
        await query.message.reply_text(format_empty_message(payment_number), parse_mode="HTML")
        return

    file_message_ids = []
    send_failed = False
    try:
        for idx, (name, content) in enumerate(files):
            sent = await query.message.reply_document(
                document=BytesIO(content),
                filename=name,
                caption=format_caption(payment_number, len(files)) if idx == 0 else None,
                parse_mode="HTML",
            )
            file_message_ids.append(sent.message_id)
    except Exception:
        # Частину файлів могли встигнути надіслати — нижче все одно ставимо їх
        # у чергу на видалення, щоб банківські документи не лишились у чаті.
        send_failed = True
        logging.exception(f"❌ Файли SWIFT: не вдалося надіслати всі файли {payment_number}")

    if not file_message_ids:
        await query.message.reply_text(format_error_message(payment_number), parse_mode="HTML")
        return

    if send_failed:
        await query.message.reply_text(format_partial_message(payment_number), parse_mode="HTML")

    notice = await query.message.reply_text(format_notice_message())

    if context.job_queue:
        context.job_queue.run_once(
            _delete_sent_files,
            DELETE_AFTER_SECONDS,
            data={
                "chat_id": chat_id,
                "file_message_ids": file_message_ids,
                "notice_message_id": notice.message_id,
                "payment_number": payment_number,
            },
            name=f"swiftfile-cleanup:{chat_id}:{notice.message_id}",
        )
    else:
        logging.error("❌ Файли SWIFT: job_queue недоступна — файли лишаться в чаті")

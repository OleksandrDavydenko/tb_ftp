"""
Перегляд файлу SWIFT по платіжному дорученню.

Кнопка «Переглянути файл» додається до повідомлення про появу SWIFT
(messages/expenses_information/check_swift_payments.py) — і тільки до нього:
для звичайного списання коштів файлу ще немає.

Поки що це заглушка: у відповідь на натискання бот пише, що функціонал
у розробці. Сам файл сюди під'єднаємо пізніше.
"""

import logging

from messages.expenses_information.payment_tablepart import (
    MAX_CALLBACK_DATA_BYTES,
    _safe_payment_number,
)

# callback_data має вигляд "swiftfile:<номер платіжки>"
CALLBACK_PREFIX = "swiftfile"
BUTTON_TEXT = "Переглянути файл"

# Тимчасова відповідь, поки перегляд файлу не реалізовано
STUB_MESSAGE = "Функціонал ще в розробці"


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


async def show_swift_file(update, context, payment_number: str) -> None:
    """Відповідає на натискання кнопки окремим повідомленням (кнопка лишається)."""
    query = update.callback_query
    if not query or not query.message:
        return

    await query.message.reply_text(STUB_MESSAGE, parse_mode="HTML")

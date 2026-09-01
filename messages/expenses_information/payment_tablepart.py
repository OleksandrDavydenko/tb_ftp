"""
Таблична частина платіжного доручення: які рахунки й по яких угодах оплачено.

Дані беремо з PBI-таблиці `telegram_payments_tablepart` за номером платіжки.
Кнопка «🧾 Показати рахунки й угоди» додається до розсилки SWIFT-платежів
(messages/check_swift_payments.py), якщо у платіжки OperationCode = 33.
"""

import asyncio
import html
import logging
import os
import re

import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import MessageLimit
from telegram.error import BadRequest

from auth import get_power_bi_token

# callback_data має вигляд "swiftdeals:<номер платіжки>"
CALLBACK_PREFIX = "swiftdeals"
BUTTON_TEXT = "🧾 Показати рахунки й угоди"

# Згортання розшифровки назад — "swiftdealsx:<номер платіжки>"
COLLAPSE_PREFIX = "swiftdealsx"
COLLAPSE_BUTTON_TEXT = "🔼 Згорнути розшифровку"

# Розшифровку вписуємо в те саме повідомлення цитатою; за цим маркером потім
# відрізаємо її назад. Префікс, а не повний тег — цитата буває і розгортною.
BLOCKQUOTE_MARKER = "<blockquote"

# До скількох рахунків показуємо список одразу, не ховаючи в розгортний блок
MAX_ROWS_ALWAYS_VISIBLE = 3

DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")
PBI_EXEC_URL = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"

TABLE_NAME = "telegram_payments_tablepart"

# Назви колонок у PBI-таблиці telegram_payments_tablepart
COLUMNS = [
    "PaymentNumber",
    "PaymentDate",
    "InvoiceNumber",
    "DealNumber",
    "SUMInCur",
    "SUM_USD",
    "Currency",
]

# Ліміт повідомлення Telegram — 4096 символів; лишаємо запас
MAX_MESSAGE_LEN = 3500

# Жорсткий ліміт Telegram — понад це повідомлення просто не відредагується
MAX_MESSAGE_TEXT_LEN = MessageLimit.MAX_TEXT_LENGTH

# Ліміт callback_data у Telegram Bot API
MAX_CALLBACK_DATA_BYTES = 64

# Номер платіжки, який безпечно підставляти в DAX-запит
_SAFE_PAYMENT_NUMBER = re.compile(r"^[A-Za-z0-9_\-/.]{1,50}$")

SEPARATOR = "━" * 20


def _get(row, col_name):
    # ключі мають вигляд 'telegram_payments_tablepart[InvoiceNumber]'
    # шукаємо точний збіг у дужках, щоб 'Currency' не підхопив 'SUMInCur'
    suffix = f"[{col_name}]"
    k = next((k for k in row if k.endswith(suffix)), None)
    if k is None:
        k = next((k for k in row if col_name in k), None)
    return row.get(k) if k else None


def _safe_payment_number(value) -> str | None:
    """Номер платіжки для підстановки в DAX; усе підозріле відкидаємо."""
    if value is None:
        return None
    text = str(value).strip()
    return text if _SAFE_PAYMENT_NUMBER.match(text) else None


def _fmt_amount(value):
    if value is None:
        return "—"
    try:
        return f"{float(value):,.2f}".replace(",", " ")
    except (TypeError, ValueError):
        return str(value)


def _to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        return float(str(value).strip().replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def _fmt_date(value) -> str | None:
    """PBI віддає дату ISO-рядком '2026-07-14T00:00:00' -> '14.07.2026'."""
    if value is None:
        return None
    s = str(value).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return f"{s[8:10]}.{s[5:7]}.{s[0:4]}"
    return s or None


def _fmt_text(value) -> str:
    text = str(value).strip() if value is not None else ""
    return html.escape(text) if text else "—"


def _fmt_code(value) -> str:
    """Номер у моноспейсі — щоб його можна було скопіювати тапом."""
    text = str(value).strip() if value is not None else ""
    if not text:
        return "—"
    return f"<code>{html.escape(text)}</code>"


# ---------------------------
# POWER BI
# ---------------------------
def fetch_payment_tablepart(payment_number) -> list[dict] | None:
    """
    Рядки табличної частини платіжки.

    Повертає:
      * list[dict] — знайдені рядки (може бути порожнім списком, якщо їх ще немає);
      * None       — помилка (немає токена, PBI недоступний, поганий номер).
    """
    safe_number = _safe_payment_number(payment_number)
    if not safe_number:
        logging.warning(f"🚫 Таблична частина: некоректний номер платіжки {payment_number!r}")
        return None

    token = get_power_bi_token()
    if not token:
        logging.error("❌ Таблична частина: не вдалося отримати токен Power BI")
        return None

    dax = f"""
EVALUATE
FILTER(
    '{TABLE_NAME}',
    '{TABLE_NAME}'[PaymentNumber] = "{safe_number}"
)
ORDER BY '{TABLE_NAME}'[DealNumber], '{TABLE_NAME}'[InvoiceNumber]
"""

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}

    try:
        r = requests.post(PBI_EXEC_URL, headers=headers, json=payload, timeout=60)
    except Exception as e:
        logging.exception(f"❌ Таблична частина: виняток при зверненні до Power BI: {e}")
        return None

    if r.status_code != 200:
        logging.error(
            f"❌ Таблична частина: Power BI повернув статус {r.status_code}: {r.text[:300]}"
        )
        return None

    try:
        rows = r.json()["results"][0]["tables"][0].get("rows", [])
    except (ValueError, KeyError, IndexError) as e:
        logging.error(f"❌ Таблична частина: неочікувана відповідь Power BI: {e}")
        return None

    logging.info(f"📥 Таблична частина {safe_number}: отримано {len(rows)} рядк(ів)")
    return rows


# ---------------------------
# ФОРМАТУВАННЯ
# ---------------------------
def _split_messages(header: str, items: list[str], footer: str) -> list[str]:
    """Збирає блоки в повідомлення, розбиваючи на межі елемента при переповненні."""
    messages: list[str] = []
    current = header

    for block in [*items, footer]:
        if not block:
            continue
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) > MAX_MESSAGE_LEN and current:
            messages.append(current)
            current = block
        else:
            current = candidate

    if current:
        messages.append(current)
    return messages


def _build_tablepart_parts(payment_number, rows: list[dict]) -> tuple[str, list[str], str]:
    """Заголовок, список рахунків і підсумок — окремо, щоб зібрати їх по-різному."""
    safe_number = _fmt_text(payment_number)

    date_str = None
    for row in rows:
        date_str = _fmt_date(_get(row, "PaymentDate"))
        if date_str:
            break

    header = f"🧾 Розшифровка платіжки <b>{safe_number}</b>"
    if date_str:
        header += f" від <b>{date_str}</b>"
    header += "\n\nОплачені рахунки:"

    items: list[str] = []
    totals_by_currency: dict[str, float] = {}
    invoices: set[str] = set()
    deals: set[str] = set()

    for idx, row in enumerate(rows, start=1):
        invoice = str(_get(row, "InvoiceNumber") or "").strip()
        deal = str(_get(row, "DealNumber") or "").strip()
        currency = str(_get(row, "Currency") or "").strip()
        raw_amount = _get(row, "SUMInCur")
        amount = _to_float(raw_amount)

        # Для доларових платежів 1С кладе 0 у SUMInCur, а справжню суму —
        # лише в SUM_USD. Тоді показуємо суму з SUM_USD і позначаємо її як USD.
        if not amount:
            amount_usd = _to_float(_get(row, "SUM_USD"))
            if amount_usd:
                raw_amount, amount, currency = amount_usd, amount_usd, "USD"

        if invoice:
            invoices.add(invoice)
        if deal:
            deals.add(deal)

        amount_str = f"{_fmt_amount(raw_amount)} {currency}".strip()

        items.append(
            f"{idx}. Рахунок {_fmt_code(invoice)}\n"
            f"   Угода: {_fmt_code(deal)}\n"
            f"   Сума: <b>{html.escape(amount_str)}</b>"
        )

        if amount is not None:
            key = currency.upper() or "—"
            totals_by_currency[key] = totals_by_currency.get(key, 0.0) + amount

    totals_parts = [
        f"<b>{_fmt_amount(total)} {html.escape(cur)}</b>".strip()
        for cur, total in totals_by_currency.items()
    ]
    footer_lines = [SEPARATOR]
    if totals_parts:
        footer_lines.append("Разом: " + " · ".join(totals_parts))
    footer_lines.append(
        f"Рахунків: <b>{len(invoices) or len(rows)}</b> · Угод: <b>{len(deals)}</b>"
    )
    footer = "\n".join(footer_lines)

    return header, items, footer


def format_tablepart_message(payment_number, rows: list[dict]) -> list[str]:
    """Плоский список оплачених рахунків + підсумок. Повертає список повідомлень."""
    header, items, footer = _build_tablepart_parts(payment_number, rows)
    return _split_messages(header, items, footer)


def format_tablepart_block(payment_number, rows: list[dict]) -> str:
    """
    Те саме одним суцільним блоком — для вставки в повідомлення.

    Без заголовка: номер платіжки й дата вже є в самому повідомленні вище, а
    перші рядки згорнутого блоку треба віддати під самі рахунки — інакше у
    прев'ю видно лише службовий текст.
    """
    _, items, footer = _build_tablepart_parts(payment_number, rows)
    return "\n\n".join([*items, footer])


def wrap_in_blockquote(block: str, rows_count: int) -> str:
    """
    Цитата навколо розшифровки.

    Короткий список лишаємо розгорнутим — користувач натиснув «Показати рахунки»
    і має одразу їх побачити. Згортаємо лише довгий, щоб не роздувати чат.
    """
    expandable = " expandable" if rows_count > MAX_ROWS_ALWAYS_VISIBLE else ""
    return f"<blockquote{expandable}>{block}</blockquote>"


def format_empty_message(payment_number) -> str:
    return (
        f"⏳ По платіжці <b>{_fmt_text(payment_number)}</b> рахунки ще не підтягнулись.\n\n"
        "Табличну частину видно тільки після того, як платіж рознесуть по рахунках в 1С — "
        "зазвичай це займає трохи часу. Натисніть кнопку ще раз трохи згодом."
    )


def format_error_message(payment_number) -> str:
    return (
        f"⚠️ Не вдалося отримати дані по платіжці <b>{_fmt_text(payment_number)}</b>.\n"
        "Сервіс звітності зараз недоступний — спробуйте, будь ласка, ще раз за кілька хвилин."
    )


# ---------------------------
# КНОПКА / ХЕНДЛЕР
# ---------------------------
def build_deals_keyboard(doc_number) -> dict | None:
    """
    Сирий JSON inline-клавіатури для Bot API (розсилка йде через requests,
    а не через PTB). None — якщо номер небезпечний або не влазить у callback_data.
    """
    safe_number = _safe_payment_number(doc_number)
    if not safe_number:
        return None

    callback_data = f"{CALLBACK_PREFIX}:{safe_number}"
    if len(callback_data.encode("utf-8")) > MAX_CALLBACK_DATA_BYTES:
        logging.warning(f"🚫 Таблична частина: callback_data задовга для {safe_number}")
        return None

    return {"inline_keyboard": [[{"text": BUTTON_TEXT, "callback_data": callback_data}]]}


def _swap_deals_button(reply_markup, payment_number: str, *, collapsed: bool) -> dict | None:
    """
    Клавіатура повідомлення з перемкнутою кнопкою розшифровки.

    Проходимо наявні ряди й підміняємо лише кнопку розшифровки — решта (зокрема
    «📎 Отримати файли») лишається як була, тож знати про неї тут не треба.
    """
    safe_number = _safe_payment_number(payment_number)
    if not safe_number or not reply_markup:
        return None

    if collapsed:
        new_button = {"text": BUTTON_TEXT, "callback_data": f"{CALLBACK_PREFIX}:{safe_number}"}
    else:
        new_button = {
            "text": COLLAPSE_BUTTON_TEXT,
            "callback_data": f"{COLLAPSE_PREFIX}:{safe_number}",
        }

    rows = []
    for row in reply_markup.inline_keyboard:
        new_row = []
        for button in row:
            data = button.callback_data or ""
            if data.startswith(f"{CALLBACK_PREFIX}:") or data.startswith(f"{COLLAPSE_PREFIX}:"):
                new_row.append(new_button)
            else:
                new_row.append({"text": button.text, "callback_data": data})
        rows.append(new_row)

    return {"inline_keyboard": rows}


async def show_payment_tablepart(update, context, payment_number: str) -> None:
    """Вписує розшифровку в те саме повідомлення розгортним блоком."""
    query = update.callback_query
    if not query or not query.message:
        return

    loop = asyncio.get_running_loop()
    rows = await loop.run_in_executor(None, fetch_payment_tablepart, payment_number)

    # Тимчасові стани (даних ще немає / PBI недоступний) у саме повідомлення не
    # вписуємо — відповідаємо на нього, щоб відповідь була прив'язана до платіжки.
    if rows is None or not rows:
        text = format_error_message(payment_number) if rows is None else format_empty_message(payment_number)
        await query.message.reply_text(
            text, parse_mode="HTML", reply_to_message_id=query.message.message_id
        )
        return

    block = wrap_in_blockquote(format_tablepart_block(payment_number, rows), len(rows))
    new_text = f"{query.message.text_html}\n\n{block}"

    # Не влізає в одне повідомлення — віддаємо як раніше, окремими відповідями
    if len(new_text) > MAX_MESSAGE_TEXT_LEN:
        for text in format_tablepart_message(payment_number, rows):
            await query.message.reply_text(
                text, parse_mode="HTML", reply_to_message_id=query.message.message_id
            )
        return

    await _edit_message(
        query,
        new_text,
        _swap_deals_button(query.message.reply_markup, payment_number, collapsed=False),
    )


async def hide_payment_tablepart(update, context, payment_number: str) -> None:
    """Прибирає розшифровку, повертаючи повідомлення до початкового вигляду."""
    query = update.callback_query
    if not query or not query.message:
        return

    text = query.message.text_html
    if BLOCKQUOTE_MARKER not in text:
        return

    await _edit_message(
        query,
        text.split(BLOCKQUOTE_MARKER)[0].rstrip(),
        _swap_deals_button(query.message.reply_markup, payment_number, collapsed=True),
    )


async def _edit_message(query, text: str, keyboard: dict | None) -> None:
    """Редагує повідомлення; сирий JSON клавіатури перетворюємо на об'єкт PTB."""
    markup = None
    if keyboard:
        markup = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row]
                for row in keyboard["inline_keyboard"]
            ]
        )

    try:
        await query.edit_message_text(text=text, parse_mode="HTML", reply_markup=markup)
    except BadRequest as e:
        # "message is not modified" — подвійне натискання; решта: повідомлення
        # видалили або воно застаре. Падати через це не варто.
        logging.warning(f"⚠️ Таблична частина: не вдалося оновити повідомлення: {e}")

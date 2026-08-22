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

from auth import get_power_bi_token

# callback_data має вигляд "swiftdeals:<номер платіжки>"
CALLBACK_PREFIX = "swiftdeals"
BUTTON_TEXT = "🧾 Показати рахунки й угоди"

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


def format_tablepart_message(payment_number, rows: list[dict]) -> list[str]:
    """Плоский список оплачених рахунків + підсумок. Повертає список повідомлень."""
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
    total_usd = 0.0
    has_usd = False
    invoices: set[str] = set()
    deals: set[str] = set()

    for idx, row in enumerate(rows, start=1):
        invoice = str(_get(row, "InvoiceNumber") or "").strip()
        deal = str(_get(row, "DealNumber") or "").strip()
        currency = str(_get(row, "Currency") or "").strip()
        amount = _to_float(_get(row, "SUMInCur"))
        amount_usd = _to_float(_get(row, "SUM_USD"))

        if invoice:
            invoices.add(invoice)
        if deal:
            deals.add(deal)

        amount_str = f"{_fmt_amount(_get(row, 'SUMInCur'))} {currency}".strip()
        amount_line = f"   Сума: <b>{html.escape(amount_str)}</b>"
        # Якщо валюта вже USD — не дублюємо приблизний еквівалент
        if amount_usd is not None and currency.upper() != "USD":
            amount_line += f" · ≈{_fmt_amount(amount_usd)} USD"

        items.append(
            f"{idx}. Рахунок <b>{_fmt_text(invoice)}</b>\n"
            f"   Угода: <b>{_fmt_text(deal)}</b>\n"
            f"{amount_line}"
        )

        if amount is not None:
            key = currency.upper() or "—"
            totals_by_currency[key] = totals_by_currency.get(key, 0.0) + amount
        if amount_usd is not None:
            total_usd += amount_usd
            has_usd = True

    totals_parts = [
        f"<b>{_fmt_amount(total)} {html.escape(cur)}</b>".strip()
        for cur, total in totals_by_currency.items()
    ]
    footer_lines = [SEPARATOR]
    if totals_parts:
        footer_lines.append("Разом: " + " · ".join(totals_parts))
        if has_usd and list(totals_by_currency) != ["USD"]:
            if len(totals_by_currency) == 1:
                # одна валюта (не USD) — еквівалент дописуємо в той самий рядок
                footer_lines[-1] += f" · ≈{_fmt_amount(total_usd)} USD"
            else:
                # мультивалютна частина: окремий рядок, щоб USD не читався двічі
                footer_lines.append(f"Еквівалент: ≈{_fmt_amount(total_usd)} USD")
    footer_lines.append(
        f"Рахунків: <b>{len(invoices) or len(rows)}</b> · Угод: <b>{len(deals)}</b>"
    )
    footer = "\n".join(footer_lines)

    return _split_messages(header, items, footer)


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


async def show_payment_tablepart(update, context, payment_number: str) -> None:
    """Відповідає на натискання кнопки окремим повідомленням (кнопка лишається)."""
    query = update.callback_query
    if not query or not query.message:
        return

    loop = asyncio.get_running_loop()
    rows = await loop.run_in_executor(None, fetch_payment_tablepart, payment_number)

    if rows is None:
        texts = [format_error_message(payment_number)]
    elif not rows:
        texts = [format_empty_message(payment_number)]
    else:
        texts = format_tablepart_message(payment_number, rows)

    for text in texts:
        await query.message.reply_text(text, parse_mode="HTML")

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

# Гортання сторінок — "swiftdealsp:<номер платіжки>:<сторінка>"
PAGE_PREFIX = "swiftdealsp"
PREV_BUTTON_TEXT = "◀ Попередні"
NEXT_BUTTON_TEXT = "Наступні ▶"

# Запас у бюджеті сторінки під теги цитати й рядок «Сторінка N з M»
PAGE_OVERHEAD = 120

# Розшифровку вписуємо в те саме повідомлення цитатою; за цим маркером потім
# відрізаємо її назад
BLOCKQUOTE_MARKER = "<blockquote"

DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")
PBI_EXEC_URL = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"

TABLE_NAME = "telegram_payments_tablepart"

# Назви колонок у PBI-таблиці telegram_payments_tablepart
COLUMNS = [
    "PaymentNumber",
    "PaymentDate",
    "InvoiceNumber",
    "OverheadInvoiceNumber",
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

# Типи позицій платіжки: звичайний рахунок і накладна витрата (без угоди)
KIND_INVOICE = "invoice"
KIND_OVERHEAD = "overhead"
LABELS = {KIND_INVOICE: "Рахунок", KIND_OVERHEAD: "Накладна витрата"}


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


def _row_kind(row) -> tuple[str, str]:
    """
    Що це за позиція платіжки: ('invoice'|'overhead', номер).

    Накладна витрата не йде в собівартість і угоди не має — у 1С вона приходить
    з порожнім InvoiceNumber і заповненим OverheadInvoiceNumber. Якщо порожні
    обидва — лишаємо рахунком без номера, як було до появи накладних витрат.
    """
    invoice = str(_get(row, "InvoiceNumber") or "").strip()
    if invoice:
        return KIND_INVOICE, invoice

    overhead = str(_get(row, "OverheadInvoiceNumber") or "").strip()
    if overhead:
        return KIND_OVERHEAD, overhead

    return KIND_INVOICE, ""


def _build_header(payment_number, date_str: str | None, kinds: set[str]) -> str:
    """Заголовок для окремих повідомлень — під те, що в платіжці насправді є."""
    header = f"🧾 Розшифровка платіжки <b>{_fmt_text(payment_number)}</b>"
    if date_str:
        header += f" від <b>{date_str}</b>"

    if kinds == {KIND_OVERHEAD}:
        subtitle = "Накладні витрати:"
    elif KIND_OVERHEAD in kinds:
        subtitle = "Оплачені рахунки й накладні витрати:"
    else:
        subtitle = "Оплачені рахунки:"

    return f"{header}\n\n{subtitle}"


def _build_tablepart_parts(payment_number, rows: list[dict]) -> tuple[str, list[str], str]:
    """Заголовок, список позицій і підсумок — окремо, щоб зібрати їх по-різному."""
    date_str = None
    for row in rows:
        date_str = _fmt_date(_get(row, "PaymentDate"))
        if date_str:
            break

    # Спершу рахунки, потім накладні витрати. Сортування стабільне, тож порядок
    # усередині групи лишається той, що віддав PBI. Заголовки секцій не потрібні —
    # тип написано в кожному рядку.
    ordered = sorted(rows, key=lambda r: _row_kind(r)[0] == KIND_OVERHEAD)

    items: list[str] = []
    totals_by_currency: dict[str, float] = {}
    invoices: set[str] = set()
    overheads: set[str] = set()
    deals: set[str] = set()
    kinds: set[str] = set()
    invoice_rows = 0
    overhead_rows = 0

    for idx, row in enumerate(ordered, start=1):
        kind, number = _row_kind(row)
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

        kinds.add(kind)
        if kind == KIND_OVERHEAD:
            overhead_rows += 1
            if number:
                overheads.add(number)
        else:
            invoice_rows += 1
            if number:
                invoices.add(number)
            if deal:
                deals.add(deal)

        amount_str = f"{_fmt_amount(raw_amount)} {currency}".strip()

        lines = [f"{idx}. {LABELS[kind]} {_fmt_code(number)}"]
        # У накладної витрати угоди не буває, тож порожній рядок не малюємо.
        # А от рахунок без угоди лишаємо з прочерком — це сигнал, що в 1С
        # щось не заповнили.
        if kind == KIND_INVOICE:
            lines.append(f"   Угода: {_fmt_code(deal)}")
        lines.append(f"   Сума: <b>{html.escape(amount_str)}</b>")
        items.append("\n".join(lines))

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

    counters = []
    if invoice_rows:
        counters.append(f"Рахунків: <b>{len(invoices) or invoice_rows}</b>")
        counters.append(f"Угод: <b>{len(deals)}</b>")
    if overhead_rows:
        counters.append(f"Накладних витрат: <b>{len(overheads) or overhead_rows}</b>")
    if counters:
        footer_lines.append(" · ".join(counters))

    footer = "\n".join(footer_lines)

    return _build_header(payment_number, date_str, kinds), items, footer


def format_tablepart_message(payment_number, rows: list[dict]) -> list[str]:
    """Плоский список оплачених рахунків + підсумок. Повертає список повідомлень."""
    header, items, footer = _build_tablepart_parts(payment_number, rows)
    return _split_messages(header, items, footer)


def paginate_items(items: list[str], budget: int) -> list[list[str]]:
    """Розкладає рахунки по сторінках так, щоб кожна вміщалась у `budget` символів."""
    pages: list[list[str]] = []
    current: list[str] = []
    current_len = 0

    for item in items:
        addition = len(item) + 2  # роздільник "\n\n"
        if current and current_len + addition > budget:
            pages.append(current)
            current, current_len = [], 0
        current.append(item)
        current_len += addition

    if current:
        pages.append(current)
    return pages or [[]]


def format_tablepart_page(items: list[str], footer: str, page: int, total_pages: int) -> str:
    """Одна сторінка розшифровки: рахунки + підсумок (+ номер сторінки)."""
    tail = footer
    if total_pages > 1:
        tail = f"{footer}\nСторінка <b>{page}</b> з <b>{total_pages}</b>"
    return "\n\n".join([*items, tail])


def wrap_in_blockquote(block: str) -> str:
    """
    Цитата навколо розшифровки — завжди розгорнута.

    Розгортну (`<blockquote expandable>`) свідомо не використовуємо: Telegram
    згортає її до кількох перших рядків, і користувач, який щойно натиснув
    «Показати рахунки й угоди», бачить лише перший рахунок. За компактність
    відповідає кнопка «Згорнути розшифровку», а не приховування даних.
    """
    return f"<blockquote>{block}</blockquote>"


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


def _page_callback_data(safe_number: str, page: int) -> str:
    return f"{PAGE_PREFIX}:{safe_number}:{page}"


def _build_keyboard(
    reply_markup, payment_number: str, *, collapsed: bool, page: int = 1, total_pages: int = 1
) -> dict | None:
    """
    Клавіатура повідомлення: перемкнута кнопка розшифровки + гортання сторінок.

    Проходимо наявні ряди й чіпаємо лише свої кнопки — решта (зокрема
    «📎 Отримати файли») лишається як була, тож знати про неї тут не треба.
    Стару навігацію завжди відкидаємо й збираємо заново під поточну сторінку.
    """
    safe_number = _safe_payment_number(payment_number)
    if not safe_number or not reply_markup:
        return None

    if collapsed:
        toggle = {"text": BUTTON_TEXT, "callback_data": f"{CALLBACK_PREFIX}:{safe_number}"}
    else:
        toggle = {
            "text": COLLAPSE_BUTTON_TEXT,
            "callback_data": f"{COLLAPSE_PREFIX}:{safe_number}",
        }

    rows = []
    for row in reply_markup.inline_keyboard:
        new_row = []
        for button in row:
            data = button.callback_data or ""
            if data.startswith(f"{PAGE_PREFIX}:"):
                continue
            if data.startswith(f"{CALLBACK_PREFIX}:") or data.startswith(f"{COLLAPSE_PREFIX}:"):
                new_row.append(toggle)
            else:
                new_row.append({"text": button.text, "callback_data": data})
        if new_row:
            rows.append(new_row)

    if not collapsed and total_pages > 1:
        nav = []
        if page > 1:
            nav.append({"text": PREV_BUTTON_TEXT, "callback_data": _page_callback_data(safe_number, page - 1)})
        if page < total_pages:
            nav.append({"text": NEXT_BUTTON_TEXT, "callback_data": _page_callback_data(safe_number, page + 1)})
        if nav:
            # Гортання — першим рядом, одразу під текстом розшифровки
            rows.insert(0, nav)

    return {"inline_keyboard": rows}


def _original_text(message) -> str:
    """Текст повідомлення без раніше вставленої розшифровки."""
    text = message.text_html
    return text.split(BLOCKQUOTE_MARKER)[0].rstrip() if BLOCKQUOTE_MARKER in text else text


async def show_payment_tablepart(update, context, payment_number: str, page: int = 1) -> None:
    """Вписує розшифровку в те саме повідомлення; довгу — посторінково."""
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

    # Гортаючи сторінки, щоразу відштовхуємось від початкового тексту, інакше
    # розшифровки накладались би одна на одну
    original = _original_text(query.message)
    _, items, footer = _build_tablepart_parts(payment_number, rows)

    budget = MAX_MESSAGE_TEXT_LEN - len(original) - len(footer) - PAGE_OVERHEAD
    pages = paginate_items(items, budget)
    page = max(1, min(page, len(pages)))

    block = wrap_in_blockquote(format_tablepart_page(pages[page - 1], footer, page, len(pages)))
    new_text = f"{original}\n\n{block}"

    # Запобіжник: навіть одна сторінка не влізла (аномально довге повідомлення
    # або номер платіжки, з яким callback_data не вміщається) — віддаємо
    # окремими відповідями, як робили раніше.
    too_long = len(new_text) > MAX_MESSAGE_TEXT_LEN
    nav_too_long = len(pages) > 1 and (
        len(_page_callback_data(_safe_payment_number(payment_number) or "", len(pages)).encode("utf-8"))
        > MAX_CALLBACK_DATA_BYTES
    )
    if too_long or nav_too_long:
        logging.warning(
            f"⚠️ Таблична частина {payment_number}: розшифровка не влазить у повідомлення, шлю окремо"
        )
        for text in format_tablepart_message(payment_number, rows):
            await query.message.reply_text(
                text, parse_mode="HTML", reply_to_message_id=query.message.message_id
            )
        return

    await _edit_message(
        query,
        new_text,
        _build_keyboard(
            query.message.reply_markup,
            payment_number,
            collapsed=False,
            page=page,
            total_pages=len(pages),
        ),
    )


async def show_payment_tablepart_page(update, context, value: str) -> None:
    """Гортання сторінок: value має вигляд "<номер платіжки>:<сторінка>"."""
    payment_number, _, page_raw = value.rpartition(":")
    if not payment_number:
        return
    try:
        page = int(page_raw)
    except ValueError:
        page = 1

    await show_payment_tablepart(update, context, payment_number, page)


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
        _build_keyboard(query.message.reply_markup, payment_number, collapsed=True),
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

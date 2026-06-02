# hr/tenure_info.py
# -*- coding: utf-8 -*-
import logging
from calendar import monthrange
from datetime import date, datetime

import pandas as pd
import requests
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from utils.thinking import with_typing_action

from auth import get_power_bi_token
from utils.name_aliases import display_name

# Логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Константи Power BI
DATASET_ID = "8b80be15-7b31-49e4-bc85-8b37a0d98f1c"
PBI_URL = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"


# ─────────────────────────────────────────────────────────────────────────────
# Допоміжні функції
# ─────────────────────────────────────────────────────────────────────────────
def _diff_ymd(start: date, end: date):
    """Різниця у роках/місяцях/днях між двома датами (без pandas)."""
    if start > end:
        start, end = end, start

    y = end.year - start.year
    m = end.month - start.month
    d = end.day - start.day

    if d < 0:
        # попередній місяць відносно 'end'
        prev_year = end.year if end.month > 1 else end.year - 1
        prev_month = end.month - 1 if end.month > 1 else 12
        days_in_prev = monthrange(prev_year, prev_month)[1]
        d += days_in_prev
        m -= 1

    if m < 0:
        m += 12
        y -= 1

    return y, m, d


def _fmt_date_any(value) -> str:
    """Приводить різні типи до формату ДД.ММ.РРРР або повертає '—'."""
    if value is None:
        return "—"
    # Пробуємо пропустити через pandas для надійного парсингу рядків/мілісекунд тощо
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return "—"
        # Переводимо до чистого date
        if isinstance(ts, pd.Timestamp):
            dt = ts.to_pydatetime()
        else:
            dt = ts
        if isinstance(dt, datetime):
            dt = dt.date()
        if isinstance(dt, date):
            return dt.strftime("%d.%m.%Y")
    except Exception:
        pass
    return "—"


def _coerce_date(value):
    """Повертає datetime.date або None."""
    if value is None:
        return None
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        if isinstance(ts, pd.Timestamp):
            dt = ts.to_pydatetime()
        else:
            dt = ts
        if isinstance(dt, datetime):
            return dt.date()
        if isinstance(dt, date):
            return dt
    except Exception:
        return None
    return None


def _row_get(row: dict, *names: str, default=None):
    """
    Дістає значення з рядка Power BI, враховуючи, що ключі можуть бути як "Column",
    так і "[Column]". Повертає default, якщо значення не знайдено.
    """
    for name in names:
        if name in row:
            return row[name]
        # Спроба з дужками
        br = f"[{name}]" if not (name.startswith("[") and name.endswith("]")) else name
        if br in row:
            return row[br]
        # Спроба без дужок
        nb = name[1:-1] if (name.startswith("[") and name.endswith("]")) else name
        if nb in row:
            return row[nb]
    return default


def _build_message(row: dict) -> str:
    """Формує текст повідомлення зі стажем тощо."""
    today = date.today()

    employee = _row_get(row, "Employee", "[Employee]", default="—")
    last_dep = _row_get(row, "LastDepartment", "[LastDepartment]", default="—")
    phone_tg = _row_get(row, "PhoneNumberTelegram", "[PhoneNumberTelegram]", default="—")
    code = _row_get(row, "Code", "[Code]", default="—")
    bday_raw = _row_get(row, "birthdayDate", "[birthdayDate]")
    hire_raw = _row_get(row, "hireDate", "[hireDate]")

    hire_dt = _coerce_date(hire_raw)

    tenure_text = "—"
    if hire_dt:
        y, m, d = _diff_ymd(hire_dt, today)
        parts = []
        if y:
            parts.append(f"{y} р.")
        if m:
            parts.append(f"{m} міс.")
        if d or not parts:
            parts.append(f"{d} дн.")
        tenure_text = " ".join(parts)

    nice_name = display_name(employee)

    lines = [
        f"👤 Співробітник: {nice_name}",
        f"🏢 Відділ: {last_dep}",
        f"🆔 Код: {code}",
        f"📱 Telegram: {phone_tg}",
        "",
        f"📅 Сьогодні: {today.strftime('%d.%m.%Y')}",
        f"📄 Дата прийняття: {_fmt_date_any(hire_dt)}",
        f"⏳ Стаж: {tenure_text}",
    ]

    bday_fmt = _fmt_date_any(bday_raw)
    if bday_fmt != "—":
        lines.append(f"🎂 Дата народження: {bday_fmt}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Публічний Telegram-хендлер
# ─────────────────────────────────────────────────────────────────────────────
@with_typing_action
async def show_tenure_info(update: Update, context: CallbackContext) -> None:
    """
    - бере ім'я співробітника з context.user_data['employee_name']
    - тягне рядок з таблиці Employees у Power BI
    - відображає стаж, відділ, код, телефон тощо
    """
    employee = context.user_data.get("employee_name")
    if not employee:
        await update.message.reply_text("❌ Неможливо визначити ім'я співробітника.")
        return

    token = get_power_bi_token()
    if not token:
        await update.message.reply_text("❌ Не вдалося отримати токен для доступу до Power BI.")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # екранування лапок у DAX
    emp_escaped = employee.replace('"', '""')

    dax_query = {
        "queries": [
            {
                "query": f"""
EVALUATE
SELECTCOLUMNS(
    FILTER(Employees, Employees[Employee] = "{emp_escaped}"),
    "Employee", Employees[Employee],
    "LastDepartment", Employees[LastDepartment],
    "PhoneNumberTelegram", Employees[PhoneNumberTelegram],
    "Status", Employees[Status],
    "Code", Employees[Code],
    "birthdayDate", Employees[birthdayDate],
    "hireDate", Employees[hireDate]
)
"""
            }
        ],
        "serializerSettings": {"includeNulls": True},
    }

    try:
        resp = requests.post(PBI_URL, headers=headers, json=dax_query, timeout=60)
        logging.info(f"📥 Power BI tenure_info status: {resp.status_code}")
        logging.debug(f"Power BI response: {resp.text}")

        if resp.status_code != 200:
            await update.message.reply_text("❌ Не вдалося отримати дані. Спробуйте пізніше.")
            return

        data = resp.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
    except Exception as e:
        logging.error(f"❌ Помилка запиту до Power BI: {e}")
        await update.message.reply_text("❌ Не вдалося отримати дані. Спробуйте пізніше.")
        return

    if not rows:
        await update.message.reply_text(f"ℹ️ Не знайдено співробітника: {employee}")
        return

    # перший запис (очікується унікальний)
    row = rows[0]
    message = _build_message(row)
    await update.message.reply_text(message)

    # навігація
    kb = [[KeyboardButton("Назад")], [KeyboardButton("Головне меню")]]
    await update.message.reply_text(
        "Виберіть опцію:",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True)
    )

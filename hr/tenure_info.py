# hr/tenure_info.py
# -*- coding: utf-8 -*-
import logging
from datetime import date, datetime

import pandas as pd
import requests
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext

from auth import get_power_bi_token
from utils.name_aliases import display_name

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATASET_ID = "8b80be15-7b31-49e4-bc85-8b37a0d98f1c"
PBI_URL = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"


def _diff_ymd(start: date, end: date):
    """Різниця у роках/місяцях/днях між двома датами."""
    if start > end:
        start, end = end, start
    y = end.year - start.year
    m = end.month - start.month
    d = end.day - start.day
    if d < 0:
        # скільки днів було у попередньому місяці
        prev_month_last = (date(end.year, end.month, 1) - pd.Timedelta(days=1)).date()
        d += prev_month_last.day
        m -= 1
    if m < 0:
        m += 12
        y -= 1
    return y, m, d


def _fmt_date(dt) -> str:
    """Форматування дати у ДД.ММ.РРРР або — якщо порожньо."""
    if dt is None or (isinstance(dt, float) and pd.isna(dt)):
        return "—"
    if isinstance(dt, str):
        try:
            dt = pd.to_datetime(dt, errors="coerce")
        except Exception:
            return str(dt)
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if isinstance(dt, datetime):
        dt = dt.date()
    if isinstance(dt, date):
        return dt.strftime("%d.%m.%Y")
    return "—"


def _build_message(row: dict) -> str:
    """Формує текст повідомлення зі стажем тощо."""
    today = date.today()

    # значення з дужками в ключах
    employee = row.get("[Employee]", "—")
    last_dep = row.get("[LastDepartment]", "—")
    phone_tg = row.get("[PhoneNumberTelegram]", "—")
    code = row.get("[Code]", "—")
    bday = row.get("[birthdayDate]")
    hire = row.get("[hireDate]")

    hire_dt = None
    if hire is not None:
        try:
            hire_ts = pd.to_datetime(hire, errors="coerce")
            if pd.notna(hire_ts):
                hire_dt = hire_ts.date()
        except Exception:
            hire_dt = None

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
        f"📄 Дата прийняття: {_fmt_date(hire_dt)}",
        f"⏳ Стаж: {tenure_text}",
    ]

    if bday is not None:
        lines.append(f"🎂 Дата народження: {_fmt_date(bday)}")

    return "\n".join(lines)


async def show_tenure_info(update: Update, context: CallbackContext) -> None:
    """
    Публічний Telegram-хендлер:
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
    await update.message.reply_text("Виберіть опцію:", reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))

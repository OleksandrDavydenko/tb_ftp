# hr/tenure_info.py
# -*- coding: utf-8 -*-
import os
import requests
import pandas as pd
from datetime import date, datetime

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext

from utils.name_aliases import display_name

# --- Параметри Power BI (ROPC) ---
CLIENT_ID  = os.getenv("PBI_CLIENT_ID",  "706d72b2-a9a2-4d90-b0d8-b08f58459ef6")
USERNAME   = os.getenv("PBI_USERNAME",   "od@ftpua.com")
PASSWORD   = os.getenv("PBI_PASSWORD",   "Hq@ssw0rd356")
DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")
TOKEN_URL  = "https://login.microsoftonline.com/common/oauth2/token"
PBI_SCOPE  = "https://analysis.windows.net/powerbi/api"

def _get_token() -> str:
    body = {
        "grant_type": "password",
        "resource": PBI_SCOPE,
        "client_id": CLIENT_ID,
        "username": USERNAME,
        "password": PASSWORD,
    }
    r = requests.post(TOKEN_URL, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]

def _exec_dax(token: str, dax: str) -> dict:
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def _to_dataframe(result_json: dict) -> pd.DataFrame:
    results = result_json.get("results", [])
    tables  = results[0].get("tables", []) if results else []
    if not tables:
        return pd.DataFrame()
    table = tables[0]
    cols  = [c.get("name") for c in table.get("columns", [])] if table.get("columns") else []
    rows  = table.get("rows", []) or []
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
        else:
            out.append({cols[i]: row[i] for i in range(len(cols))})
    def clean(k: str) -> str:
        return k.split("[", 1)[-1].rstrip("]") if "[" in k else k
    return pd.DataFrame([{clean(k): v for k, v in r.items()} for r in out])

def _query_employee_row(token: str, employee: str) -> pd.DataFrame:
    emp_escaped = (employee or "").replace('"', '""')
    dax = f"""
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
    return _to_dataframe(_exec_dax(token, dax))

def _diff_ymd(start: date, end: date):
    if start > end:
        start, end = end, start
    y = end.year - start.year
    m = end.month - start.month
    d = end.day - start.day
    if d < 0:
        days_in_prev = (date(end.year, end.month, 1) - pd.Timedelta(days=1)).day if end.month != 1 else 31
        d += days_in_prev
        m -= 1
    if m < 0:
        m += 12
        y -= 1
    return y, m, d

def _fmt_date(dt) -> str:
    if dt is None or (isinstance(dt, float) and pd.isna(dt)):
        return "—"
    if isinstance(dt, str):
        try:
            dt = pd.to_datetime(dt).date()
        except Exception:
            return str(dt)
    if isinstance(dt, datetime):
        dt = dt.date()
    return dt.strftime("%d.%m.%Y")

def _build_message(row: pd.Series) -> str:
    today = date.today()
    hire_raw = row.get("hireDate")
    hire_dt = None
    if pd.notna(hire_raw):
        hire_dt = pd.to_datetime(hire_raw, errors="coerce")
        if pd.notna(hire_dt):
            hire_dt = hire_dt.date()

    tenure_text = "—"
    if hire_dt:
        y, m, d = _diff_ymd(hire_dt, today)
        parts = []
        if y: parts.append(f"{y} р.")
        if m: parts.append(f"{m} міс.")
        if d or not parts: parts.append(f"{d} дн.")
        tenure_text = " ".join(parts)

    nice_name = display_name(row.get("Employee","—"))
    lines = []
    lines.append(f"👤 Співробітник: {nice_name}")
    lines.append(f"🏢 Відділ: {row.get('LastDepartment','—')}")
    lines.append(f"🆔 Код: {row.get('Code','—')}")
    lines.append(f"📱 Telegram: {row.get('PhoneNumberTelegram','—')}")
    lines.append("")
    lines.append(f"📅 Сьогодні: {today.strftime('%d.%m.%Y')}")
    lines.append(f"📄 Дата прийняття: {_fmt_date(hire_dt)}")
    lines.append(f"⏳ Стаж: {tenure_text}")
    bd = row.get("birthdayDate")
    if pd.notna(bd):
        lines.append(f"🎂 Дата народження: {_fmt_date(bd)}")
    return "\n".join(lines)

# ── ПУБЛІЧНИЙ TELEGRAM-ХЕНДЛЕР ───────────────────────────────────────────────
async def show_tenure_info(update: Update, context: CallbackContext) -> None:
    # очікуємо, що ПІБ вже збережений при авторизації
    employee = context.user_data.get("employee_name")
    if not employee:
        await update.message.reply_text("⚠️ Не знайдено ПІБ користувача. Поділіться контактом або зверніться до HR.")
        return

    try:
        token = _get_token()
        df = _query_employee_row(token, employee)
    except Exception:
        await update.message.reply_text("❌ Не вдалося отримати дані. Спробуйте пізніше.")
        return

    if df.empty:
        await update.message.reply_text(f"⚠️ Не знайдено співробітника: {employee}")
        return

    await update.message.reply_text(_build_message(df.iloc[0]))

    # Невелика навігація
    kb = [[KeyboardButton("Назад")], [KeyboardButton("Головне меню")]]
    await update.message.reply_text("Виберіть опцію:", reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from datetime import datetime
from auth import get_power_bi_token
import requests
import logging

from utils.name_aliases import display_name


POWER_BI_URL = "https://api.powerbi.com/v1.0/myorg/datasets/8b80be15-7b31-49e4-bc85-8b37a0d98f1c/executeQueries"


MONTHS_UA = [
    "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
    "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
]

MONTH_MAP = {
    "Січень": "01", "Лютий": "02", "Березень": "03", "Квітень": "04",
    "Травень": "05", "Червень": "06", "Липень": "07", "Серпень": "08",
    "Вересень": "09", "Жовтень": "10", "Листопад": "11", "Грудень": "12"
}
MONTH_MAP_REV = {v: k for k, v in MONTH_MAP.items()}


def _power_bi_headers():
    token = get_power_bi_token()
    if not token:
        return None
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def _execute_dax(headers: dict, dax: str) -> list[dict]:
    payload = {
        "queries": [{"query": dax}],
        "serializerSettings": {"includeNulls": True}
    }
    resp = requests.post(POWER_BI_URL, headers=headers, json=payload)
    logging.info(f"📥 Статус відповіді Power BI: {resp.status_code}")
    logging.info(f"📄 Вміст відповіді: {resp.text}")

    if resp.status_code != 200:
        return []

    data = resp.json()
    return data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])


def _get_employee_periods(employee_name: str) -> list[str]:
    """
    Повертає список Period (як строки) для конкретного працівника з таблиці workdays_by_employee.
    """
    headers = _power_bi_headers()
    if not headers:
        return []

    dax = f"""
        EVALUATE
        SELECTCOLUMNS(
            FILTER(
                workdays_by_employee,
                workdays_by_employee[Employee] = "{employee_name}"
            ),
            "Period", workdays_by_employee[Period]
        )
    """
    rows = _execute_dax(headers, dax)

    # rows містять ключ '[Period]'
    periods = []
    for r in rows:
        p = r.get("[Period]") or r.get("[Period]".replace("[", "").replace("]", ""))  # на всяк випадок
        p = r.get("[Period]")  # основний варіант
        if p:
            periods.append(str(p))

    # прибираємо дублікати, але зберігаємо порядок
    seen = set()
    uniq = []
    for p in periods:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def _extract_year_month(period_str: str) -> tuple[int | None, int | None]:
    """
    period_str очікуємо як 'YYYY-MM-DD...' або 'DD.MM.YYYY' (якщо раптом).
    Повертає (year, month) або (None, None).
    """
    s = (period_str or "").strip()

    # Найчастіше з PBI приходить ISO 'YYYY-MM-DD...'
    try:
        dt = datetime.fromisoformat(s[:10])
        return dt.year, dt.month
    except Exception:
        pass

    # Якщо раптом dd.mm.yyyy
    try:
        dt = datetime.strptime(s[:10], "%d.%m.%Y")
        return dt.year, dt.month
    except Exception:
        return None, None


async def show_workdays_years(update: Update, context: CallbackContext) -> None:
    context.user_data["menu"] = "workdays_years"

    employee_name = context.user_data.get("employee_name")
    if not employee_name:
        await update.message.reply_text("⚠️ Не знайдено працівника в контексті.")
        return

    periods = _get_employee_periods(employee_name)
    ym = [_extract_year_month(p) for p in periods]
    years = sorted({y for (y, m) in ym if y is not None})

    if not years:
        await update.message.reply_text("ℹ️ Дані по відпрацьованих днях відсутні.")
        return

    keyboard = [[KeyboardButton(str(y))] for y in years]
    keyboard.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])

    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("🗓 Оберіть рік:", reply_markup=reply_markup)


async def show_workdays_months(update: Update, context: CallbackContext) -> None:
    selected_year = update.message.text
    context.user_data["selected_year"] = selected_year
    context.user_data["menu"] = "workdays_months"

    employee_name = context.user_data.get("employee_name")
    if not employee_name:
        await update.message.reply_text("⚠️ Не знайдено працівника в контексті.")
        return

    try:
        year_int = int(selected_year)
    except ValueError:
        await update.message.reply_text("⚠️ Невірний рік.")
        return

    periods = _get_employee_periods(employee_name)
    ym = [_extract_year_month(p) for p in periods]

    months_nums = sorted({m for (y, m) in ym if y == year_int and m is not None})
    if not months_nums:
        await update.message.reply_text("ℹ️ Немає даних за обраний рік.")
        return

    months = [MONTHS_UA[m - 1] for m in months_nums]  # 1..12 -> index 0..11

    keyboard = [[KeyboardButton(month)] for month in months]
    keyboard.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])

    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("📅 Оберіть місяць:", reply_markup=reply_markup)


# show_workdays_details лишаємо твоїм (з VacationOnWeekends) — без змін

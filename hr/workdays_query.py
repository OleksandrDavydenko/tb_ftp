from __future__ import annotations

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
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


# =========================
# Helpers
# =========================
def _to_int(v, default: int = 0) -> int:
    """Перетворює значення з Power BI у ціле число без крапок."""
    if v is None:
        return default
    try:
        # буває float або Decimal-подібне
        return int(round(float(v)))
    except Exception:
        try:
            # буває строка "12" або "12.0"
            return int(round(float(str(v).replace(",", "."))))
        except Exception:
            return default


def _get_headers() -> dict | None:
    token = get_power_bi_token()
    if not token:
        return None
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def _execute_dax(headers: dict, dax_query: str) -> list[dict]:
    payload = {
        "queries": [{"query": dax_query}],
        "serializerSettings": {"includeNulls": True}
    }
    response = requests.post(POWER_BI_URL, headers=headers, json=payload)

    logging.info(f"📥 Статус відповіді Power BI: {response.status_code}")
    logging.info(f"📄 Вміст відповіді: {response.text}")

    if response.status_code != 200:
        return []

    data = response.json()
    return data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])


def _extract_year_month(period_str: str) -> tuple[int | None, int | None]:
    """
    period_str очікуємо як ISO 'YYYY-MM-DD...' або 'DD.MM.YYYY'.
    Повертає (year, month) або (None, None).
    """
    s = (period_str or "").strip()

    # ISO
    try:
        dt = datetime.fromisoformat(s[:10])
        return dt.year, dt.month
    except Exception:
        pass

    # dd.mm.yyyy
    try:
        dt = datetime.strptime(s[:10], "%d.%m.%Y")
        return dt.year, dt.month
    except Exception:
        return None, None


def _get_employee_periods_cached(context: CallbackContext, employee_name: str) -> list[str]:
    """
    Дістає Period для працівника з Power BI і кешує в context.user_data,
    щоб не робити зайві запити при виборі років/місяців.
    """
    cache_key = "workdays_available_periods"
    cache_emp_key = "workdays_available_periods_employee"

    cached = context.user_data.get(cache_key)
    cached_emp = context.user_data.get(cache_emp_key)

    if cached and cached_emp == employee_name:
        return cached

    headers = _get_headers()
    if not headers:
        return []

    emp = employee_name.replace('"', '""')
    dax = f"""
        EVALUATE
        SELECTCOLUMNS(
            FILTER(
                workdays_by_employee,
                workdays_by_employee[TaxCode] IN
                    SELECTCOLUMNS(
                        FILTER(Employees, Employees[Employee] = "{emp}"),
                        "INN", Employees[INN]
                    )
            ),
            "Period", workdays_by_employee[Period]
        )
    """

    rows = _execute_dax(headers, dax)

    periods: list[str] = []
    for r in rows:
        p = r.get("[Period]")
        if p:
            periods.append(str(p))

    # Унікалізація зі збереженням порядку
    seen = set()
    uniq = []
    for p in periods:
        if p not in seen:
            seen.add(p)
            uniq.append(p)

    context.user_data[cache_key] = uniq
    context.user_data[cache_emp_key] = employee_name
    return uniq


# =========================
# Handlers
# =========================
async def show_workdays_years(update: Update, context: CallbackContext) -> None:
    context.user_data["menu"] = "workdays_years"

    employee_name = context.user_data.get("employee_name")
    msg = update.effective_message
    if not employee_name:
        await msg.reply_text("⚠️ Не знайдено працівника в контексті.")
        return

    periods = _get_employee_periods_cached(context, employee_name)
    ym = [_extract_year_month(p) for p in periods]
    years = sorted({y for (y, m) in ym if y is not None})

    nav_kb = ReplyKeyboardMarkup([[KeyboardButton("Назад"), KeyboardButton("Головне меню")]], resize_keyboard=True, one_time_keyboard=True)

    if not years:
        await msg.reply_text("ℹ️ Дані по відпрацьованих днях відсутні.", reply_markup=nav_kb)
        return

    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(str(y), callback_data=f"workdays_year:{y}")] for y in years])
    await msg.reply_text("🗓 Оберіть рік:", reply_markup=inline_kb)
    await msg.reply_text("або поверніться:", reply_markup=nav_kb)


async def show_workdays_months(update: Update, context: CallbackContext) -> None:
    selected_year = context.user_data.get("selected_year")
    context.user_data["menu"] = "workdays_months"

    employee_name = context.user_data.get("employee_name")
    msg = update.effective_message
    if not employee_name:
        await msg.reply_text("⚠️ Не знайдено працівника в контексті.")
        return

    try:
        year_int = int(selected_year)
    except (ValueError, TypeError):
        await msg.reply_text("⚠️ Невірний рік.")
        return

    periods = _get_employee_periods_cached(context, employee_name)
    ym = [_extract_year_month(p) for p in periods]

    months_nums = sorted({m for (y, m) in ym if y == year_int and m is not None})

    nav_kb = ReplyKeyboardMarkup([[KeyboardButton("Назад"), KeyboardButton("Головне меню")]], resize_keyboard=True, one_time_keyboard=True)

    if not months_nums:
        await msg.reply_text("ℹ️ Немає даних за обраний рік.", reply_markup=nav_kb)
        return

    months = [MONTHS_UA[m - 1] for m in months_nums]

    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(month, callback_data=f"workdays_month:{month}")] for month in months])
    await msg.reply_text("📅 Оберіть місяць:", reply_markup=inline_kb)
    await msg.reply_text("або поверніться:", reply_markup=nav_kb)


async def show_workdays_details(update: Update, context: CallbackContext) -> None:
    selected_month = context.user_data.get("selected_month")
    context.user_data["menu"] = "workdays_details"

    employee_name = context.user_data.get("employee_name")
    year = context.user_data.get("selected_year")

    if not employee_name or not year:
        await update.effective_message.reply_text("⚠️ Не знайдено працівника або року в контексті.")
        return

    month_num = MONTH_MAP.get(selected_month)
    if not month_num:
        await update.effective_message.reply_text("⚠️ Невідомий місяць.")
        return

    headers = _get_headers()
    if not headers:
        await update.effective_message.reply_text("❌ Не вдалося отримати токен для Power BI.")
        return

    emp = employee_name.replace('"', '""')
    dax = f"""
        EVALUATE
        SELECTCOLUMNS(
            FILTER(
                workdays_by_employee,
                workdays_by_employee[TaxCode] IN
                    SELECTCOLUMNS(
                        FILTER(Employees, Employees[Employee] = "{emp}"),
                        "INN", Employees[INN]
                    ) &&
                DATEVALUE(workdays_by_employee[Period]) = DATE({year}, {int(month_num)}, 1)
            ),
            "Period", workdays_by_employee[Period],
            "TotalDays", workdays_by_employee[TotalDays],
            "WeekendDays", workdays_by_employee[WeekendDays],
            "HolidayDays", workdays_by_employee[HolidayDays],
            "WorkDays", workdays_by_employee[WorkDays],
            "LeaveWithoutPay", workdays_by_employee[LeaveWithoutPay],
            "RegularVacationDays", workdays_by_employee[RegularVacationDays],
            "VacationOnWeekends", workdays_by_employee[VacationOnWeekends],
            "SickLeaveDays", workdays_by_employee[SickLeaveDays],
            "WorkedDays", workdays_by_employee[WorkedDays]
        )
    """

    rows = _execute_dax(headers, dax)

    if not rows:
        await update.effective_message.reply_text("ℹ️ Дані по відпрацьованих днях відсутні.")
        return

    row = rows[0]
    nice_name = display_name(employee_name)

    period_val = (row.get("[Period]") or "")[:10]

    total_days = _to_int(row.get("[TotalDays]", 0))
    work_days = _to_int(row.get("[WorkDays]", 0))
    weekend_days = _to_int(row.get("[WeekendDays]", 0))
    holiday_days = _to_int(row.get("[HolidayDays]", 0))
    leave_wo_pay = _to_int(row.get("[LeaveWithoutPay]", 0))
    regular_vac = _to_int(row.get("[RegularVacationDays]", 0))
    vac_on_non_working = _to_int(row.get("[VacationOnWeekends]", 0))
    sick_days = _to_int(row.get("[SickLeaveDays]", 0))
    worked_days = _to_int(row.get("[WorkedDays]", 0))

    message = (
        f"📅 Період: {period_val}\n"
        f"👤 Працівник: {nice_name}\n"
        f"📊 Всього днів: {total_days}\n"
        f"📆 Робочі дні: {work_days}\n"
        f"🛌 Вихідні дні: {weekend_days}\n"
        f"🎉 Святкові дні: {holiday_days}\n"
        f"🚫 Відпустка за свій рахунок: {leave_wo_pay}\n"
        f"🏖 Звичайна відпустка: {regular_vac}\n"
        f"🤒 Лікарняні: {sick_days}\n"
        f"✅ Відпрацьовано: {worked_days}\n"
        f"\n"
        f"Відпустка у неробочі дні: {vac_on_non_working}\n"
    )

    await update.effective_message.reply_text(message)

    keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.effective_message.reply_text("⬅️ Оберіть дію:", reply_markup=reply_markup)

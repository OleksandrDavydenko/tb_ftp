from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from datetime import datetime
from auth import get_power_bi_token
import requests
import logging

async def show_workdays_years(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'workdays_years'
    years = ["2024", "2025"]
    keyboard = [[KeyboardButton(year)] for year in years] + [[KeyboardButton("Назад")]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("🗓 Оберіть рік:", reply_markup=reply_markup)


async def show_workdays_months(update: Update, context: CallbackContext) -> None:
    selected_year = update.message.text
    context.user_data['selected_year'] = selected_year
    context.user_data['menu'] = 'workdays_months'

    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    keyboard = [[KeyboardButton(month)] for month in months]
    keyboard.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("📅 Оберіть місяць:", reply_markup=reply_markup)


async def show_workdays_details(update: Update, context: CallbackContext) -> None:
    selected_month = update.message.text
    context.user_data['selected_month'] = selected_month
    context.user_data['menu'] = 'workdays_details'

    employee_name = context.user_data.get('employee_name')
    year = context.user_data.get('selected_year')

    month_map = {
        "Січень": "01", "Лютий": "02", "Березень": "03", "Квітень": "04",
        "Травень": "05", "Червень": "06", "Липень": "07", "Серпень": "08",
        "Вересень": "09", "Жовтень": "10", "Листопад": "11", "Грудень": "12"
    }
    month_num = month_map.get(selected_month)
    if not month_num:
        await update.message.reply_text("⚠️ Невідомий місяць.")
        return

    token = get_power_bi_token()
    if not token:
        await update.message.reply_text("❌ Не вдалося отримати токен для Power BI.")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    dax_query = {
        "queries": [
            {
                "query": f"""
                    EVALUATE
                    SELECTCOLUMNS(
                        FILTER(
                            workdays_by_employee,
                            workdays_by_employee[Employee] = \"{employee_name}\" &&
                            workdays_by_employee[Period] = DATE({year}, {int(month_num)}, 1)
                        ),
                        \"Period\", workdays_by_employee[Period],
                        \"TotalDays\", workdays_by_employee[TotalDays],
                        \"WeekendDays\", workdays_by_employee[WeekendDays],
                        \"HolidayDays\", workdays_by_employee[HolidayDays],
                        \"WorkDays\", workdays_by_employee[WorkDays],
                        \"LeaveWithoutPay\", workdays_by_employee[LeaveWithoutPay],
                        \"RegularVacationDays\", workdays_by_employee[RegularVacationDays],
                        \"SickLeaveDays\", workdays_by_employee[SickLeaveDays],
                        \"WorkedDays\", workdays_by_employee[WorkedDays]
                    )
                """
            }
        ],
        "serializerSettings": {"includeNulls": True}
    }

    power_bi_url = "https://api.powerbi.com/v1.0/myorg/datasets/8b80be15-7b31-49e4-bc85-8b37a0d98f1c/executeQueries"
    response = requests.post(power_bi_url, headers=headers, json=dax_query)

    logging.info(f"📥 Статус відповіді Power BI: {response.status_code}")
    logging.info(f"📄 Вміст відповіді: {response.text}")

    if response.status_code != 200:
        await update.message.reply_text("❌ Помилка при отриманні даних з Power BI.")
        return

    data = response.json()
    rows = data['results'][0]['tables'][0].get('rows', [])

    if not rows:
        await update.message.reply_text("ℹ️ Дані по відпрацьованих днях відсутні.")
        return

    row = rows[0]
    message = (
        f"📅 Період: {row['[Period]'][:10]}\n"
        f"👤 Працівник: {employee_name}\n"
        f"📊 Всього днів: {row['[TotalDays]']}\n"
        f"📆 Робочі дні: {row['[WorkDays]']}\n"
        f"🛌 Вихідні дні: {row['[WeekendDays]']}\n"
        f"🎉 Святкові дні: {row['[HolidayDays]']}\n"
        f"🚫 Відпустка за свій рахунок: {row['[LeaveWithoutPay]']}\n"
        f"🏖 Звичайна відпустка: {row['[RegularVacationDays]']}\n"
        f"🤒 Лікарняні: {row['[SickLeaveDays]']}\n"
        f"✅ Відпрацьовано: {row['[WorkedDays]']}\n"
    )

    await update.message.reply_text(message)

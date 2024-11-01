from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import datetime
from .analytics_table import get_income_data, format_analytics_table

async def show_analytics_years(update: Update, context: CallbackContext) -> None:
    current_year = datetime.datetime.now().year
    years = [str(year) for year in range(2024, current_year + 1)]
    custom_keyboard = [[KeyboardButton(year)] for year in years]
    custom_keyboard.append([KeyboardButton("Назад")])
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    context.user_data['menu'] = 'analytics_years'
    await update.message.reply_text("Оберіть рік:", reply_markup=reply_markup)

async def show_analytics_months(update: Update, context: CallbackContext) -> None:
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    custom_keyboard = [[KeyboardButton(month)] for month in months]
    custom_keyboard.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    context.user_data['menu'] = 'analytics_months'
    await update.message.reply_text("Оберіть місяць:", reply_markup=reply_markup)

# Функція для відображення аналітики
async def show_analytics_details(update: Update, context: CallbackContext) -> None:
    employee_name = context.user_data.get('employee_name')  # Отримуємо ім'я працівника
    year = context.user_data.get('selected_year')           # Отримуємо вибраний рік
    month = context.user_data.get('selected_month')         # Отримуємо вибраний місяць

    if not employee_name or not year or not month:
        await update.message.reply_text("Помилка: необхідно вибрати рік і місяць.")
        return

    # Отримуємо дані аналітики
    manager_income = get_income_data(employee_name, "Менеджер", year, month)
    sales_income = get_income_data(employee_name, "Сейлс", year, month)

    # Форматування таблиці з додаванням місяця та року
    formatted_table = format_analytics_table(manager_income or sales_income, employee_name, month, year)
    await update.message.reply_text(f"```\n{formatted_table}\n```", parse_mode="Markdown")

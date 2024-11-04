from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import datetime
from .analytics_table import get_income_data, format_analytics_table
from .analytics_chart import show_yearly_chart
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Відображення вибору аналітики (за місяць або за рік)
async def show_analytics_options(update: Update, context: CallbackContext) -> None:
    options_keyboard = [
        [KeyboardButton("Аналітика за місяць"), KeyboardButton("Аналітика за рік")],
        [KeyboardButton("Головне меню")]
    ]
    reply_markup = ReplyKeyboardMarkup(options_keyboard, one_time_keyboard=True)
    await update.message.reply_text("Оберіть тип аналітики:", reply_markup=reply_markup)

# Відображення років для аналітики
async def show_analytics_years(update: Update, context: CallbackContext) -> None:
    current_year = datetime.datetime.now().year
    years = [str(year) for year in range(2024, current_year + 1)]
    custom_keyboard = [[KeyboardButton(year)] for year in years]
    custom_keyboard.append([KeyboardButton("Назад")])
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    context.user_data['menu'] = 'analytics_years'
    await update.message.reply_text("Оберіть рік:", reply_markup=reply_markup)

# Відображення місяців для помісячної аналітики
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

# Відображення аналітики за місяць
async def show_monthly_analytics(update: Update, context: CallbackContext) -> None:
    employee_name = context.user_data.get('employee_name')
    year = context.user_data.get('selected_year')
    month = context.user_data.get('selected_month')

    if not employee_name or not year or not month:
        await update.message.reply_text("Помилка: необхідно вибрати рік і місяць.")
        return

    income_data = get_income_data(employee_name, "Менеджер", year, month) or get_income_data(employee_name, "Сейлс", year, month)
    if not income_data:
        await update.message.reply_text("Немає даних для вибраного періоду.")
        return

    formatted_table = format_analytics_table(income_data, employee_name, month, year)
    await update.message.reply_text(f"```\n{formatted_table}\n```", parse_mode="Markdown")

# Відображення аналітики за рік (тільки для графіків або інший річний аналіз)
async def show_yearly_analytics(update: Update, context: CallbackContext):
    employee_name = context.user_data.get('employee_name')
    year = context.user_data.get('selected_year')

    if not employee_name or not year:
        await update.message.reply_text("Помилка: необхідно вибрати рік.")
        return

    # Виклик річного графіка
    await show_yearly_chart(update, context, employee_name, year)
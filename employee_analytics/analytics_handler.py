from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import datetime
from .analytics_table import get_income_data, format_analytics_table
from .analytics_chart import show_yearly_chart
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Відображення вибору типу аналітики
async def show_analytics_options(update: Update, context: CallbackContext) -> None:
    custom_keyboard = [[KeyboardButton("Аналітика за місяць"), KeyboardButton("Аналітика за рік")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)
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

# Відображення місяців для аналітики за місяць
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

    # Отримання даних за місяць
    income_data = get_income_data(employee_name, "Менеджер", year, month) or get_income_data(employee_name, "Сейлс", year, month)
    
    if not income_data:
        await update.message.reply_text("Немає даних для вибраного періоду.")
        return

    # Логування отриманих даних
    logging.info(f"Отримані дані для аналітики {employee_name}: {income_data}")

    # Форматування та відображення таблиці
    formatted_table = format_analytics_table(income_data, employee_name, month, year)
    await update.message.reply_text(f"```\n{formatted_table}\n```", parse_mode="Markdown")

# Відображення аналітики за рік з побудовою графіка
async def show_yearly_analytics(update: Update, context: CallbackContext) -> None:
    employee_name = context.user_data.get('employee_name')
    year = context.user_data.get('selected_year')

    if not employee_name or not year:
        await update.message.reply_text("Помилка: необхідно вибрати рік.")
        return

    # Побудова та відображення графіка за рік
    await show_yearly_chart(update, context, employee_name, year)

# Основний обробник для вибору аналітики
async def show_analytics_details(update: Update, context: CallbackContext) -> None:
    choice = update.message.text

    if choice == "Аналітика за місяць":
        await show_analytics_years(update, context)
        context.user_data['analytics_type'] = 'monthly'
    elif choice == "Аналітика за рік":
        await show_analytics_years(update, context)
        context.user_data['analytics_type'] = 'yearly'
    else:
        await update.message.reply_text("Помилка вибору. Оберіть аналітику за місяць або за рік.")

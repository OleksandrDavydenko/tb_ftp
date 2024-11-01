from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import datetime
from .analytics_table import get_analytics_data, format_analytics_table  # Functions to fetch and format data

# Function to display available years for analytics
async def show_analytics_years(update: Update, context: CallbackContext) -> None:
    current_year = datetime.datetime.now().year
    years = [str(year) for year in range(2024, current_year + 1)]

    # Create year buttons and add a "Back" button
    custom_keyboard = [[KeyboardButton(year)] for year in years]
    custom_keyboard.append([KeyboardButton("Назад")])
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    context.user_data['menu'] = 'analytics_years'  # Set context for year selection menu
    await update.message.reply_text("Оберіть рік:", reply_markup=reply_markup)

# Function to display available months for analytics
async def show_analytics_months(update: Update, context: CallbackContext) -> None:
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]

    # Create month buttons and add "Back" and "Main Menu" buttons
    custom_keyboard = [[KeyboardButton(month)] for month in months]
    custom_keyboard.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    context.user_data['menu'] = 'analytics_months'  # Set context for month selection menu
    await update.message.reply_text("Оберіть місяць:", reply_markup=reply_markup)

# Function to display analytics data for the selected period
async def show_analytics_details(update: Update, context: CallbackContext) -> None:
    employee_name = context.user_data.get('employee_name')  # Get the employee name
    year = context.user_data.get('selected_year')           # Get selected year
    month = context.user_data.get('selected_month')         # Get selected month

    if not employee_name or not year or not month:
        await update.message.reply_text("Помилка: необхідно вибрати рік і місяць.")
        return

    # Fetch analytics data (currently empty)
    analytics_data = get_analytics_data(employee_name, year, month)

    if analytics_data:
        formatted_table = format_analytics_table(analytics_data, employee_name, year, month)
        await update.message.reply_text(f"```\n{formatted_table}\n```", parse_mode="Markdown")
    else:
        await update.message.reply_text("Немає даних для вибраного періоду.")

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import datetime
from .analytics_table import get_income_data, format_analytics_table
from .analytics_chart import show_yearly_chart_for_parameter  # Оновлена функція для відображення графіка
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

# Відображення параметрів для вибору графіка за рік
async def show_yearly_parameters(update: Update, context: CallbackContext) -> None:
    parameter_keyboard = [
        [KeyboardButton("Дохід"), KeyboardButton("Валовий прибуток"), KeyboardButton("Маржинальність"), KeyboardButton("Кількість угод")],
        [KeyboardButton("Назад"), KeyboardButton("Головне меню")]
    ]
    reply_markup = ReplyKeyboardMarkup(parameter_keyboard, one_time_keyboard=True)
    context.user_data['menu'] = 'analytics_parameters'
    await update.message.reply_text("Оберіть параметр для аналізу:", reply_markup=reply_markup)

# Відображення аналітики за рік для обраного параметра
async def show_yearly_analytics(update: Update, context: CallbackContext):
    employee_name = context.user_data.get('employee_name')
    year = context.user_data.get('selected_year')
    selected_parameter = context.user_data.get('selected_parameter')

    if not employee_name or not year or not selected_parameter:
        await update.message.reply_text("Помилка: необхідно вибрати рік і параметр.")
        return

    # Виклик річного графіка для обраного параметра
    await show_yearly_chart_for_parameter(update, context, employee_name, year, selected_parameter)
    
    
    # Додаємо кнопки "Назад" та "Головне меню"
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)

    # Відправляємо повідомлення з кнопками
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)


# Обробка вибору параметра для аналітики за рік
async def handle_yearly_parameter_selection(update: Update, context: CallbackContext) -> None:
    parameter = update.message.text
    context.user_data['selected_parameter'] = parameter
    await show_yearly_analytics(update, context)

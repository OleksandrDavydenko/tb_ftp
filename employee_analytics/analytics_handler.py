from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackContext
import datetime
from .analytics_table import (
    get_income_data, format_analytics_table,
    get_available_years_analytics, get_available_months_analytics,
    get_yearly_breakdown, format_smart_monthly_card, MONTHS_UA
)
from .analytics_chart import show_yearly_chart_for_parameter, show_yearly_dashboard
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
    employee = context.user_data.get("employee_name")
    years = get_available_years_analytics(employee) if employee else []
    context.user_data['menu'] = 'analytics_years'
    msg = update.effective_message
    nav_kb = ReplyKeyboardMarkup([[KeyboardButton("Назад"), KeyboardButton("Головне меню")]], resize_keyboard=True, one_time_keyboard=True)
    if not years:
        await msg.reply_text("ℹ️ Немає доступних років аналітики.", reply_markup=nav_kb)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(y, callback_data=f"analytics_year:{y}")] for y in years])
    await msg.reply_text("Оберіть рік:", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=nav_kb)


# Відображення місяців для помісячної аналітики
async def show_analytics_months(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    year = context.user_data.get("selected_year")
    months = get_available_months_analytics(employee, year) if (employee and year) else []
    context.user_data['menu'] = 'analytics_months'
    msg = update.effective_message
    nav_kb = ReplyKeyboardMarkup([[KeyboardButton("Назад"), KeyboardButton("Головне меню")]], resize_keyboard=True, one_time_keyboard=True)
    if not months:
        await msg.reply_text("ℹ️ Немає доступних місяців за обраний рік.", reply_markup=nav_kb)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(m, callback_data=f"analytics_month:{m}")] for m in months])
    await msg.reply_text("Оберіть місяць:", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=nav_kb)


# Відображення аналітики за місяць (розумна картка)
async def show_monthly_analytics(update: Update, context: CallbackContext) -> None:
    employee_name = context.user_data.get('employee_name')
    year  = context.user_data.get('selected_year')
    month = context.user_data.get('selected_month')

    if not employee_name or not year or not month:
        await update.message.reply_text("Помилка: необхідно вибрати рік і місяць.")
        return

    income_data = (get_income_data(employee_name, "Менеджер", year, month) or
                   get_income_data(employee_name, "Сейлс", year, month))

    nav_kb = ReplyKeyboardMarkup([[KeyboardButton("Назад"), KeyboardButton("Головне меню")]], one_time_keyboard=True, resize_keyboard=True)

    context.user_data['menu'] = 'analytics_monthly_card'

    if not income_data:
        await update.effective_message.reply_text("Немає даних для вибраного періоду.", reply_markup=nav_kb)
        return

    # Попередній місяць
    try:
        month_idx = MONTHS_UA.index(month)
    except ValueError:
        month_idx = -1

    if month_idx == 0:
        prev_month, prev_year = MONTHS_UA[11], str(int(year) - 1)
    elif month_idx > 0:
        prev_month, prev_year = MONTHS_UA[month_idx - 1], year
    else:
        prev_month, prev_year = None, None

    previous_data = None
    if prev_month:
        previous_data = (get_income_data(employee_name, "Менеджер", prev_year, prev_month) or
                         get_income_data(employee_name, "Сейлс", prev_year, prev_month))

    ytd_months = get_yearly_breakdown(employee_name, year)

    card = format_smart_monthly_card(income_data, previous_data, ytd_months, employee_name, month, year)
    await update.effective_message.reply_text(card)
    await update.effective_message.reply_text("Виберіть опцію:", reply_markup=nav_kb)

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

    # Додаємо кнопки "Назад" та "Головне меню" після показу річної аналітики
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

# Обробка вибору параметра для аналітики за рік
async def handle_yearly_parameter_selection(update: Update, context: CallbackContext) -> None:
    parameter = update.message.text
    context.user_data['selected_parameter'] = parameter
    await show_yearly_analytics(update, context)

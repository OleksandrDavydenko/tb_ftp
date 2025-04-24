import asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import datetime
from .salary_queries import get_salary_data, get_salary_payments, get_bonuses, format_salary_table

# Відображення списку років
async def show_salary_years(update: Update, context: CallbackContext) -> None:
    current_year = datetime.datetime.now().year
    years = [str(year) for year in range(2025, current_year + 1)]
    custom_keyboard = [[KeyboardButton(year)] for year in years] + [[KeyboardButton("Назад")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)
    context.user_data['menu'] = 'salary_years'
    await update.message.reply_text("Оберіть рік:", reply_markup=reply_markup)

# Відображення списку місяців
async def show_salary_months(update: Update, context: CallbackContext) -> None:
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    custom_keyboard = [[KeyboardButton(month)] for month in months] + [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)
    context.user_data['menu'] = 'salary_months'
    await update.message.reply_text("Оберіть місяць:", reply_markup=reply_markup)

# Відображення розрахункового листа
async def show_salary_details(update: Update, context: CallbackContext) -> None:
    employee_name = context.user_data.get('employee_name')
    year = context.user_data.get('selected_year')
    month_name = context.user_data.get('selected_month')

    if not employee_name or not year or not month_name:
        await update.message.reply_text("Помилка: необхідно вибрати рік і місяць.")
        return

    months_mapping = {
        "Січень": 1, "Лютий": 2, "Березень": 3, "Квітень": 4, "Травень": 5, "Червень": 6,
        "Липень": 7, "Серпень": 8, "Вересень": 9, "Жовтень": 10, "Листопад": 11, "Грудень": 12
    }
    month_number = months_mapping[month_name]

    salary_data = get_salary_data(employee_name, year, month_name)
    payments_data = get_salary_payments(employee_name, year, month_name)
    bonuses_data = get_bonuses(employee_name, year, month_name)

    if salary_data or payments_data or bonuses_data:
        main_table, bonus_table = format_salary_table(salary_data, employee_name, int(year), month_number, payments_data, bonuses_data)

        if main_table:
            salary_message = await update.message.reply_text(f"```{main_table}```", parse_mode="Markdown")
            delete_warning = await update.message.reply_text("⚠️ Розрахунковий лист буде видалено через 60 секунд!")
            asyncio.create_task(delete_salary_message(update, context, salary_message.message_id, delete_warning.message_id, delay=60))

        if bonus_table:
            await update.message.reply_text(f"```{bonus_table}```", parse_mode="Markdown")
    else:
        await update.message.reply_text("Немає даних для вибраного періоду.")

    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

# Видалення повідомлень після затримки
async def delete_salary_message(update: Update, context: CallbackContext, salary_message_id: int, warning_message_id: int, delay: int = 60):
    await asyncio.sleep(delay)
    chat_id = update.message.chat_id
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=salary_message_id)
        await context.bot.delete_message(chat_id=chat_id, message_id=warning_message_id)
    except Exception as e:
        print(f"Помилка видалення розрахункового листа: {e}")

import asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import datetime
from .salary_queries import get_salary_data, get_salary_payments, get_bonuses, format_salary_table

# Функція для відображення списку доступних років
async def show_salary_years(update: Update, context: CallbackContext) -> None:
    current_year = datetime.datetime.now().year
    years = [str(year) for year in range(2025, current_year + 1)]

    custom_keyboard = [[KeyboardButton(year)] for year in years]
    custom_keyboard.append([KeyboardButton("Назад")])
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    context.user_data['menu'] = 'salary_years'
    await update.message.reply_text("Оберіть рік:", reply_markup=reply_markup)

# Функція для відображення списку місяців
async def show_salary_months(update: Update, context: CallbackContext) -> None:
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]

    custom_keyboard = [[KeyboardButton(month)] for month in months]
    custom_keyboard.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    context.user_data['menu'] = 'salary_months'
    await update.message.reply_text("Оберіть місяць:", reply_markup=reply_markup)

# Функція для відображення розрахункового листа
async def show_salary_details(update: Update, context: CallbackContext) -> None:
    employee_name = context.user_data.get('employee_name')
    year = context.user_data.get('selected_year')
    month = context.user_data.get('selected_month')

    if not employee_name or not year or not month:
        await update.message.reply_text("Помилка: необхідно вибрати рік і місяць.")
        return

    salary_data = get_salary_data(employee_name, year, month)
    payments_data = get_salary_payments(employee_name, year, month)
    bonuses_data = get_bonuses(employee_name, year, month)

    if salary_data or payments_data or bonuses_data:
        # Формуємо розрахунковий лист
        formatted_table = format_salary_table(salary_data, employee_name, year, month, payments_data, bonuses_data)
        main_table = formatted_table.split("\nБонуси:")[0].strip()




        bonuses_table = formatted_table.split("\nБонуси:")[1].strip() if "\nБонуси:" in formatted_table else ""

        bonuses_section = f"\nБонуси:\n\n```\n{bonuses_table}\n```" if bonuses_table else ""

        final_message = (
            f"Розрахунковий лист:\n"
            f"{employee_name} за {month} {year}:\n\n"
            f"```\n{main_table}\n```"
            f"{bonuses_section}"
        )

        # Відправляємо розрахунковий лист
        salary_message = await update.message.reply_text(final_message, parse_mode="Markdown")

        # Повідомлення про авто-видалення через 60 секунд
        delete_warning = await update.message.reply_text("⚠️ Розрахунковий лист буде видалено через 60 секунд!")

        # Запускаємо видалення через 60 секунд
        asyncio.create_task(delete_salary_message(update, context, salary_message.message_id, delete_warning.message_id, delay=60))
    else:
        await update.message.reply_text("Немає даних для вибраного періоду.")

    # Кнопки навігації
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

# Функція для видалення повідомлень
async def delete_salary_message(update: Update, context: CallbackContext, salary_message_id: int, warning_message_id: int, delay: int = 60):
    await asyncio.sleep(delay)
    chat_id = update.message.chat_id
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=salary_message_id)
        await context.bot.delete_message(chat_id=chat_id, message_id=warning_message_id)
    except Exception as e:
        print(f"Помилка видалення розрахункового листа: {e}")

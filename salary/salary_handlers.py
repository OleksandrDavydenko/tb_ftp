from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import datetime
from .salary_queries import get_salary_data, get_salary_payments, format_salary_table  # Додано імпорт функції для платежів

# Функція для відображення списку доступних років
async def show_salary_years(update: Update, context: CallbackContext) -> None:
    current_year = datetime.datetime.now().year
    years = [str(year) for year in range(2024, current_year + 1)]

    # Створюємо кнопки з роками та додаємо кнопку "Назад"
    custom_keyboard = [[KeyboardButton(year)] for year in years]
    custom_keyboard.append([KeyboardButton("Назад")])  # Додаємо кнопку "Назад"
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    context.user_data['menu'] = 'salary_years'  # Контекст для меню вибору року
    await update.message.reply_text("Оберіть рік:", reply_markup=reply_markup)

# Функція для відображення списку місяців
async def show_salary_months(update: Update, context: CallbackContext) -> None:
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]

    # Створюємо кнопки з місяцями та кнопки "Назад" і "Головне меню"
    custom_keyboard = [[KeyboardButton(month)] for month in months]
    custom_keyboard.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])  # Додаємо кнопки "Назад" і "Головне меню"
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    context.user_data['menu'] = 'salary_months'  # Контекст для меню вибору місяців
    await update.message.reply_text("Оберіть місяць:", reply_markup=reply_markup)

# Функція для показу розрахункової таблиці за вибраний період
async def show_salary_details(update: Update, context: CallbackContext) -> None:
    employee_name = context.user_data.get('first_name')  # Отримуємо ім'я користувача
    year = context.user_data.get('selected_year')        # Отримуємо вибраний рік
    month = context.user_data.get('selected_month')      # Отримуємо вибраний місяць
    phone_number = context.user_data.get('phone_number') # Отримуємо номер телефону

    # Перевірка, чи всі необхідні дані отримані
    if not employee_name or not year or not month or not phone_number:
        await update.message.reply_text("Помилка: необхідно вибрати рік, місяць та вказати номер телефону.")
        return

    # Отримання даних про нарахування
    salary_data = get_salary_data(employee_name, year, month)

    # Отримання даних про виплати, з додаванням phone_number
    payments_data = get_salary_payments(employee_name, year, month, phone_number)

    # Форматування та відображення результатів, якщо є дані
    if salary_data or payments_data:
        formatted_table = format_salary_table(salary_data, employee_name, year, month, payments_data)
        await update.message.reply_text(f"```\n{formatted_table}\n```", parse_mode="Markdown")
    else:
        await update.message.reply_text("Немає даних для вибраного періоду.")


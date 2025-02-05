from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import datetime
from .salary_queries import get_salary_data, get_salary_payments, get_bonuses, format_salary_table  # Додано імпорт функції для платежів

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
    employee_name = context.user_data.get('employee_name')  # Отримуємо ім'я користувача
    year = context.user_data.get('selected_year')        # Отримуємо вибраний рік
    month = context.user_data.get('selected_month')      # Отримуємо вибраний місяць

    if not employee_name or not year or not month:
        await update.message.reply_text("Помилка: необхідно вибрати рік і місяць.")
        return

    salary_data = get_salary_data(employee_name, year, month)
    payments_data = get_salary_payments(employee_name, year, month)
    bonuses_data = get_bonuses(employee_name, year, month)  # Додаємо отримання бонусів

    if salary_data or payments_data or bonuses_data:
        # Формуємо таблицю з урахуванням бонусів
        formatted_table = format_salary_table(salary_data, employee_name, year, month, payments_data, bonuses_data)
        
        # Заголовок без кавичок
        await update.message.reply_text(f"Розрахунковий лист:\n{employee_name} за {month} {year}:")

        # Таблиця з нарахуваннями та виплатами у форматі коду
        main_table = formatted_table.split("\nБонуси:")[0].strip()  # Виділяємо частину до "Бонуси"
        await update.message.reply_text(f"```\n{main_table}\n```", parse_mode="Markdown")

        # Текст "Бонуси" без кавичок
        await update.message.reply_text("Бонуси:")

        # Таблиця з бонусами та виплатами у форматі коду
        bonuses_table = formatted_table.split("\nБонуси:")[1].strip() if "\nБонуси:" in formatted_table else "Немає даних про бонуси."
        await update.message.reply_text(f"```\n{bonuses_table}\n```", parse_mode="Markdown")
    else:
        await update.message.reply_text("Немає даних для вибраного періоду.")

    # Додаємо кнопки "Назад" та "Головне меню"
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)

    # Відправляємо повідомлення з кнопками
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)
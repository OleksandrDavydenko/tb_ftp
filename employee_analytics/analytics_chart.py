from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import matplotlib.pyplot as plt
from io import BytesIO
from .analytics_table import get_income_data, get_available_months_analytics
import logging
from datetime import datetime
import pytz


from utils.name_aliases import display_name

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для побудови річного графіка за обраним параметром
async def show_yearly_chart_for_parameter(update: Update, context: CallbackContext, employee_name: str, year: str, parameter: str):
    # Повідомлення користувачу про очікування
    await update.message.reply_text("Зачекайте, будь ласка. Це може зайняти деякий час...")
    nice_name = display_name(employee_name)

    # Місяці для отримання даних та побудови графіка
    months = get_available_months_analytics(employee_name, year) or []
    monthly_values = []
    if not months:
        custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
        reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text(f"Для {nice_name} немає даних за {year} рік.", reply_markup=reply_markup)
        return


    # Визначення параметра для отримання даних
    parameter_column = {
        "Дохід": "[Sum USD]",
        "Валовий прибуток": "[Gross Profit]",
        "Маржинальність": "[Margin Percentage]",
        "Кількість угод": "[Deal Count]"
    }.get(parameter)

    # Перевірка на випадок, якщо обраний параметр недоступний
    if not parameter_column:
        await update.message.reply_text("Обраний параметр не підтримується.")
        return

    # Отримання даних про обраний параметр за кожен місяць року
    for month in months:
        income_data = get_income_data(employee_name, "Менеджер", year, month) or get_income_data(employee_name, "Сейлс", year, month)
        
        # Розрахунок маржинальності, якщо вибрано цей параметр
        if parameter == "Маржинальність":
            income = income_data.get("[Sum USD]", 0) if income_data else 0
            gross_profit = income_data.get("[Gross Profit]", 0) if income_data else 0
            value = (gross_profit / income * 100) if income else 0
        else:
            value = income_data.get(parameter_column, 0) if income_data else 0
        
        monthly_values.append(value)

    # Додавання кнопок "Назад" та "Головне меню" для навігації
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)

    # Перевірка на випадок, якщо всі значення рівні нулю
    if all(value == 0 for value in monthly_values):
        await update.message.reply_text(f"Для {nice_name} немає інформації за {year} рік.", reply_markup=reply_markup)
        logging.info(f"Немає даних для графіка {parameter.lower()} для {employee_name} за {year} рік.")
        return

    # Побудова графіка з більшим розміром
    plt.figure(figsize=(12, 8))
    plt.plot(months, monthly_values, marker='o', label=parameter)

    # Додавання значень біля точок
    for i, value in enumerate(monthly_values):
        plt.annotate(f"{value:.2f}", (months[i], monthly_values[i]), textcoords="offset points", xytext=(0, 10), ha='center')

    plt.title(f"Аналітика {parameter.lower()} {nice_name} за {year} рік")
    plt.xlabel("Місяці")
    plt.ylabel(
        "Маржинальність (%)" if parameter == "Маржинальність"
        else (parameter if parameter == "Кількість угод" else f"{parameter} (USD)")
    )
    plt.xticks(rotation=45)
    plt.grid()
    plt.legend()

    # Додавання підпису з датою та часом формування в лівому верхньому куті (київський час)
    kyiv_timezone = pytz.timezone("Europe/Kyiv")
    current_datetime = datetime.now(kyiv_timezone).strftime("%Y-%m-%d %H:%M")
    plt.figtext(0.01, 0.98, f"Згенеровано ботом FTP | Дата формування: {current_datetime}", ha="left", fontsize=8, color="gray", va="top")

    # Збереження графіка як зображення
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()

    # Відправка графіка як зображення
    await update.message.reply_photo(photo=buffer)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)
    logging.info(f"Графік {parameter.lower()} для {employee_name} за {year} рік відображено.")


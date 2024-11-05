import matplotlib.pyplot as plt
from io import BytesIO
from telegram import Update
from telegram.ext import CallbackContext
from .analytics_table import get_income_data
import logging
from datetime import datetime
import pytz

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для побудови річного графіка за обраним параметром
async def show_yearly_chart_for_parameter(update: Update, context: CallbackContext, employee_name: str, year: str, parameter: str):
    # Повідомлення користувачу про очікування
    await update.message.reply_text("Зачекайте, будь ласка. Це може зайняти деякий час...")

    # Місяці для отримання даних та побудови графіка
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    monthly_values = []

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

    # Побудова графіка з більшим розміром
    plt.figure(figsize=(12, 8))  # Збільшений розмір графіка
    plt.plot(months, monthly_values, marker='o', label=parameter)
    plt.title(f"Аналітика {parameter.lower()} {employee_name} за {year} рік")
    plt.xlabel("Місяці")
    plt.ylabel(f"{parameter} (USD)" if parameter not in ["Кількість угод", "Маржинальність"] else parameter)
    plt.xticks(rotation=45)
    plt.grid()
    plt.legend()

    # Додавання підпису з датою та часом формування в лівому верхньому куті (київський час)
    kyiv_timezone = pytz.timezone("Europe/Kyiv")
    current_datetime = datetime.now(kyiv_timezone).strftime("%Y-%m-%d %H:%M")
    plt.figtext(0.01, 0.98, f"Згенеровано ботом FTP | Дата формування: {current_datetime} (Київ)", ha="left", fontsize=8, color="gray", va="top")

    # Збереження графіка як зображення
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()

    await update.message.reply_photo(photo=buffer)
    logging.info(f"Графік {parameter.lower()} для {employee_name} за {year} рік відображено.")

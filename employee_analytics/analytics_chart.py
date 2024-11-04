import matplotlib.pyplot as plt
from io import BytesIO
from telegram import Update
from telegram.ext import CallbackContext
from .analytics_table import get_income_data
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для побудови річного графіка за обраним параметром
async def show_yearly_chart_for_parameter(update: Update, context: CallbackContext, employee_name: str, year: str, parameter: str):
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
        "Кількість угод": "[Deal Count]"
    }.get(parameter)

    # Перевірка на випадок, якщо обраний параметр недоступний
    if not parameter_column:
        await update.message.reply_text("Обраний параметр не підтримується.")
        return

    # Отримання даних про обраний параметр за кожен місяць року
    for month in months:
        income_data = get_income_data(employee_name, "Менеджер", year, month) or get_income_data(employee_name, "Сейлс", year, month)
        value = income_data.get(parameter_column, 0) if income_data else 0
        monthly_values.append(value)

    # Побудова графіка
    plt.figure(figsize=(10, 5))
    plt.plot(months, monthly_values, marker='o', label=parameter)
    plt.title(f"Аналітика {parameter.lower()} {employee_name} за {year} рік")
    plt.xlabel("Місяці")
    plt.ylabel(f"{parameter} (USD)" if parameter != "Кількість угод" else "Кількість угод")
    plt.xticks(rotation=45)
    plt.grid()
    plt.legend()

    # Збереження графіка як зображення
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()

    await update.message.reply_photo(photo=buffer)
    logging.info(f"Графік {parameter.lower()} для {employee_name} за {year} рік відображено.")

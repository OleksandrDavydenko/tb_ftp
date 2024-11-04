import matplotlib.pyplot as plt
from io import BytesIO
from telegram import Update
from telegram.ext import CallbackContext
from .analytics_table import get_income_data
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для побудови річного графіка доходів
async def show_yearly_chart(update: Update, context: CallbackContext, employee_name: str, year: str):
    # Місяці для отримання даних та побудови графіка
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    monthly_incomes = []

    # Отримання даних про доходи за кожен місяць року
    for month in months:
        income_data = get_income_data(employee_name, "Менеджер", year, month) or get_income_data(employee_name, "Сейлс", year, month)
        total_income = income_data.get("[Sum USD]", 0) if income_data else 0
        monthly_incomes.append(total_income)

    # Побудова графіка
    plt.figure(figsize=(10, 5))
    plt.plot(months, monthly_incomes, marker='o', label='Доходи')
    plt.title(f"Аналітика доходів {employee_name} за {year} рік")
    plt.xlabel("Місяці")
    plt.ylabel("Доходи (USD)")
    plt.xticks(rotation=45)
    plt.grid()
    plt.legend()

    # Збереження графіка як зображення
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()

    await update.message.reply_photo(photo=buffer)
    logging.info(f"Графік доходів для {employee_name} за {year} рік відображено.")

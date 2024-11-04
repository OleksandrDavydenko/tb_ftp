import matplotlib.pyplot as plt
from io import BytesIO
from telegram import Update
from telegram.ext import CallbackContext
from .analytics_table import get_income_data
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для побудови річного графіка доходів, валового прибутку та маржинальності
async def show_yearly_chart(update: Update, context: CallbackContext, employee_name: str, year: str):
    # Місяці для отримання даних та побудови графіка
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    monthly_incomes = []
    monthly_gross_profits = []
    monthly_margins = []

    # Виведення повідомлення для користувача, що графік генерується
    await update.message.reply_text("Зачекайте, будь ласка, графік генерується...")

    # Отримання даних про доходи, валовий прибуток та розрахунок маржинальності за кожен місяць року
    for month in months:
        income_data = get_income_data(employee_name, "Менеджер", year, month) or get_income_data(employee_name, "Сейлс", year, month)
        
        total_income = income_data.get("[Sum USD]", 0) if income_data else 0
        gross_profit = income_data.get("[Gross Profit]", 0) if income_data else 0

        # Розрахунок маржинальності
        margin = (gross_profit / total_income * 100) if total_income != 0 else 0
        
        # Збереження даних у списки для побудови графіка
        monthly_incomes.append(total_income)
        monthly_gross_profits.append(gross_profit)
        monthly_margins.append(margin)

    # Побудова графіка
    plt.figure(figsize=(12, 6))

    # Лінії для кожного параметра
    plt.plot(months, monthly_incomes, marker='o', label='Доходи (USD)')
    plt.plot(months, monthly_gross_profits, marker='o', label='Валовий прибуток (USD)')
    plt.plot(months, monthly_margins, marker='o', label='Маржинальність (%)')

    # Оформлення графіка
    plt.title(f"Аналітика доходів, валового прибутку та маржинальності для {employee_name} за {year} рік")
    plt.xlabel("Місяці")
    plt.ylabel("Значення")
    plt.xticks(rotation=45)
    plt.grid()
    plt.legend()

    # Збереження графіка як зображення
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()

    # Відправка графіка користувачеві
    await update.message.reply_photo(photo=buffer)
    logging.info(f"Графік доходів, валового прибутку та маржинальності для {employee_name} за {year} рік відображено.")

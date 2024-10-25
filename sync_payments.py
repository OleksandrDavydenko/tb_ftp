import requests
import logging
from auth import get_power_bi_token
from db import add_payment, get_user_joined_at  # Імпортуємо функцію для отримання дати приєднання користувача

# Налаштовуємо logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sync_payments(employee_name, phone_number):
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    # Отримуємо дату приєднання користувача
    joined_at = get_user_joined_at(phone_number)
    if not joined_at:
        logging.error("Не вдалося отримати дату приєднання користувача.")
        return None

    year, month, day = joined_at.year, joined_at.month, joined_at.day
    logging.info(f"Дата приєднання користувача: {joined_at}, Рік: {year}, Місяць: {month}, День: {day}")

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Запит для отримання виплат після дати приєднання користувача
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            SalaryPayment,
                            SalaryPayment[Employee] = "{employee_name}" &&
                            DATEVALUE(SalaryPayment[DocDate]) >= DATE({year}, {month}, {day})
                        ),
                        "Дата платежу", SalaryPayment[DocDate],
                        "Документ", SalaryPayment[DocNumber],
                        "Сума UAH", SalaryPayment[SUM_UAH],
                        "Сума USD", SalaryPayment[SUM_USD]
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    logging.info(f"Виконуємо запит до Power BI з умовою дати приєднання: DATE({year}, {month}, {day})")
    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        logging.info("Запит до Power BI для синхронізації успішний.")
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])

        # Додаємо нові виплати в БД
        for payment in rows:
            сума_uah = float(payment.get("[Сума UAH]", 0))
            сума_usd = float(payment.get("[Сума USD]", 0))
            дата_платежу = payment.get("[Дата платежу]", "")
            номер_платежу = payment.get("[Документ]", "")

            if сума_usd > 0:  # Якщо сума в USD, тоді використовуємо її
                сума = сума_usd
                currency = "USD"
            else:  # В іншому випадку використовуємо UAH
                сума = сума_uah
                currency = "UAH"

            add_payment(phone_number, сума, currency, дата_платежу, номер_платежу)

        logging.info(f"Додано {len(rows)} нових платежів у базу даних.")
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")

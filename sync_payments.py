import requests
import logging
from auth import get_power_bi_token
from db import add_payment  # Імпортуємо функцію додавання платежу в БД

# Налаштовуємо logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sync_payments(employee_name, phone_number):
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Запит для отримання всіх доступних виплат по employee_name
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            SalaryPayment,
                            SalaryPayment[Employee] = "{employee_name}"
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

    logging.info("Виконуємо запит до Power BI для синхронізації платежів.")
    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        logging.info("Запит до Power BI для синхронізації успішний.")
        data = response.json()
        logging.info(f"Відповідь на запит: {data}")
        rows = data['results'][0]['tables'][0].get('rows', [])

        # Додаємо всі виплати в БД
        for payment in rows:
            сума = float(payment.get("[Сума UAH]", 0))
            дата_платежу = payment.get("[Дата платежу]", "")
            номер_платежу = payment.get("[Документ]", "")
            currency = "UAH" if сума > 0 else "USD"

            add_payment(phone_number, сума, currency, дата_платежу, номер_платежу)

        logging.info(f"Додано {len(rows)} платежів у базу даних.")
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")

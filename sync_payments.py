import requests
import psycopg2
import os
from datetime import datetime
from auth import get_power_bi_token
from db import add_payment  # Імпортуємо функцію додавання платежу в БД
import logging
import asyncio
import aiohttp

# Отримуємо URL бази даних з змінної середовища Heroku
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

async def sync_payments(employee_name, phone_number, joined_at):
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            SalaryPayment,
                            SalaryPayment[Employee] = "{employee_name}" &&
                            SalaryPayment[DocDate] >= "{joined_at.strftime('%Y-%m-%d')}"
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

    async with aiohttp.ClientSession() as session:
        async with session.post(power_bi_url, headers=headers, json=query_data) as response:
            if response.status == 200:
                data = await response.json()
                rows = data['results'][0]['tables'][0].get('rows', [])

                conn = get_db_connection()
                cursor = conn.cursor()

                for payment in rows:
                    сума_uah = float(payment.get("[Сума UAH]", 0))
                    сума_usd = float(payment.get("[Сума USD]", 0))
                    дата_платежу = payment.get("[Дата платежу]", "")
                    номер_платежу = payment.get("[Документ]", "")

                    # Визначаємо валюту та суму
                    if сума_usd > 0:
                        сума = сума_usd
                        currency = "USD"
                    else:
                        сума = сума_uah
                        currency = "UAH"

                    # Перевірка на дублікати перед додаванням
                    cursor.execute("""
                        SELECT 1 FROM payments
                        WHERE phone_number = %s AND amount = %s AND currency = %s AND payment_date = %s AND payment_number = %s
                    """, (phone_number, сума, currency, дата_платежу, номер_платежу))

                    if not cursor.fetchone():
                        add_payment(phone_number, сума, currency, дата_платежу, номер_платежу)

                conn.commit()
                cursor.close()
                conn.close()
                logging.info(f"Успішно синхронізовано {len(rows)} платежів для користувача {employee_name}.")
            else:
                error_text = await response.text()
                logging.error(f"Помилка при виконанні запиту: {response.status}, {error_text}")

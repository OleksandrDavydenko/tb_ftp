import asyncio
import requests
import psycopg2
import os
import logging
from datetime import datetime
from auth import get_power_bi_token
from db import add_payment  # Імпортуємо функцію додавання платежу в БД

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

async def async_add_payment(phone_number, сума, currency, дата_платежу, номер_платежу):
    """Асинхронне додавання платежу до БД."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Перевірка на дублікати перед додаванням
        cursor.execute("""
            SELECT 1 FROM payments
            WHERE phone_number = %s AND amount = %s AND currency = %s AND payment_date = %s AND payment_number = %s
        """, (phone_number, сума, currency, дата_платежу, номер_платежу))

        if not cursor.fetchone():
            add_payment(phone_number, сума, currency, дата_платежу, номер_платежу)
            logging.info(f"Додано новий платіж для користувача: {phone_number} на суму {сума} {currency} (№ {номер_платежу}).")
        
        conn.commit()

    except Exception as e:
        logging.error(f"Помилка при додаванні платежу: {e}")

    finally:
        cursor.close()
        conn.close()

async def sync_payments():
    """Асинхронна версія функції синхронізації для всіх користувачів."""
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

    conn = get_db_connection()
    cursor = conn.cursor()

    # Отримуємо список усіх користувачів
    cursor.execute("SELECT phone_number, first_name, joined_at FROM users")
    users = cursor.fetchall()

    for user in users:
        phone_number, employee_name, joined_at = user

        # Оновлений DAX-запит
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

        try:
            response = requests.post(power_bi_url, headers=headers, json=query_data)
            if response.status_code == 200:
                data = response.json()
                rows = data['results'][0]['tables'][0].get('rows', [])

                # Асинхронно додаємо кожен платіж до бази даних
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

                    await async_add_payment(phone_number, сума, currency, дата_платежу, номер_платежу)

                logging.info(f"Успішно синхронізовано {len(rows)} платежів для користувача {employee_name}.")
            else:
                logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")

        except Exception as e:
            logging.error(f"Помилка при синхронізації для {employee_name}: {e}")

    cursor.close()
    conn.close()

async def run_periodic_sync():
    """Асинхронна функція для періодичної синхронізації."""
    while True:
        try:
            await sync_payments()
        except Exception as e:
            logging.error(f"Помилка при періодичній синхронізації: {e}")
        await asyncio.sleep(30)  # Перевірка кожні 30 секунд

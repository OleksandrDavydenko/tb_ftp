import asyncio
import requests
import psycopg2
import os
import logging
from datetime import datetime
from auth import get_power_bi_token
from db import add_payment

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DATABASE_URL = os.getenv('DATABASE_URL')

TARGET_PHONE = "380632773227"

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def normalize_phone_number(phone_number):
    return phone_number[1:] if phone_number.startswith('+') else phone_number

def fetch_existing_records(cursor, phone_number, payment_number):
    cursor.execute("""
        SELECT amount, currency, payment_date::text, accrual_month
        FROM payments
        WHERE phone_number = %s AND payment_number = %s
    """, (phone_number, payment_number))
    return cursor.fetchall()

def records_differ(existing, new):
    def normalize(record):
        # Округлення до 2 знаків після коми, форматування дати
        amount = round(float(record[0]), 2)
        currency = str(record[1])
        payment_date = str(record[2])[:10]  # тільки YYYY-MM-DD
        accrual_month = str(record[3]).strip()
        return (amount, currency, payment_date, accrual_month)

    normalized_existing = sorted([normalize(r) for r in existing])
    normalized_new = sorted([normalize(r) for r in new])
    
    return normalized_existing != normalized_new


def delete_payment_records(phone_number, payment_number):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DELETE FROM payments
            WHERE phone_number = %s AND payment_number = %s
        """, (phone_number, payment_number))
        conn.commit()
        logging.info(f"🧹 Видалено старі записи по платіжці {payment_number} для {phone_number}")
    except Exception as e:
        logging.error(f"❌ Помилка при видаленні: {e}")
    finally:
        cursor.close()
        conn.close()

async def async_add_payment(phone_number, сума, currency, дата_платежу, номер_платежу, місяць_нарахування, is_notified=False):
    try:
        add_payment(phone_number, сума, currency, дата_платежу, номер_платежу, місяць_нарахування, is_notified)
        logging.info(f"✅ Додано платіж: {phone_number} | {сума} {currency} | {місяць_нарахування} | № {номер_платежу}")
    except Exception as e:
        logging.error(f"❌ Помилка при додаванні: {e}")

async def sync_payments():
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати токен Power BI.")
        return

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT phone_number, employee_name, joined_at FROM users WHERE status = 'active' AND phone_number = %s", (TARGET_PHONE,))
    users = cursor.fetchall()

    for phone_number, employee_name, joined_at in users:
        phone_number = normalize_phone_number(phone_number)
        query_data = {
            "queries": [{
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
                        "Сума USD", SalaryPayment[SUM_USD],
                        "МісяцьНарахування", SalaryPayment[МісяцьНарахування]
                    )
                """
            }],
            "serializerSettings": {"includeNulls": True}
        }

        try:
            response = requests.post(url, headers=headers, json=query_data)
            if response.status_code != 200:
                logging.error(f"❌ Power BI Error: {response.status_code}, {response.text}")
                continue

            rows = response.json()['results'][0]['tables'][0].get('rows', [])
            grouped = {}
            for row in rows:
                номер = row.get("[Документ]", "")
                grouped.setdefault(номер, []).append(row)

            for номер_платежу, items in grouped.items():
                new_data = []
                for row in items:
                    сума_uah = float(row.get("[Сума UAH]", 0))
                    сума_usd = float(row.get("[Сума USD]", 0))
                    дата = row.get("[Дата платежу]", "")
                    місяць = row.get("[МісяцьНарахування]", "").strip()

                    if abs(сума_usd) > 0:
                        сума, валюта = сума_usd, "USD"
                    elif abs(сума_uah) > 0:
                        сума, валюта = сума_uah, "UAH"
                    else:
                        continue

                    new_data.append((сума, валюта, дата, місяць))

                existing_data = fetch_existing_records(cursor, phone_number, номер_платежу)

                if records_differ(existing_data, new_data):
                    delete_payment_records(phone_number, номер_платежу)
                    for сума, валюта, дата, місяць in new_data:
                        await async_add_payment(phone_number, сума, валюта, дата, номер_платежу, місяць, is_notified=False)
                else:
                    logging.info(f"⚖️ Платіж {номер_платежу} для {phone_number} без змін")

        except Exception as e:
            logging.error(f"❌ Помилка синхронізації для {employee_name}: {e}")

    cursor.close()
    conn.close()

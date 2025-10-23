import asyncio
import requests
import psycopg2
import os
import logging
import pandas as pd
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

def fetch_db_payments(phone_number, payment_number):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT amount, currency, payment_date, accrual_month
        FROM payments
        WHERE phone_number = %s AND payment_number = %s
    """, (phone_number, payment_number))
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return set((f"{float(r[0]):.2f}", r[1], r[2].strftime('%Y-%m-%d'), r[3].strip()) for r in records)

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

async def async_add_payment(phone_number, amount, currency, payment_date, payment_number, accrual_month):
    try:
        add_payment(phone_number, amount, currency, payment_date, payment_number, accrual_month, False)
        logging.info(f"✅ Додано платіж: {phone_number} | {amount} {currency} | {accrual_month} | № {payment_number}")
    except Exception as e:
        logging.error(f"❌ Помилка при додаванні: {e}")

async def sync_payments():
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати токен Power BI.")
        return

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Формуємо запит для отримання всіх записів з SalaryPayment без фільтрів
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        SalaryPayment,
                        "Дата платежу", SalaryPayment[DocDate],
                        "Документ", SalaryPayment[DocNumber],
                        "Сума UAH", SalaryPayment[SUM_UAH],
                        "Сума USD", SalaryPayment[SUM_USD],
                        "МісяцьНарахування", SalaryPayment[МісяцьНарахування],
                        "Employee", SalaryPayment[Employee]
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
        if response.status_code != 200:
            logging.error(f"❌ Power BI error: {response.status_code} | {response.text}")
            return

        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        
        # Створюємо DataFrame
        df = pd.DataFrame(rows)
        
        # Логування DataFrame
        logging.info(f"📝 Отриманий DataFrame:\n{df}")

        for _, row in df.iterrows():
            employee_name = row["Employee"]
            payment_number = row["Документ"]
            amount_uah = float(row["Сума UAH"] or 0)
            amount_usd = float(row["Сума USD"] or 0)
            amount = amount_usd if abs(amount_usd) > 0 else amount_uah
            currency = "USD" if amount_usd > 0 else "UAH"
            payment_date = str(row["Дата платежу"]).split("T")[0]
            accrual_month = str(row["МісяцьНарахування"]).strip()

            # Порівнюємо з БД
            db_set = fetch_db_payments(TARGET_PHONE, payment_number)
            bi_set = {(f"{amount:.2f}", currency, payment_date, accrual_month)}

            if bi_set != db_set:
                delete_payment_records(TARGET_PHONE, payment_number)
                await async_add_payment(TARGET_PHONE, amount, currency, payment_date, payment_number, accrual_month)
            else:
                logging.info(f"⏭️ Платіж {payment_number} для {TARGET_PHONE} без змін")

        logging.info(f"🔄 Синхронізовано {len(rows)} рядків.")
    except Exception as e:
        logging.error(f"❌ Помилка при синхронізації: {e}")

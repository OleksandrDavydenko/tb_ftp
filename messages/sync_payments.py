import asyncio
import requests
import psycopg2
import os
import logging
from datetime import datetime
import pandas as pd
from auth import get_power_bi_token, normalize_phone_number
from db import add_payment

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

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

    conn = get_db_connection()
    cursor = conn.cursor()

    # Отримуємо всі дані співробітників (phone_number, employee_name, joined_at)
    cursor.execute("""
        SELECT phone_number, employee_name, joined_at 
        FROM users 
        WHERE status = 'active'
    """)
    users = cursor.fetchall()
    cursor.close()
    conn.close()

    # Формуємо список імен співробітників для запиту
    employee_names = [user[1] for user in users if user[1]]
    if not employee_names:
        logging.error("❌ Не знайдено активних співробітників.")
        return

    # Формуємо запит для отримання всіх платежів для всіх співробітників за один раз
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            SalaryPayment,
                            SalaryPayment[Employee] IN ({','.join(f'"{name}"' for name in employee_names)})
                        ),
                        "Employee", SalaryPayment[Employee],
                        "DocDate", SalaryPayment[DocDate],
                        "DocNumber", SalaryPayment[DocNumber],
                        "SUM_UAH", SalaryPayment[SUM_UAH],
                        "SUM_USD", SalaryPayment[SUM_USD],
                        "AccrualMonth", SalaryPayment[МісяцьНарахування]
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

        # Перетворюємо дані в DataFrame для подальшої обробки
        df = pd.DataFrame(rows)

        # Перевірка на порожні значення в "AccrualMonth" і заповнення їх, якщо потрібно
        df['AccrualMonth'] = df['AccrualMonth'].fillna('Не вказано')

        # Групуємо дані по співробітникам та платіжним номерам
        grouped = df.groupby(['Employee', 'DocNumber']).agg(
            amount=('SUM_USD', 'sum'),
            currency=('SUM_USD', lambda x: 'USD' if x.sum() > 0 else 'UAH'),
            payment_date=('DocDate', 'first'),
            accrual_month=('AccrualMonth', 'first')
        ).reset_index()

        # Тепер перевіряємо записи в базі даних і додаємо нові або оновлюємо існуючі
        for user in users:
            phone_number, employee_name, joined_at = user
            phone_number = normalize_phone_number(phone_number)

            # Фільтруємо дані для кожного співробітника та порівнюємо з базою
            employee_data = grouped[grouped['Employee'] == employee_name]
            for _, payment in employee_data.iterrows():
                payment_number = payment['DocNumber']
                bi_set = {(f"{payment['amount']:.2f}", payment['currency'], payment['payment_date'], payment['accrual_month'])}
                db_set = fetch_db_payments(phone_number, payment_number)

                if bi_set != db_set:
                    delete_payment_records(phone_number, payment_number)
                    for amount, currency, payment_date, accrual_month in bi_set:
                        await async_add_payment(phone_number, float(amount), currency, payment_date, payment_number, accrual_month)
                else:
                    logging.info(f"⏭️ Платіж {payment_number} для {phone_number} без змін")

            logging.info(f"🔄 Синхронізовано {len(employee_data)} рядків для {employee_name}")

    except Exception as e:
        logging.error(f"❌ Помилка при синхронізації платежів: {e}")

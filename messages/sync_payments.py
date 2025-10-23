import asyncio
import requests
import psycopg2
import os
import logging
import pandas as pd
from datetime import datetime
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

    # Отримуємо активних користувачів
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT phone_number, employee_name, joined_at 
        FROM users 
        WHERE status = 'active'
    """)
    users = cursor.fetchall()
    cursor.close()
    conn.close()

    # Одна функція для отримання всіх платежів для всіх користувачів
    query_data = {
        "queries": [
            {
                "query": """
                    EVALUATE 
                    SELECTCOLUMNS(
                        SalaryPayment,
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
        # Отримуємо дані з Power BI
        logging.info("🔄 Відправка запиту до Power BI...")
        response = requests.post(power_bi_url, headers=headers, json=query_data)
        
        # Логування отриманого результату
        if response.status_code == 200:
            data = response.json()
            logging.info(f"✅ Відповідь від Power BI: {data}")
        else:
            logging.error(f"❌ Power BI error: {response.status_code} | {response.text}")
            return

        # Перетворюємо дані у DataFrame
        rows = data['results'][0]['tables'][0].get('rows', [])
        df = pd.DataFrame(rows)
        logging.info(f"✅ Отримано {len(df)} записів з Power BI")

        # Логування даних з DataFrame
        logging.debug(f"Отримані дані: {df.head()}")

        # Обробляємо кожного користувача
        for user in users:
            phone_number, employee_name, joined_at = user
            phone_number = normalize_phone_number(phone_number)

            # Фільтруємо дані по конкретному користувачу
            user_data = df[df['Employee'] == employee_name]
            logging.info(f"🔄 Обробка даних для {employee_name} ({phone_number})")

            if user_data.empty:
                logging.info(f"❌ Для {employee_name} не знайдено даних.")
                continue

            # Групуємо платежі за номерами документів
            grouped = user_data.groupby('DocNumber')

            for payment_number, payments in grouped:
                bi_set = set()
                for _, p in payments.iterrows():
                    # Перевірка на наявність поля 'Employee'
                    if 'Employee' not in p or pd.isna(p['Employee']):
                        logging.error(f"❌ Поле 'Employee' відсутнє або пусте для платіжного запису: {p}")
                        continue  # Пропускаємо цей запис, якщо поле 'Employee' відсутнє або пусте

                    employee_name = p['Employee']
                    amount = float(p['SUM_USD']) if abs(p['SUM_USD']) > 0 else float(p['SUM_UAH'])
                    currency = "USD" if abs(p['SUM_USD']) > 0 else "UAH"
                    payment_date = p['DocDate'].split("T")[0]
                    accrual_month = p['AccrualMonth'].strip()
                    bi_set.add((f"{amount:.2f}", currency, payment_date, accrual_month))

                # Логування для кожного платежу
                logging.debug(f"Платіж {payment_number} для {employee_name}: {bi_set}")

                # Порівнюємо з даними з БД
                db_set = fetch_db_payments(phone_number, payment_number)

                if bi_set != db_set:
                    logging.info(f"🔄 Дані по платіжці {payment_number} для {phone_number} змінилися.")
                    delete_payment_records(phone_number, payment_number)
                    for amount, currency, payment_date, accrual_month in bi_set:
                        await async_add_payment(phone_number, float(amount), currency, payment_date, payment_number, accrual_month)
                else:
                    logging.info(f"⏭️ Платіж {payment_number} для {phone_number} без змін")

            logging.info(f"🔄 Синхронізовано {len(payments)} рядків для {employee_name}")

    except Exception as e:
        logging.error(f"❌ Помилка при обробці платежів: {e}")
    logging.info("✅ Синхронізація платежів завершена.")
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

    # Запитуємо всі дані з таблиці SalaryPayment
    query_data = {
        "queries": [
            {
                "query": """
                    EVALUATE 
                    SELECTCOLUMNS(
                        SalaryPayment,
                        "Employee", SalaryPayment[Employee],
                        "Дата платежу", SalaryPayment[DocDate],
                        "Документ", SalaryPayment[DocNumber],
                        "Сума UAH", SalaryPayment[SUM_UAH],
                        "Сума USD", SalaryPayment[SUM_USD],
                        "МісяцьНарахування", SalaryPayment[МісяцьНарахування]
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

        # Перевірка наявності таблиць у відповіді
        if 'results' not in data or len(data['results']) == 0 or 'tables' not in data['results'][0]:
            logging.error("❌ Немає даних у відповіді від Power BI.")
            return

        rows = data['results'][0]['tables'][0].get('rows', [])

        # Логування отриманих даних для діагностики
        logging.info(f"✅ Отримані дані з Power BI: {rows}")

        if len(rows) == 0:
            logging.info("❌ Немає записів у даних.")
            return

        # Перетворюємо список в датафрейм
        df = pd.DataFrame(rows)

        # Перейменовуємо колонки для зручності
        df.columns = df.columns.str.replace(r'[\[\]]', '', regex=True)  # Видаляємо квадратні дужки з назв колонок
        logging.info(f"✅ Оновлені колонки: {df.columns}")

        # Перевірка наявності колонки 'Employee'
        if 'Employee' not in df.columns:
            logging.error("❌ Відсутня колонка 'Employee' в отриманих даних.")
            return

        # Фільтруємо порожні записи в колонці Employee
        df = df[df['Employee'].notna()]

        # Виводимо датафрейм для перевірки
        logging.info(f"✅ Датафрейм даних: {df}")

        # Синхронізуємо дані для кожного співробітника
        for _, row in df.iterrows():
            employee_name = row['Employee']
            payment_number = row['Документ']
            amount = float(row['Сума USD']) if abs(row['Сума USD']) > 0 else float(row['Сума UAH'])
            currency = "USD" if abs(row['Сума USD']) > 0 else "UAH"
            payment_date = str(row['Дата платежу']).split("T")[0]
            accrual_month = row['МісяцьНарахування'].strip()

            # Отримуємо запис із БД
            db_set = fetch_db_payments(normalize_phone_number(employee_name), payment_number)

            bi_set = {(f"{amount:.2f}", currency, payment_date, accrual_month)}

            if bi_set != db_set:
                delete_payment_records(normalize_phone_number(employee_name), payment_number)
                for amount, currency, payment_date, accrual_month in bi_set:
                    await async_add_payment(normalize_phone_number(employee_name), float(amount), currency, payment_date, payment_number, accrual_month)
            else:
                logging.info(f"⏭️ Платіж {payment_number} для {employee_name} без змін")

    except Exception as e:
        logging.error(f"❌ Помилка при обробці: {e}")

import asyncio
import psycopg2
import os
import logging
import pandas as pd
from datetime import datetime
from auth import normalize_phone_number
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

    for user in users:
        phone_number, employee_name, joined_at = user
        phone_number = normalize_phone_number(phone_number)

        # Запит до бази даних для отримання всіх платежів для співробітника після дати приєднання
        conn = get_db_connection()
        query = f"""
            SELECT DocDate, DocNumber, SUM_UAH, SUM_USD, МісяцьНарахування
            FROM SalaryPayment
            WHERE Employee = '{employee_name}' AND DocDate >= '{joined_at.strftime('%Y-%m-%d')}'
        """
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if rows:
            # Створюємо датафрейм з отриманих даних
            df = pd.DataFrame(rows, columns=["Дата платежу", "Документ", "Сума UAH", "Сума USD", "МісяцьНарахування"])

            # Фільтруємо порожні записи в колонці Employee
            df = df.dropna(subset=["Документ"])

            # Логування датафрейму
            logging.info(f"✅ Датафрейм даних для {employee_name}: {df}")

            # Перевірка та синхронізація
            for _, row in df.iterrows():
                payment_number = row["Документ"]
                amount = float(row["Сума USD"]) if abs(row["Сума USD"]) > 0 else float(row["Сума UAH"])
                currency = "USD" if abs(row["Сума USD"]) > 0 else "UAH"
                payment_date = row["Дата платежу"].strftime('%Y-%m-%d')
                accrual_month = row["МісяцьНарахування"]

                # Отримуємо запис з бази даних
                db_set = fetch_db_payments(phone_number, payment_number)
                bi_set = {(f"{amount:.2f}", currency, payment_date, accrual_month)}

                if bi_set != db_set:
                    delete_payment_records(phone_number, payment_number)
                    for amount, currency, payment_date, accrual_month in bi_set:
                        await async_add_payment(phone_number, float(amount), currency, payment_date, payment_number, accrual_month)
                else:
                    logging.info(f"⏭️ Платіж {payment_number} для {phone_number} без змін")

        else:
            logging.info(f"❌ Не знайдено платежів для {employee_name} після {joined_at.strftime('%Y-%m-%d')}")


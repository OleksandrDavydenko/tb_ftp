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
#TARGET_PHONE = "380632773227"

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
                continue

            data = response.json()
            rows = data['results'][0]['tables'][0].get('rows', [])
            grouped = {}
            for payment in rows:
                doc = payment.get("[Документ]", "")
                grouped.setdefault(doc, []).append(payment)

            for payment_number, payments in grouped.items():
                bi_set = set()
                for p in payments:
                    amount = float(p.get("[Сума USD]", 0)) if abs(p.get("[Сума USD]", 0)) > 0 else float(p.get("[Сума UAH]", 0))
                    currency = "USD" if abs(p.get("[Сума USD]", 0)) > 0 else "UAH"
                    payment_date = str(p.get("[Дата платежу]", "")).split("T")[0]
                    accrual_month = p.get("[МісяцьНарахування]", "").strip()
                    bi_set.add((f"{amount:.2f}", currency, payment_date, accrual_month))

                db_set = fetch_db_payments(phone_number, payment_number)

                if bi_set != db_set:
                    delete_payment_records(phone_number, payment_number)
                    for amount, currency, payment_date, accrual_month in bi_set:
                        await async_add_payment(phone_number, float(amount), currency, payment_date, payment_number, accrual_month)
                else:
                    logging.info(f"⏭️ Платіж {payment_number} для {phone_number} без змін")

            logging.info(f"🔄 Синхронізовано {len(rows)} рядків для {employee_name}")
        except Exception as e:
            logging.error(f"❌ Помилка при обробці {employee_name}: {e}")

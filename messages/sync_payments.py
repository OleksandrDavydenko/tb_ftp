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

def delete_payment_records(phone_number, payment_number):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DELETE FROM payments
            WHERE phone_number = %s AND payment_number = %s
        """, (phone_number, payment_number))
        conn.commit()
        logging.info(f"🧹 Видалено записи по платіжці {payment_number} для {phone_number}")
    except Exception as e:
        logging.error(f"❌ Помилка при видаленні записів: {e}")
    finally:
        cursor.close()
        conn.close()

async def async_add_payment(phone_number, сума, currency, дата_платежу, номер_платежу, місяць_нарахування, already_notified):
    try:
        add_payment(phone_number, сума, currency, дата_платежу, номер_платежу, місяць_нарахування, already_notified)
        logging.info(f"✅ Додано платіж: {phone_number} | {сума} {currency} | {місяць_нарахування} | № {номер_платежу}")
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
        WHERE status = 'active' AND phone_number = %s
    """, (TARGET_PHONE,))
    users = cursor.fetchall()

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
            if response.status_code == 200:
                data = response.json()
                rows = data['results'][0]['tables'][0].get('rows', [])

                grouped = {}
                for payment in rows:
                    номер_платежу = payment.get("[Документ]", "")
                    grouped.setdefault(номер_платежу, []).append(payment)

                for номер_платежу, payments in grouped.items():
                    for payment in payments:
                        сума_uah = float(payment.get("[Сума UAH]", 0))
                        сума_usd = float(payment.get("[Сума USD]", 0))
                        дата_платежу = payment.get("[Дата платежу]", "")
                        місяць_нарахування = payment.get("[МісяцьНарахування]", "").strip()

                        if abs(сума_usd) > 0:
                            сума = сума_usd
                            currency = "USD"
                        elif abs(сума_uah) > 0:
                            сума = сума_uah
                            currency = "UAH"
                        else:
                            continue

                        # Перевіряємо, чи існує запис
                        cursor.execute("""
                            SELECT COUNT(*) FROM payments
                            WHERE phone_number = %s AND payment_number = %s
                        """, (phone_number, номер_платежу))
                        record_exists = cursor.fetchone()[0] > 0

                        if record_exists:
                            # Оновлюємо існуючий запис
                            cursor.execute("""
                                UPDATE payments
                                SET amount = %s, currency = %s, payment_date = %s, accrual_month = %s, is_notified = %s
                                WHERE phone_number = %s AND payment_number = %s
                            """, (сума, currency, дата_платежу, місяць_нарахування, False, phone_number, номер_платежу))
                            logging.info(f"🔄 Оновлено платіж: {phone_number} | {сума} {currency} | {місяць_нарахування} | № {номер_платежу}")
                        else:
                            # Додаємо новий запис
                            await async_add_payment(phone_number, сума, currency, дата_платежу, номер_платежу, місяць_нарахування, False)

                logging.info(f"🔄 Синхронізовано {len(rows)} рядків для {employee_name}")
            else:
                logging.error(f"❌ Power BI error: {response.status_code} | {response.text}")
        except Exception as e:
            logging.error(f"❌ Помилка при обробці користувача {employee_name}: {e}")

    cursor.close()
    conn.close()

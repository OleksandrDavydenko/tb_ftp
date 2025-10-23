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


def fetch_all_db_payments():
    """
    Отримуємо всі платежі для всіх користувачів з бази даних.
    Платежі зв'язуються з користувачами через телефон і ім'я співробітника.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT phone_number, employee_name, payment_number, amount, currency, payment_date, accrual_month
        FROM payments
        JOIN users ON payments.phone_number = users.phone_number
        WHERE users.status = 'active'
    """)
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Логування отриманих даних з БД
    logging.info(f"✅ Отримано {len(records)} записів з БД.")
    
    return pd.DataFrame(records, columns=['phone_number', 'employee_name', 'payment_number', 'amount', 'currency', 'payment_date', 'accrual_month'])


async def async_add_payment(phone_number, amount, currency, payment_date, payment_number, accrual_month):
    """
    Додає платіж в базу даних.
    """
    try:
        add_payment(phone_number, amount, currency, payment_date, payment_number, accrual_month, False)
        logging.info(f"✅ Додано платіж: {phone_number} | {amount} {currency} | {accrual_month} | № {payment_number}")
    except Exception as e:
        logging.error(f"❌ Помилка при додаванні: {e}")


def delete_payment_records(phone_number, payment_number):
    """
    Видаляє старі платіжні записи з бази даних за номером телефону і платіжним номером.
    """
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


async def sync_payments():
    """
    Основна функція для синхронізації платежів з Power BI і базою даних.
    Відправляє один запит до Power BI, отримує дані, порівнює їх з базою даних,
    та синхронізує зміни.
    """
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

    # Логування перед запитом до Power BI
    logging.info("🔄 Відправка запиту до Power BI...")

    query_data = {
        "queries": [
            {
                "query": """
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            SalaryPayment,
                            NOT(ISBLANK(SalaryPayment[Employee])) && SalaryPayment[Employee] <> ""
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
        # Отримуємо дані з Power BI
        response = requests.post(power_bi_url, headers=headers, json=query_data)

        if response.status_code != 200:
            logging.error(f"❌ Power BI error: {response.status_code} | {response.text}")
            return

        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        df_power_bi = pd.DataFrame(rows)
        logging.info(f"✅ Отримано {len(df_power_bi)} записів з Power BI")

        # Логування отриманих даних з Power BI
        logging.debug(f"Отримані дані з Power BI: {df_power_bi.head()}")

        # Фільтрація даних на основі наявності 'Employee'
        df_power_bi = df_power_bi[df_power_bi['Employee'].notna() & (df_power_bi['Employee'] != '')]
        logging.info(f"✅ Після фільтрації залишилося {len(df_power_bi)} записів для обробки.")

        # Отримуємо всі платежі з БД для порівняння
        df_db = fetch_all_db_payments()

        # Логування отриманих даних з БД
        logging.debug(f"Отримані дані з БД: {df_db.head()}")

        # Для кожного користувача з Power BI, порівнюємо платежі
        for _, user_payment in df_power_bi.iterrows():
            employee_name = user_payment['Employee']
            payment_number = user_payment['DocNumber']
            amount = float(user_payment['SUM_USD']) if abs(user_payment['SUM_USD']) > 0 else float(user_payment['SUM_UAH'])
            currency = "USD" if abs(user_payment['SUM_USD']) > 0 else "UAH"
            payment_date = user_payment['DocDate'].split("T")[0]
            accrual_month = user_payment['AccrualMonth'].strip()

            # Логування для кожного платіжного запису
            logging.debug(f"Обробка платіжного запису для {employee_name}, платіж №{payment_number}.")

            # Фільтруємо платежі по співробітнику
            db_payment = df_db[df_db['employee_name'] == employee_name]

            # Логування для кожного співробітника
            logging.debug(f"Знайдено {len(db_payment)} записів в БД для {employee_name}.")

            # Якщо в базі є запис для цього користувача, порівнюємо
            if not db_payment.empty:
                db_payment_set = set(db_payment[['payment_number', 'amount', 'currency', 'payment_date', 'accrual_month']].apply(tuple, axis=1))
                bi_payment_set = {(payment_number, f"{amount:.2f}", currency, payment_date, accrual_month)}

                # Логування для порівняння
                logging.debug(f"Порівняння даних: {db_payment_set} з {bi_payment_set}")

                # Порівнюємо
                if db_payment_set != bi_payment_set:
                    logging.info(f"🔄 Зміни в платіжці {payment_number} для {employee_name}. Синхронізуємо...")
                    delete_payment_records(user_payment['phone_number'], payment_number)  # Видаляємо старі записи перед додаванням нових
                    await async_add_payment(user_payment['phone_number'], amount, currency, payment_date, payment_number, accrual_month)
                else:
                    logging.info(f"⏭️ Платіж {payment_number} для {employee_name} без змін.")
            else:
                logging.info(f"❌ Платіж {payment_number} для {employee_name} не знайдено в БД.")

    except Exception as e:
        logging.error(f"❌ Помилка при обробці платежів: {e}")
    logging.info("✅ Синхронізація платежів завершена.")

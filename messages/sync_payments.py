import asyncio
import requests
import psycopg2
import os
import logging
import pandas as pd
from datetime import datetime
from auth import get_power_bi_token, normalize_phone_number
from db import add_payment
from utils.blocking import run_blocking

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
    token = await run_blocking(get_power_bi_token)
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
        if len(rows) == 0:
            logging.info("❌ Немає записів у даних.")
            return

        # Перетворюємо список в датафрейм
        df = pd.DataFrame(rows)

        # Перейменовуємо колонки для зручності
        df.columns = df.columns.str.replace(r'[\[\]]', '', regex=True)
        logging.info(f"✅ Оновлені колонки: {df.columns}")

        # Перевірка наявності колонки 'Employee'
        if 'Employee' not in df.columns:
            logging.error("❌ Відсутня колонка 'Employee' в отриманих даних.")
            return

        # Фільтруємо порожні записи в колонці Employee
        df = df[df['Employee'].notna() & (df['Employee'] != '')]

        # Приводимо дату платежу до формату datetime і нормалізуємо (видаляємо час)
        df['Дата платежу'] = pd.to_datetime(df['Дата платежу'], errors='coerce').dt.normalize()

        # Логування отриманих даних
        logging.info(f"✅ Отримано {len(df)} записів з Power BI")
        logging.info(f"📊 Унікальні співробітники в даних Power BI: {df['Employee'].unique()[:10]}")  # Перші 10
        
        # Перевіримо, чи є платежі за сьогодні
        today = pd.Timestamp.now().normalize()
        today_payments = df[df['Дата платежу'] == today]
        logging.info(f"📅 Платежі за сьогодні ({today}): {len(today_payments)} записів")
        if len(today_payments) > 0:
            logging.info(f"📋 Співробітники з платежами за сьогодні: {today_payments['Employee'].unique()}")

        # Отримуємо дату приєднання для кожного співробітника з таблиці users
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""SELECT employee_name, phone_number, joined_at FROM users WHERE status = 'active'""")
        users = cursor.fetchall()
        cursor.close()
        conn.close()

        # Створюємо словник для швидкого пошуку співробітників за іменем
        users_dict = {}
        for user in users:
            employee_name, phone_number, joined_at = user
            normalized_phone = normalize_phone_number(phone_number)
            # Нормалізуємо дату приєднання (видаляємо час)
            joined_at_normalized = pd.to_datetime(joined_at).normalize()
            users_dict[employee_name] = {
                'phone_number': normalized_phone,
                'joined_at': joined_at_normalized
            }

        logging.info(f"✅ Отримано {len(users_dict)} активних користувачів з БД")
        logging.info(f"📋 Співробітники з БД: {list(users_dict.keys())[:10]}")  # Перші 10

        # Синхронізуємо дані для кожного співробітника
        synced_count = 0
        
        for employee_name, user_info in users_dict.items():
            phone_number = user_info['phone_number']
            joined_at = user_info['joined_at']

            if not phone_number:
                logging.warning(f"❌ Номер телефону для {employee_name} не нормалізований.")
                continue

            logging.info(f"🔍 Обробляємо співробітника: {employee_name} (тел: {phone_number}, приєднався: {joined_at})")

            # Фільтруємо платежі по ІМЕНІ співробітника та даті (порівнюємо тільки дати)
            employee_payments = df[
                (df['Employee'] == employee_name) & 
                (df['Дата платежу'] >= joined_at)
            ]

            logging.info(f"📋 Знайдено {len(employee_payments)} платежів для {employee_name} після {joined_at}")

            if employee_payments.empty:
                logging.info(f"⏭️ Немає платежів для {employee_name} після {joined_at}")
                # Додатково перевіримо, чи взагалі є цей співробітник в даних Power BI
                all_employee_payments = df[df['Employee'] == employee_name]
                if len(all_employee_payments) > 0:
                    logging.info(f"ℹ️ Увага! {employee_name} є в Power BI, але всі платежі до {joined_at}")
                continue

            # Групуємо платежі по номерам документів
            grouped = employee_payments.groupby('Документ')
            
            for payment_number, group in grouped:
                bi_set = set()
                for _, row in group.iterrows():
                    amount = float(row["Сума USD"]) if abs(row["Сума USD"]) > 0 else float(row["Сума UAH"])
                    currency = "USD" if abs(row["Сума USD"]) > 0 else "UAH"
                    payment_date = row["Дата платежу"].strftime('%Y-%m-%d') if pd.notna(row["Дата платежу"]) else ""
                    accrual_month = str(row["МісяцьНарахування"]).strip() if pd.notna(row["МісяцьНарахування"]) else ""
                    
                    bi_set.add((f"{amount:.2f}", currency, payment_date, accrual_month))

                # Порівнюємо з даними з БД
                db_set = fetch_db_payments(phone_number, payment_number)
                
                logging.info(f"🔍 Порівняння для {employee_name}, платіж {payment_number}:")
                logging.info(f"   BI set: {bi_set}")
                logging.info(f"   DB set: {db_set}")
                
                if bi_set != db_set:
                    logging.info(f"🔄 Знайдені розбіжності для {employee_name}, платіж {payment_number}")
                    delete_payment_records(phone_number, payment_number)
                    for amount, currency, payment_date, accrual_month in bi_set:
                        await async_add_payment(phone_number, float(amount), currency, payment_date, payment_number, accrual_month)
                    synced_count += 1
                else:
                    logging.info(f"⏭️ Платіж {payment_number} для {employee_name} без змін")

        logging.info(f"✅ Синхронізацію завершено. Оновлено {synced_count} платежів")

    except Exception as e:
        logging.error(f"❌ Помилка при обробці: {e}")
        import traceback
        logging.error(f"❌ Деталі помилки: {traceback.format_exc()}")
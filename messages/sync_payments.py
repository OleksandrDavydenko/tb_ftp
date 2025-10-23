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
    """Отримує платежі з БД за нормалізованим номером телефону та номером платежу"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Отримуємо всі записи з БД за номером платежу
    cursor.execute("""
        SELECT amount, currency, payment_date, accrual_month, phone_number
        FROM payments 
        WHERE payment_number = %s
    """, (payment_number,))
    
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Фільтруємо за нормалізованим номером телефону
    filtered_records = []
    for r in records:
        db_phone_normalized = normalize_phone_number(r[4])  # phone_number є 5-м полем
        if db_phone_normalized == phone_number:
            filtered_records.append(r)
    
    logging.info(f"🔍 Знайдено {len(filtered_records)} записів у БД для {phone_number}, платіж {payment_number}")
    
    return set((f"{float(r[0]):.2f}", r[1], r[2].strftime('%Y-%m-%d'), r[3].strip()) for r in filtered_records)

def delete_payment_records(phone_number, payment_number):
    """Видаляє платежі з БД за нормалізованим номером телефону та номером платежу"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Спочатку знайдемо всі записи, які потрібно видалити
        cursor.execute("""
            SELECT id, phone_number 
            FROM payments 
            WHERE payment_number = %s
        """, (payment_number,))
        
        records_to_delete = cursor.fetchall()
        
        # Відфільтруємо за нормалізованим номером телефону
        ids_to_delete = []
        for record_id, db_phone in records_to_delete:
            db_phone_normalized = normalize_phone_number(db_phone)
            if db_phone_normalized == phone_number:
                ids_to_delete.append(record_id)
        
        if ids_to_delete:
            # Видалимо знайдені записи
            placeholders = ','.join(['%s'] * len(ids_to_delete))
            cursor.execute(f"""
                DELETE FROM payments 
                WHERE id IN ({placeholders})
            """, ids_to_delete)
            
            conn.commit()
            logging.info(f"🧹 Видалено {len(ids_to_delete)} старих записів по платіжці {payment_number} для {phone_number}")
        else:
            logging.info(f"ℹ️ Не знайдено записів для видалення: {phone_number}, платіж {payment_number}")
            
    except Exception as e:
        logging.error(f"❌ Помилка при видаленні: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

async def async_add_payment(phone_number, amount, currency, payment_date, payment_number, accrual_month):
    """Додає платіж з нормалізованим номером телефону"""
    try:
        add_payment(phone_number, amount, currency, payment_date, payment_number, accrual_month, False)
        logging.info(f"✅ Додано платіж: {phone_number} | {amount} {currency} | {accrual_month} | № {payment_number}")
    except Exception as e:
        logging.error(f"❌ Помилка при додаванні: {e}")

def check_payment_state(phone_number, payment_number):
    """Функція для дебагу - перевіряє стан платежів у БД"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, phone_number, amount, currency, payment_date, payment_number, accrual_month
        FROM payments 
        WHERE payment_number = %s
    """, (payment_number,))
    
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    
    logging.info(f"🔍 СТАН БД для платежу {payment_number}:")
    for record in records:
        record_id, db_phone, amount, currency, payment_date, payment_num, accrual_month = record
        db_phone_normalized = normalize_phone_number(db_phone)
        matches = "✅" if db_phone_normalized == phone_number else "❌"
        logging.info(f"   {matches} ID: {record_id}, Телефон: {db_phone} (норм: {db_phone_normalized}), Сума: {amount} {currency}")
    
    return records

def delete_all_payments(confirm=False):
    """
    Видаляє всі записи з таблиці payments
    
    Args:
        confirm (bool): Потрібно підтвердження для видалення
    
    Returns:
        int: Кількість видалених записів
    """
    if not confirm:
        logging.warning("⚠️  Для видалення всіх записів встановіть confirm=True")
        return 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Спочатку порахуємо кількість записів
        cursor.execute("SELECT COUNT(*) FROM payments")
        count_before = cursor.fetchone()[0]
        
        if count_before == 0:
            logging.info("📭 Таблиця payments вже порожня")
            return 0
        
        # Виконуємо видалення
        cursor.execute("DELETE FROM payments")
        conn.commit()
        
        logging.info(f"🗑️ Видалено всі записи з таблиці payments. Кількість: {count_before}")
        return count_before
        
    except Exception as e:
        logging.error(f"❌ Помилка при видаленні всіх записів: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

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
        logging.info(f"📊 Унікальні співробітники в даних Power BI: {df['Employee'].unique()[:10]}")
        
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
        logging.info(f"📋 Співробітники з БД: {list(users_dict.keys())[:10]}")

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
                    logging.info(f"📅 Дати платежів: {all_employee_payments['Дата платежу'].unique()}")
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

                # Додамо перевірку стану БД перед видаленням
                check_payment_state(phone_number, payment_number)
                
                # Порівнюємо з даними з БД
                db_set = fetch_db_payments(phone_number, payment_number)
                
                logging.info(f"🔍 Порівняння для {employee_name}, платіж {payment_number}:")
                logging.info(f"   BI set: {bi_set}")
                logging.info(f"   DB set: {db_set}")
                
                if bi_set != db_set:
                    logging.info(f"🔄 Знайдені розбіжності для {employee_name}, платіж {payment_number}")
                    delete_payment_records(phone_number, payment_number)
                    
                    # Після видалення ще раз перевіримо стан
                    check_payment_state(phone_number, payment_number)
                    
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


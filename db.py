import psycopg2
import os
from datetime import datetime
import logging

# Отримуємо URL бази даних з змінної середовища Heroku
DATABASE_URL = os.getenv('DATABASE_URL')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    # Підключаємось до бази даних PostgreSQL через URL з Heroku
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Створюємо таблицю користувачів
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        phone_number VARCHAR(20) UNIQUE NOT NULL,
        telegram_id BIGINT NOT NULL,
        telegram_name VARCHAR(50),
        employee_name VARCHAR(50),
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

     # Створюємо таблицю виплат
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id SERIAL PRIMARY KEY,
        phone_number VARCHAR(20) NOT NULL,
        amount NUMERIC(10, 2),
        currency VARCHAR(10),
        payment_date TIMESTAMP,
        payment_number VARCHAR(50),
        is_notified BOOLEAN DEFAULT FALSE
    )
    """)

    # Створюємо таблицю для аналізу девальвації з колонкою is_notified
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DevaluationAnalysis (
        id SERIAL PRIMARY KEY,
        client VARCHAR(255),
        payment_number VARCHAR(50),
        acc_number VARCHAR(50),
        contract_number VARCHAR(50),
        date_from_acc TIMESTAMP,
        date_from_payment TIMESTAMP,
        date_difference_in_days INTEGER,
        currency_from_inform_acc VARCHAR(10),
        exchange_rate_acc_nbu NUMERIC(10, 4),
        exchange_rate_payment_nbu NUMERIC(10, 4),
        devaluation_percentage NUMERIC(5, 2),
        payment_sum NUMERIC(12, 2),
        compensation NUMERIC(12, 2),
        manager VARCHAR(255),
        is_notified BOOLEAN DEFAULT FALSE
    )
    """)

    # Створюємо таблицю курсів валют
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ExchangeRates (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        currency VARCHAR(10) NOT NULL,
        rate NUMERIC(10, 4) NOT NULL
    )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Усі таблиці створено або оновлено успішно.")

# Викликаємо функцію для створення таблиць при запуску
create_tables()

def add_telegram_user(phone_number, telegram_id, telegram_name, employee_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO users (phone_number, telegram_id, telegram_name, employee_name, joined_at)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (phone_number) DO UPDATE SET
        telegram_id = EXCLUDED.telegram_id,
        telegram_name = EXCLUDED.telegram_name,
        employee_name = EXCLUDED.employee_name,
        joined_at = COALESCE(users.joined_at, EXCLUDED.joined_at)
    """, (phone_number, telegram_id, telegram_name, employee_name, datetime.now()))

    conn.commit()
    cursor.close()
    conn.close()

def add_payment(phone_number, amount, currency, payment_date, payment_number):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO payments (phone_number, amount, currency, payment_date, payment_number)
    VALUES (%s, %s, %s, %s, %s)
    """, (phone_number, amount, currency, payment_date, payment_number))

    conn.commit()
    cursor.close()
    conn.close()

def add_devaluation_record(data):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO DevaluationAnalysis (
        client, payment_number, acc_number, contract_number, date_from_acc,
        date_from_payment, date_difference_in_days, currency_from_inform_acc,
        exchange_rate_acc_nbu, exchange_rate_payment_nbu, devaluation_percentage,
        payment_sum, compensation, manager
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (payment_number) DO NOTHING  -- Перевірка унікальності payment_number
    """, (
        data["Client"], data["PaymentNumber"], data["AccNumber"], data["ContractNumber"],
        data["DateFromAcc"], data["DateFromPayment"], data["DateDifferenceInDays"],
        data["CurrencyFromInformAcc"], data["ExchangeRateAccNBU"], data["ExchangeRatePaymentNBU"],
        data["Devalvation%"], data["PaymentSum"], data["Compensation"], data["Manager"]
    ))

    conn.commit()
    cursor.close()
    conn.close()

def add_exchange_rate(currency, rate):
    """
    Функція для додавання запису про курс валют у таблицю ExchangeRates.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO ExchangeRates (currency, rate)
    VALUES (%s, %s)
    """, (currency, rate))

    conn.commit()
    cursor.close()
    conn.close()

def get_user_joined_at(phone_number):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT joined_at FROM users WHERE phone_number = %s
    """, (phone_number,))
    result = cursor.fetchone()

    conn.close()

    if result:
        return result[0]
    return None

""" def get_all_users():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT telegram_id, telegram_name FROM users")
    users = cursor.fetchall()

    conn.close()

    return [{'telegram_id': user[0], 'telegram_name': user[1]} for user in users] """




def get_all_users():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT telegram_id, telegram_name, employee_name FROM users")
    users = cursor.fetchall()

    conn.close()

    return [{'telegram_id': user[0], 'telegram_name': user[1], 'employee_name': user[2]} for user in users]




def get_latest_currency_rates(currencies):
    """
    Отримує останній курс для кожної валюти із бази даних.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT DISTINCT ON (currency) currency, rate
            FROM exchangerates
            WHERE currency = ANY(%s)
            ORDER BY currency, timestamp DESC;
        """
        cursor.execute(query, (currencies,))
        rows = cursor.fetchall()
        conn.close()
        return [{"currency": row[0], "rate": row[1]} for row in rows]
    except Exception as e:
        print(f"Помилка отримання курсів: {e}")
        raise e
    




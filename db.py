import psycopg2
import os

# Отримуємо URL бази даних з змінної середовища Heroku
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    # Підключаємось до бази даних PostgreSQL через URL з Heroku
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Створюємо таблицю користувачів, якщо її ще не існує
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        phone_number VARCHAR(20) UNIQUE NOT NULL,
        telegram_id BIGINT NOT NULL,
        first_name VARCHAR(50),
        last_name VARCHAR(50)
    )
    """)

    # Створюємо таблицю виплат, якщо її ще не існує
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id SERIAL PRIMARY KEY,
        phone_number VARCHAR(20) NOT NULL,
        amount NUMERIC(10, 2),
        currency VARCHAR(10),
        payment_date TIMESTAMP,
        payment_number VARCHAR(50),  -- Номер виплати
        is_notified BOOLEAN DEFAULT FALSE
    )
    """)

    conn.commit()
    cursor.close()
    conn.close()

def add_telegram_user(phone_number, telegram_id, first_name, last_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Додаємо користувача в таблицю
    cursor.execute("""
    INSERT INTO users (phone_number, telegram_id, first_name, last_name)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (phone_number) DO NOTHING
    """, (phone_number, telegram_id, first_name, last_name))

    conn.commit()
    cursor.close()
    conn.close()

def add_payment(phone_number, amount, currency, payment_date, payment_number):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Додаємо запис про виплату
    cursor.execute("""
    INSERT INTO payments (phone_number, amount, currency, payment_date, payment_number)
    VALUES (%s, %s, %s, %s, %s)
    """, (phone_number, amount, currency, payment_date, payment_number))

    conn.commit()
    cursor.close()
    conn.close()

# Викликаємо функцію для створення таблиць при запуску
create_tables()

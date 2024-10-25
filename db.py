import psycopg2
import os
from datetime import datetime

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
        last_name VARCHAR(50),
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        payment_number VARCHAR(50),
        is_notified BOOLEAN DEFAULT FALSE
    )
    """)

    conn.commit()
    cursor.close()
    conn.close()

def add_telegram_user(phone_number, telegram_id, first_name, last_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Оновлюємо дату приєднання для нових користувачів
    cursor.execute("""
    INSERT INTO users (phone_number, telegram_id, first_name, last_name, joined_at)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (phone_number) DO UPDATE SET
        telegram_id = EXCLUDED.telegram_id,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        joined_at = COALESCE(users.joined_at, EXCLUDED.joined_at)
    """, (phone_number, telegram_id, first_name, last_name, datetime.now()))

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

def update_existing_users_joined_at():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Оновлюємо поле joined_at для всіх записів, де воно є NULL
    cursor.execute("""
    UPDATE users
    SET joined_at = %s
    WHERE joined_at IS NULL
    """, (datetime.now(),))

    conn.commit()
    cursor.close()
    conn.close()

def clear_payments_table():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Очищаємо таблицю payments
    cursor.execute("TRUNCATE TABLE payments;")
    conn.commit()

    cursor.close()
    conn.close()
    print("Таблиця payments успішно очищена.")

# Викликаємо функцію для створення таблиць при запуску
create_tables()

# Оновлюємо поле joined_at для існуючих користувачів
update_existing_users_joined_at()

# Очищення таблиці payments
clear_payments_table()

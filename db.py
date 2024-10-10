import psycopg2
import os

# Отримуємо URL бази даних з змінної середовища Heroku
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    # Підключаємось до бази даних PostgreSQL через URL з Heroku
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def create_users_table():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Створюємо таблицю, якщо її ще не існує
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        phone_number VARCHAR(20) UNIQUE NOT NULL,
        telegram_id BIGINT NOT NULL,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        employee_name VARCHAR(70)
    )
    """)
    conn.commit()
    cursor.close()
    conn.close()

def add_telegram_user(phone_number, telegram_id, first_name, last_name, employee_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Додаємо користувача в таблицю
    cursor.execute("""
    INSERT INTO users (phone_number, telegram_id, first_name, last_name, employee_name)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (phone_number) DO NOTHING
    """, (phone_number, telegram_id, first_name, last_name, employee_name))

    conn.commit()
    cursor.close()
    conn.close()

# Викликаємо функцію для створення таблиці при запуску
create_users_table()

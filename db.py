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

    # Створюємо таблицю користувачів з новими назвами стовпців
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        phone_number VARCHAR(20) UNIQUE NOT NULL,
        telegram_id BIGINT NOT NULL,
        telegram_name VARCHAR(50),  -- Заміна first_name на telegram_name
        employee_name VARCHAR(50),  -- Заміна last_name на employee_name
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

def add_telegram_user(phone_number, telegram_id, telegram_name, employee_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Оновлюємо дату приєднання для нових користувачів з новими назвами стовпців
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

    # Додаємо запис про виплату
    cursor.execute("""
    INSERT INTO payments (phone_number, amount, currency, payment_date, payment_number)
    VALUES (%s, %s, %s, %s, %s)
    """, (phone_number, amount, currency, payment_date, payment_number))

    conn.commit()
    cursor.close()
    conn.close()

def get_user_joined_at(phone_number):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Отримуємо дату приєднання користувача за номером телефону
    cursor.execute("""
    SELECT joined_at FROM users WHERE phone_number = %s
    """, (phone_number,))
    result = cursor.fetchone()

    conn.close()

    if result:
        return result[0]  # Повертаємо дату приєднання
    return None

# Викликаємо функцію для створення таблиць при запуску
create_tables()








# Отримуємо URL бази даних з змінної середовища Heroku
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    # Підключаємось до бази даних PostgreSQL через URL з Heroku
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def rename_columns_in_users_table():
    """
    Змінює назви стовпців у таблиці users:
    first_name -> telegram_name
    last_name -> employee_name
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Перейменування стовпців
    try:
        cursor.execute("""
        ALTER TABLE users
        RENAME COLUMN first_name TO telegram_name;
        """)
        cursor.execute("""
        ALTER TABLE users
        RENAME COLUMN last_name TO employee_name;
        """)
        conn.commit()
        print("Назви стовпців успішно змінені.")
    except Exception as e:
        print(f"Помилка при зміні назв стовпців: {e}")
    finally:
        cursor.close()
        conn.close()

# Виклик функції для зміни назв стовпців
rename_columns_in_users_table()

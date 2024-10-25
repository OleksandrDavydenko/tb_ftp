import psycopg2
import os
import time
from telegram import Bot
import logging
from key import KEY  # Імпортуємо токен з key.py

# Отримуємо URL бази даних з змінної середовища Heroku
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    # Підключаємось до бази даних PostgreSQL через URL з Heroku
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def check_new_payments():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Отримуємо нові виплати, про які ще не було сповіщено
    cursor.execute("""
    SELECT phone_number, amount, currency, payment_date, payment_number
    FROM payments
    WHERE is_notified = FALSE
    """)
    new_payments = cursor.fetchall()

    for payment in new_payments:
        phone_number, amount, currency, payment_date, payment_number = payment

        # Отримуємо Telegram ID користувача
        cursor.execute("SELECT telegram_id FROM users WHERE phone_number = %s", (phone_number,))
        user_data = cursor.fetchone()

        if user_data:
            telegram_id = user_data[0]
            # Відправляємо сповіщення
            send_notification(telegram_id, amount, currency, payment_number)

            # Оновлюємо статус сповіщення
            cursor.execute("""
            UPDATE payments
            SET is_notified = TRUE
            WHERE phone_number = %s AND amount = %s AND payment_date = %s AND payment_number = %s
            """, (phone_number, amount, payment_date, payment_number))

    conn.commit()
    cursor.close()
    conn.close()

def send_notification(telegram_id, amount, currency, payment_number):
    try:
        bot = Bot(token=KEY)  # Використовуємо токен з key.py
        message = f"Доброго дня! Відбулась виплата на суму {amount} {currency} (№ {payment_number})."
        bot.send_message(chat_id=telegram_id, text=message)
    except Exception as e:
        logging.error(f"Помилка при відправці сповіщення: {e}")

def run_periodic_check():
    while True:
        try:
            check_new_payments()
        except Exception as e:
            logging.error(f"Помилка при перевірці нових платежів: {e}")
        time.sleep(30)  # Перевірка кожні 30 секунд

if __name__ == '__main__':
    run_periodic_check()

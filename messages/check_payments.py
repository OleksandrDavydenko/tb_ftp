import psycopg2
import os
import time
from telegram import Bot
import logging
import asyncio
from key import KEY  # Імпортуємо токен з key.py

# Налаштовуємо logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Отримуємо URL бази даних з змінної середовища Heroku
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        logging.info("Підключення до бази даних успішне.")
        return conn
    except Exception as e:
        logging.error(f"Помилка при підключенні до бази даних: {e}")
        return None

async def check_new_payments():
    conn = get_db_connection()
    if not conn:
        logging.error("Не вдалося підключитися до бази даних.")
        return

    cursor = conn.cursor()

    try:
        # Отримуємо нові виплати, про які ще не було сповіщено
        cursor.execute("""
        SELECT phone_number, amount, currency, payment_date, payment_number
        FROM payments
        WHERE is_notified = FALSE
        """)
        new_payments = cursor.fetchall()

        logging.info(f"Знайдено {len(new_payments)} нових платежів для обробки.")

        for payment in new_payments:
            phone_number, amount, currency, payment_date, payment_number = payment
            logging.info(f"Обробка платежу: {phone_number}, {amount}, {currency}, {payment_date}, {payment_number}")

            # Отримуємо Telegram ID користувача
            cursor.execute("SELECT telegram_id FROM users WHERE phone_number = %s", (phone_number,))
            user_data = cursor.fetchone()

            if user_data:
                telegram_id = user_data[0]
                logging.info(f"Знайдено Telegram ID: {telegram_id} для користувача з номером: {phone_number}")
                
                # Відправка сповіщення
                await send_notification(telegram_id, amount, currency, payment_number)

                # Оновлення статусу сповіщення
                cursor.execute("""
                UPDATE payments
                SET is_notified = TRUE
                WHERE phone_number = %s AND amount = %s AND payment_date = %s AND payment_number = %s
                """, (phone_number, amount, payment_date, payment_number))
                logging.info(f"Статус сповіщення оновлено для платежу: {payment_number}")
            else:
                logging.warning(f"Telegram ID для номера {phone_number} не знайдено.")

        conn.commit()
    except Exception as e:
        logging.error(f"Помилка при обробці нових платежів: {e}")
    finally:
        cursor.close()
        conn.close()

async def send_notification(telegram_id, amount, currency, payment_number):
    try:
        bot = Bot(token=KEY)
        message = f"Доброго дня! Відбулась виплата на суму {amount} {currency} (№ {payment_number})."
        logging.info(f"Спроба відправити повідомлення: '{message}' до Telegram ID: {telegram_id}")
        await bot.send_message(chat_id=telegram_id, text=message)
        logging.info(f"Повідомлення успішно відправлено до Telegram ID: {telegram_id}")
    except Exception as e:
        logging.error(f"Помилка при відправці сповіщення: {e}")

async def run_periodic_check():
    while True:
        try:
            logging.info("Початок перевірки нових платежів.")
            await check_new_payments()
            logging.info("Завершення перевірки нових платежів.")
        except Exception as e:
            logging.error(f"Помилка при перевірці нових платежів: {e}")
        await asyncio.sleep(30)  # Перевірка кожні 30 секунд

if __name__ == '__main__':
    logging.info("Запуск асинхронного циклу для періодичної перевірки платежів.")
    asyncio.run(run_periodic_check())

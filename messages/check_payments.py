import requests
import psycopg2
import os
import logging
import asyncio
from datetime import datetime
from telegram import Bot
from telegram.error import NetworkError, TelegramError
from key import KEY
from sync_payments import async_sync_payments  # Використовуємо асинхронну версію sync_payments
from auth import get_power_bi_token

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

async def check_new_payments():
    """Перевірка нових платежів та надсилання сповіщень користувачам."""
    logging.info("Перевірка нових платежів розпочата.")
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        SELECT phone_number, amount, currency, payment_date, payment_number
        FROM payments
        WHERE is_notified = FALSE
        """)
        new_payments = cursor.fetchall()

        if new_payments:
            logging.info(f"Знайдено {len(new_payments)} нових платежів.")
        else:
            logging.info("Немає нових платежів.")

        for payment in new_payments:
            phone_number, amount, currency, payment_date, payment_number = payment

            cursor.execute("SELECT telegram_id FROM users WHERE phone_number = %s", (phone_number,))
            user_data = cursor.fetchone()

            if user_data:
                telegram_id = user_data[0]
                logging.info(f"Надсилаємо сповіщення користувачу з Telegram ID: {telegram_id}")
                await send_notification(telegram_id, amount, currency, payment_number)

                cursor.execute("""
                UPDATE payments
                SET is_notified = TRUE
                WHERE phone_number = %s AND amount = %s AND currency = %s 
                AND payment_date = %s AND payment_number = %s
                """, (phone_number, amount, currency, payment_date, payment_number))
            else:
                logging.warning(f"Не знайдено Telegram ID для номера: {phone_number}")

        conn.commit()

    except Exception as e:
        logging.error(f"Помилка під час перевірки нових платежів: {e}")

    finally:
        cursor.close()
        conn.close()

async def async_sync_all_users():
    """Асинхронна синхронізація платежів для всіх користувачів."""
    logging.info("Початок періодичної синхронізації платежів.")
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Отримуємо всіх користувачів із таблиці users
        cursor.execute("SELECT phone_number, first_name, joined_at FROM users")
        users = cursor.fetchall()

        tasks = [
            async_sync_payments(employee_name, phone_number, joined_at) 
            for phone_number, employee_name, joined_at in users
        ]

        # Виконуємо асинхронну синхронізацію для всіх користувачів одночасно
        await asyncio.gather(*tasks)

    except Exception as e:
        logging.error(f"Помилка під час періодичної синхронізації: {e}")

    finally:
        cursor.close()
        conn.close()

async def send_notification(telegram_id, amount, currency, payment_number):
    """Асинхронне надсилання сповіщення користувачу."""
    bot = Bot(token=KEY)
    message = f"Доброго дня! Відбулась виплата на суму {amount} {currency} (№ {payment_number})."

    try:
        await bot.send_message(chat_id=telegram_id, text=message)
        logging.info(f"Сповіщення відправлено: {message}")
    except NetworkError as ne:
        logging.error(f"Помилка мережі при відправці сповіщення: {ne}")
    except TelegramError as te:
        logging.error(f"Telegram помилка при відправці сповіщення: {te}")
    except Exception as e:
        logging.error(f"Інша помилка при відправці сповіщення: {e}")

async def run_periodic_check():
    """Основний цикл перевірки нових платежів та синхронізації."""
    while True:
        await check_new_payments()
        await async_sync_all_users()  # Асинхронний виклик синхронізації для всіх користувачів
        await asyncio.sleep(30)

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run_periodic_check())
    except KeyboardInterrupt:
        logging.info("Зупинка перевірки платежів вручну.")
    except Exception as e:
        logging.error(f"Головна помилка: {e}")

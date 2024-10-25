import psycopg2
import os
import logging
import asyncio
from telegram import Bot
from telegram.error import NetworkError, TelegramError
from key import KEY
from sync_payments import sync_payments  # Імпортуємо функцію синхронізації

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

async def check_new_payments():
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
                WHERE phone_number = %s AND amount = %s AND payment_date = %s AND payment_number = %s
                """, (phone_number, amount, payment_date, payment_number))
            else:
                logging.warning(f"Не знайдено Telegram ID для номера: {phone_number}")

        conn.commit()

    except Exception as e:
        logging.error(f"Помилка під час перевірки нових платежів: {e}")

    finally:
        cursor.close()
        conn.close()

async def sync_all_users():
    logging.info("Початок періодичної синхронізації платежів.")
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Отримуємо всіх користувачів із таблиці users
        cursor.execute("SELECT phone_number, first_name, joined_at FROM users")
        users = cursor.fetchall()

        for user in users:
            phone_number, employee_name, joined_at = user
            logging.info(f"Синхронізація платежів для користувача: {employee_name} ({phone_number})")
            try:
                await sync_payments(employee_name, phone_number, joined_at)
                logging.info(f"Успішно синхронізовано для користувача: {employee_name}")
            except Exception as e:
                logging.error(f"Помилка при синхронізації для {employee_name}: {e}")

    except Exception as e:
        logging.error(f"Помилка під час періодичної синхронізації: {e}")

    finally:
        cursor.close()
        conn.close()

async def send_notification(telegram_id, amount, currency, payment_number):
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
    while True:
        await check_new_payments()
        await sync_all_users()  # Додаємо періодичну синхронізацію платежів
        await asyncio.sleep(30)  # Інтервал між перевірками

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run_periodic_check())
    except KeyboardInterrupt:
        logging.info("Зупинка перевірки платежів вручну.")
    except Exception as e:
        logging.error(f"Головна помилка: {e}")

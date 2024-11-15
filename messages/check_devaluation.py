import psycopg2
import os
import logging
from telegram import Bot

KEY = os.getenv('TELEGRAM_BOT_TOKEN')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

async def check_new_devaluation_records():
    logging.info("Перевірка нових записів девальвації розпочата.")
    conn = get_db_connection()
    cursor = conn.cursor()

    # Отримуємо нові записи без сповіщення
    cursor.execute("""
    SELECT client, payment_number, acc_number, contract_number, date_from_acc, 
           date_from_payment, date_difference_in_days, currency_from_inform_acc,
           exchange_rate_acc_nbu, exchange_rate_payment_nbu, devaluation_percentage,
           payment_sum, compensation, manager
    FROM DevaluationAnalysis
    WHERE is_notified = FALSE
    """)
    new_records = cursor.fetchall()

    for record in new_records:
        client, payment_number, acc_number, contract_number, date_from_acc, \
        date_from_payment, date_difference_in_days, currency_from_inform_acc, \
        exchange_rate_acc_nbu, exchange_rate_payment_nbu, devaluation_percentage, \
        payment_sum, compensation, manager = record

        # Пошук Telegram ID для відповідного менеджера
        cursor.execute("SELECT telegram_id FROM users WHERE employee_name = %s", (manager,))
        manager_data = cursor.fetchone()

        # Пошук Telegram ID для Давиденка Олександра
        cursor.execute("SELECT telegram_id FROM users WHERE employee_name = 'Давиденко Олександр'")
        davidenko_data = cursor.fetchone()

        # Формуємо повідомлення
        message = (
            f"📉 Новий запис девальвації:\n"
            f"Клієнт: {client}\n"
            f"Номер платежу: {payment_number}\n"
            f"Сума: {payment_sum} {currency_from_inform_acc}\n"
            f"Відсоток девальвації: {devaluation_percentage}%\n"
            f"Менеджер: {manager}"
        )

        # Відправляємо повідомлення менеджеру, якщо його знайдено
        if manager_data:
            telegram_id = manager_data[0]
            logging.info(f"Надсилаємо сповіщення менеджеру з Telegram ID: {telegram_id}")
            await send_notification(telegram_id, message)
        else:
            logging.warning(f"Менеджер {manager} не знайдений у базі даних.")

        # Відправляємо повідомлення Давиденку Олександру, якщо його знайдено
        if davidenko_data:
            davidenko_id = davidenko_data[0]
            logging.info(f"Надсилаємо сповіщення Давиденку Олександру з Telegram ID: {davidenko_id}")
            await send_notification(davidenko_id, message)
        else:
            logging.warning("Давиденко Олександр не знайдений у базі даних.")

        # Оновлюємо статус сповіщення
        cursor.execute("""
        UPDATE DevaluationAnalysis
        SET is_notified = TRUE
        WHERE client = %s AND payment_number = %s
        """, (client, payment_number))

    conn.commit()
    cursor.close()
    conn.close()

async def send_notification(telegram_id, message):
    try:
        bot = Bot(token=KEY)
        await bot.send_message(chat_id=telegram_id, text=message)
        logging.info(f"Сповіщення відправлено: {message}")
    except Exception as e:
        logging.error(f"Помилка при відправці сповіщення: {e}")

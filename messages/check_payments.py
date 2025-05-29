import psycopg2
import os
import logging
from collections import defaultdict
from telegram import Bot
from datetime import datetime

KEY = os.getenv('TELEGRAM_BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

async def check_new_payments():
    logging.info("Перевірка нових платежів розпочата.")
    conn = get_db_connection()
    cursor = conn.cursor()

    # Крок 1: знайти унікальні пари payment_number + phone_number
    cursor.execute("""
    SELECT DISTINCT payment_number, phone_number
    FROM payments
    WHERE is_notified = FALSE
    """)
    payment_groups = cursor.fetchall()

    for payment_number, phone_number in payment_groups:
        # Крок 2: дістати всі записи по цій парі
        cursor.execute("""
        SELECT phone_number, amount, currency, payment_date, payment_number, accrual_month
        FROM payments
        WHERE payment_number = %s AND phone_number = %s
        """, (payment_number, phone_number))
        payments = cursor.fetchall()

        if not payments:
            continue

        currency = payments[0][2]

        # Отримуємо Telegram ID
        cursor.execute("SELECT telegram_id FROM users WHERE phone_number = %s", (phone_number,))
        user_data = cursor.fetchone()

        if not user_data:
            logging.warning(f"Не знайдено Telegram ID для номера: {phone_number}")
            continue

        telegram_id = user_data[0]

        # Підготовка суми по кожному періоду
        amounts_by_month = defaultdict(float)
        for p in payments:
            accrual_month = p[5]
            amounts_by_month[accrual_month] += float(p[1])
        
        payment_date = payments[0][3]

        # Надсилаємо повідомлення
        await send_notification(telegram_id, amounts_by_month, currency, payment_number, payment_date)

        # Позначаємо ці платежі як повідомлені
        cursor.execute("""
        UPDATE payments
        SET is_notified = TRUE
        WHERE payment_number = %s AND phone_number = %s
        """, (payment_number, phone_number))

    conn.commit()
    cursor.close()
    conn.close()

async def send_notification(telegram_id, amounts_by_month, currency, payment_number, payment_date):
    MONTHS_UA = {
        "01": "Січень", "02": "Лютий", "03": "Березень", "04": "Квітень",
        "05": "Травень", "06": "Червень", "07": "Липень", "08": "Серпень",
        "09": "Вересень", "10": "Жовтень", "11": "Листопад", "12": "Грудень"
    }
    formatted_periods = {
        f"{MONTHS_UA.get(month[-2:], month)}": amount
        for month, amount in amounts_by_month.items()
    }

    try:
        bot = Bot(token=KEY)

        details = "\n".join(
            [f"• {month} – {amount:.2f} {currency}" for month, amount in formatted_periods.items()]
        )
        total_amount = sum(amounts_by_month.values())
        formatted_date = payment_date.strftime('%d.%m.%Y')

        message = (
            f"💸 *Здійснена виплата!*\n"
            f"📄 *Документ №:* {payment_number} від {formatted_date}\n\n"
            f"📅 *Періоди та суми:*\n"
            f"{details}\n\n"
            f"💰 *Загальна сума:* {total_amount:.2f} {currency}"
        )

        await bot.send_message(chat_id=telegram_id, text=message, parse_mode="Markdown")
        logging.info(f"Сповіщення відправлено: {message}")
    except Exception as e:
        logging.error(f"Помилка при відправці сповіщення: {e}")

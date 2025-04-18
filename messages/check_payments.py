import psycopg2
import os
import logging
from collections import defaultdict
from telegram import Bot

KEY = os.getenv('TELEGRAM_BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

async def check_new_payments():
    logging.info("Перевірка нових платежів розпочата.")
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT phone_number, amount, currency, payment_date, payment_number, accrual_month
    FROM payments
    WHERE is_notified = FALSE
    """)
    new_payments = cursor.fetchall()

    # Групуємо за номером документа
    grouped_by_document = defaultdict(list)
    for payment in new_payments:
        payment_number = payment[4]
        grouped_by_document[payment_number].append(payment)

    for payment_number, payments in grouped_by_document.items():
        phone_number = payments[0][0]
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

        # Формування повідомлення
        await send_notification(telegram_id, amounts_by_month, currency, payment_number)

        # Позначаємо платежі як сповіщені
        for p in payments:
            cursor.execute("""
            UPDATE payments
            SET is_notified = TRUE
            WHERE phone_number = %s AND amount = %s AND payment_date = %s AND payment_number = %s
            """, (p[0], p[1], p[3], p[4]))

    conn.commit()
    cursor.close()
    conn.close()

async def send_notification(telegram_id, amounts_by_month, currency, payment_number):
    try:
        bot = Bot(token=KEY)

        details = "\n".join([f"• {month} – {amount:.2f} {currency}" for month, amount in amounts_by_month.items()])
        total_amount = sum(amounts_by_month.values())

        message = (
            f"💸 *Здійснена виплата!*\n"
            f"📄 *Документ №:* {payment_number}\n\n"
            f"📅 *Періоди та суми:*\n"
            f"{details}\n\n"
            f"💰 *Загальна сума:* {total_amount:.2f} {currency}"
        )

        await bot.send_message(chat_id=telegram_id, text=message, parse_mode="Markdown")
        logging.info(f"Сповіщення відправлено: {message}")
    except Exception as e:
        logging.error(f"Помилка при відправці сповіщення: {e}")

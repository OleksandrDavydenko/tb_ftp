import psycopg2
import os
import logging
from telegram import Bot
from utils.name_aliases import display_name

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

        # У тексті — псевдонім; пошук у БД нижче лишається за справжнім ім'ям
        nice_manager = display_name(manager)

        # Формуємо повідомлення
        message = (
            f"📉 Новий запис девальвації:\n\n"
            f"Клієнт: {client}\n"
            f"Номер платежу: {payment_number}\n"
            f"Сума: {payment_sum} грн.\n"
            f"Валюта заявки: {currency_from_inform_acc}\n"
            f"Відсоток девальвації: {devaluation_percentage}%\n"
            f"Менеджер: {nice_manager}\n\n"

            f"📝 Важливо:\n"
            f"Відповідальний співробітник {nice_manager}, будь ласка, перевірте наявність пункту про девальвацію в договорі з клієнтом.\n\n"

            f"🔍 Деталі угоди:\n"
            f"Номер угоди: {contract_number}\n"
            f"Рахунок №: {acc_number}\n"
            f"Рахунок виставлений клієнту на дату: {date_from_acc}\n"
            f"Курс НБУ на дату виставлення рахунку: {exchange_rate_acc_nbu}\n"
            f"Дата оплати: {date_from_payment}\n"
            f"Курс НБУ на дату оплати: {exchange_rate_payment_nbu}\n"
            f"Термін прострочення: {date_difference_in_days} днів\n"
            f"№ документа оплати: {payment_number}\n"

            f"📄 Необхідні дії:\n"
            f"1️⃣ Перевірте умови договору щодо компенсації девальвації у юридичному відділі.\n"
            f"2️⃣ Після отримання підтвердження зверніться до бухгалтерії з проханням виставити додатковий рахунок клієнту на суму компенсації.\n\n"

            f"💰 Сума компенсації до виставлення: {compensation} грн\n"
        )

        # Підготовка списку Telegram ID
        telegram_ids = [203148640, 225659191, 1852978563]  # Давиденко і Ступа

        # Додаємо менеджера, якщо знайдений і він не дублюється
        cursor.execute("SELECT telegram_id FROM users WHERE employee_name = %s", (manager,))
        manager_data = cursor.fetchone()
        if manager_data and manager_data[0] not in telegram_ids:
            telegram_ids.append(manager_data[0])
        elif not manager_data:
            logging.warning(f"Менеджер {manager} не знайдений у базі даних.")

        # Відправляємо повідомлення всім з отриманого списку
        for telegram_id in telegram_ids:
            try:
                await send_notification(telegram_id, message)
                logging.info(f"Повідомлення відправлено Telegram ID: {telegram_id}")
            except Exception as e:
                logging.error(f"Помилка при відправці повідомлення Telegram ID {telegram_id}: {e}")

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
    except Exception as e:
        logging.error(f"Помилка при відправці повідомлення: {e}")

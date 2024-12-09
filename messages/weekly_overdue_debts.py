from datetime import datetime
from db import get_db_connection  # Функція для підключення до бази даних
from telegram import Bot
import logging

# Логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ID бота для надсилання повідомлень
BOT_TOKEN = "YOUR_BOT_TOKEN"
bot = Bot(token=BOT_TOKEN)

# Telegram ID Давиденко Олександра
USER_TELEGRAM_ID = 203148640  # Замініть на реальний ID

def get_overdue_debts(manager_name):
    """
    Отримує прострочену дебіторську заборгованість для вказаного менеджера.
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        query = """
            SELECT Client, Sum_$, PlanDatePay
            FROM Deb
            WHERE Manager = %s AND PlanDatePay < %s
        """
        today = datetime.now().date()
        cursor.execute(query, (manager_name, today))

        rows = cursor.fetchall()
        connection.close()

        overdue_debts = [{"client": row[0], "sum": row[1], "plan_date_pay": row[2]} for row in rows]
        return overdue_debts
    except Exception as e:
        logging.error(f"Помилка отримання простроченої заборгованості: {e}")
        return []

def send_overdue_debt_message(telegram_id, debts):
    """
    Відправляє повідомлення з інформацією про прострочену заборгованість.
    """
    if not debts:
        message = "У вас немає простроченої дебіторської заборгованості. 🎉"
    else:
        message = "📋 Прострочена дебіторська заборгованість:\n"
        for debt in debts:
            message += f"Клієнт: {debt['client']}, Сума: ${debt['sum']:.2f}, Планована дата оплати: {debt['plan_date_pay']}\n"
    
    try:
        bot.send_message(chat_id=telegram_id, text=message)
        logging.info(f"Повідомлення надіслано користувачу {telegram_id}.")
    except Exception as e:
        logging.error(f"Помилка надсилання повідомлення користувачу {telegram_id}: {e}")

def check_and_notify_overdue_debts():
    """
    Перевіряє та надсилає повідомлення про прострочену дебіторську заборгованість.
    """
    manager_name = "Давиденко Олександр"  # Тестовий менеджер
    overdue_debts = get_overdue_debts(manager_name)
    send_overdue_debt_message(USER_TELEGRAM_ID, overdue_debts)

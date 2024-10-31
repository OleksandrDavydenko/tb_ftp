import logging
import os
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from db import get_all_users  # Імпортуємо функцію для отримання списку всіх користувачів

KEY = os.getenv('TELEGRAM_BOT_TOKEN')

# Ініціалізуємо бота
bot = Bot(token=KEY)

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_previous_month():
    """Отримуємо назву попереднього місяця українською."""
    current_month = datetime.now().month
    previous_month = current_month - 1 if current_month > 1 else 12
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    return months[previous_month - 1]

async def send_reminder_to_all_users():
    """Відправляє нагадування всім користувачам про закриття угод."""
    users = get_all_users()  # Отримуємо список усіх користувачів з БД
    previous_month = get_previous_month()
    message = f"Нагадування: будь ласка, закрийте свої угоди за {previous_month}."

    for user in users:
        try:
            await bot.send_message(chat_id=user['telegram_id'], text=message)
            logging.info(f"Нагадування відправлено користувачу: {user['telegram_name']}")
        except Exception as e:
            logging.error(f"Помилка при відправці повідомлення користувачу {user['telegram_name']}: {e}")

def schedule_monthly_reminder(scheduler):
    """Налаштовуємо планувальник для нагадування кожні 2 хвилини для тестування."""
    scheduler.add_job(
        send_reminder_to_all_users,
        'interval',
        minutes=2
    )
    logging.info("Планувальник нагадувань кожні 2 хвилини запущено.")

import asyncio
import logging
import os
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from db import get_all_users  # Імпортуємо функцію для отримання списку всіх користувачів

KEY = os.getenv('TELEGRAM_BOT_TOKEN')

bot = Bot(token=KEY)



def get_previous_month():
    """Отримати назву попереднього місяця українською."""
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

    # Повідомлення для користувачів
    message = f"Нагадування: будь ласка, закрийте свої угоди за {previous_month}."

    for user in users:
        try:
            # Відправляємо повідомлення кожному користувачу
            await bot.send_message(chat_id=user['telegram_id'], text=message)
            logging.info(f"Нагадування відправлено користувачу: {user['telegram_name']}")
        except Exception as e:
            logging.error(f"Помилка при відправці повідомлення користувачу {user['telegram_name']}: {e}")

def schedule_monthly_reminder():
    """Налаштовуємо планувальник для щомісячного нагадування 5-го числа о 10:00."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        lambda: asyncio.run(send_reminder_to_all_users()), 
        'cron', 
        day=31, 
        hour=0, 
        minute=20
    )
    scheduler.start()
    logging.info("Щомісячний планувальник нагадувань запущено.")

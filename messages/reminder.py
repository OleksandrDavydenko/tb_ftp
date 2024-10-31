import logging
import os
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from db import get_all_users
from pytz import timezone

KEY = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=KEY)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для отримання української назви попереднього місяця
def get_previous_month():
    current_month = datetime.now().month
    previous_month = current_month - 1 if current_month > 1 else 12
    months_ua = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    return months_ua[previous_month - 1]

# Асинхронна функція для відправки нагадування всім користувачам
async def send_reminder_to_all_users():
    users = get_all_users()
    
    # Визначаємо попередній місяць та дату для повідомлення
    previous_month_name = get_previous_month()
    reminder_date = f"07.{datetime.now().strftime('%m')}"

    # Формуємо повідомлення
    message = (
        f"Нагадування!\n"
        f"Колеги, закриваємо {previous_month_name.upper()} місяць 💪\n"
        f"Прошу усіх в термін до {reminder_date} включно, завершити свої угоди в Експедиторі.\n\n"
        "Продуктивного дня."
    )

    # Відправляємо повідомлення кожному користувачу
    for user in users:
        try:
            await bot.send_message(chat_id=user['telegram_id'], text=message)
            logging.info(f"Нагадування відправлено користувачу: {user['telegram_name']}")
        except Exception as e:
            logging.error(f"Помилка при відправці повідомлення користувачу {user['telegram_name']}: {e}")

# Функція для налаштування щомісячного нагадування

def schedule_monthly_reminder(scheduler):
    scheduler.add_job(
        send_reminder_to_all_users,
        'cron',
        day=5,
        hour=10,
        minute=00,
        misfire_grace_time=60,  # Дозволяє завданню пропустити запуск, якщо є затримка
        timezone='Europe/Kiev'  # Вказуємо часовий пояс
    )
    logging.info("Планувальник щомісячного нагадування на 31 число о 15:00 за київським часом запущено.")

    logging.info("Планувальник щомісячного нагадування на 5 число об 11:00 запущено.")

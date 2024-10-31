import logging
import os
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from datetime import datetime, timedelta
import calendar
from db import get_all_users

KEY = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=KEY)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_previous_month():
    current_month = datetime.now().month
    previous_month = current_month - 1 if current_month > 1 else 12
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    return months[previous_month - 1]

async def send_reminder_to_all_users():
    users = get_all_users()
    
    # Отримуємо поточну дату
    today = datetime.today()
    
    # Визначаємо попередній місяць
    previous_month_date = today - timedelta(days=today.day)
    previous_month_name = calendar.month_name[previous_month_date.month]
    
    # Формуємо повідомлення
    message = (
        f"Закриваємо {previous_month_name.upper()} місяць 💪\n"
        f"Прошу усіх в термін до {today.strftime('%d.%m')} включно, завершити свої угоди в Експедиторі.\n\n"
        "Продуктивного дня."
    )

    for user in users:
        try:
            await bot.send_message(chat_id=user['telegram_id'], text=message)
            logging.info(f"Нагадування відправлено користувачу: {user['telegram_name']}")
        except Exception as e:
            logging.error(f"Помилка при відправці повідомлення користувачу {user['telegram_name']}: {e}")

def schedule_monthly_reminder(scheduler):
    scheduler.add_job(
        send_reminder_to_all_users,
        'interval',
        minutes=2,
        misfire_grace_time=60  # Дозволяє завданню пропустити запуск, якщо є затримка
    )
    logging.info("Планувальник нагадувань кожні 2 хвилини запущено.")

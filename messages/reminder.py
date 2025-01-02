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

# Функція для перевірки, чи є день вихідним
def is_weekend(date):
    return date.weekday() >= 5  # 5 - субота, 6 - неділя

# Отримуємо дату наступного робочого дня
def get_next_workday(date):
    while is_weekend(date):
        date += timedelta(days=1)
    return date

# Асинхронна функція для відправки нагадування всім користувачам
async def send_reminder_to_all_users():
    users = get_all_users()
    
    # Визначаємо попередній місяць та дату для повідомлення
    previous_month_name = get_previous_month()
    now = datetime.now()
    reminder_date = f"07.{now.strftime('%m')}"
    
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
    # Перевіряємо, чи 1 число місяця є вихідним, і налаштовуємо запуск на найближчий робочий день
    now = datetime.now(timezone('Europe/Kiev'))
    first_day_of_month = datetime(now.year, now.month, 1, 10, 0, tzinfo=timezone('Europe/Kiev'))
    next_workday = get_next_workday(first_day_of_month)

    # Додаємо задачу в планувальник
    scheduler.add_job(
        send_reminder_to_all_users,
        'date',
        run_date=next_workday,  # Наступного понеділка має прийти повідомлення, якщо зараз вихідний
        misfire_grace_time=60,  # Дозволяє завданню пропустити запуск, якщо є затримка
        timezone='Europe/Kiev'  # Вказуємо часовий пояс
    )

    logging.info(
        f"Планувальник щомісячного нагадування налаштовано на {next_workday.strftime('%Y-%m-%d %H:%M')} за київським часом."
    )

import logging
import os
from datetime import datetime, timedelta
from telegram import Bot
from db import get_active_users
from pytz import timezone

# Ініціалізація бота
KEY = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=KEY)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Список державних свят в Україні (формат: MM-DD)
HOLIDAYS = []

# Функція для отримання української назви попереднього місяця
def get_previous_month():
    current_month = datetime.now().month
    previous_month = current_month - 1 if current_month > 1 else 12
    months_ua = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    return months_ua[previous_month - 1]

# Функція для перевірки, чи є день вихідним або святковим
def is_holiday_or_weekend(date):
    return date.weekday() >= 5 or date.strftime("%m-%d") in HOLIDAYS

# Отримуємо дату наступного робочого дня
def get_next_workday(date):
    while is_holiday_or_weekend(date):
        date += timedelta(days=1)
    return date

# Асинхронна функція для відправки нагадування всім користувачам
async def send_reminder_to_all_users():
    users = get_active_users()
    previous_month_name = get_previous_month()
    now = datetime.now()
    reminder_date = f"07.{now.strftime('%m')}"

    message = (
        f"Нагадування!\n"
        f"Колеги, закриваємо {previous_month_name.upper()} місяць 💪\n"
        f"Прошу усіх в термін до {reminder_date} включно, завершити свої угоди в Експедиторі.\n\n"
        "Продуктивного дня."
    )

    for user in users:
        try:
            await bot.send_message(chat_id=user['telegram_id'], text=message)
            logging.info(f"Нагадування відправлено користувачу: {user['telegram_name']}")
        except Exception as e:
            logging.error(f"Помилка при відправці повідомлення користувачу {user['telegram_name']}: {e}")

# Функція для отримання дати запуску наступного нагадування
def get_next_reminder_date():
    now = datetime.now(timezone('Europe/Kiev'))
    first_day_of_next_month = datetime(
        now.year + (now.month // 12),
        (now.month % 12) + 1,
        1, 10, 0, tzinfo=timezone('Europe/Kiev')
    )
    return get_next_workday(first_day_of_next_month)

# Функція для отримання дати запуску першого нагадування цього місяця
def get_this_month_reminder_date():
    now = datetime.now(timezone('Europe/Kiev'))
    first_day_of_month = datetime(now.year, now.month, 1, 10, 30, tzinfo=timezone('Europe/Kiev'))
    return get_next_workday(first_day_of_month)

import logging
import os
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from db import get_active_users, get_test_user
from pytz import timezone
from apscheduler.events import EVENT_JOB_EXECUTED  # Імпортуємо подію

# Ініціалізація бота
KEY = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=KEY)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Список державних свят в Україні (формат: MM-DD)
HOLIDAYS = [
    "01-01",  # Новий рік
    "25-12",  # Різдво Христове
    "08-03",  # Міжнародний жіночий день
    "04-05",  # День праці
    "09-05",  # День перемоги
    "28-06",  # День Конституції України
    "24-08",  # День Незалежності України
    "14-10",  # День захисників і захисниць України
]

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
    #users = get_active_users()
    users = get_test_user()
    
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

# Функція для переналаштування нагадування на наступний місяць
def reschedule_next_month(scheduler):
    now = datetime.now(timezone('Europe/Kiev'))
    first_day_next_month = datetime(
        now.year + (now.month // 12), 
        (now.month % 12) + 1, 
        1, 
        10, 
        0, 
        tzinfo=timezone('Europe/Kiev')
    )
    next_workday = get_next_workday(first_day_next_month)
    
    scheduler.add_job(
        send_reminder_to_all_users,
        'date',
        run_date=next_workday,
        misfire_grace_time=60,
        timezone='Europe/Kiev',
        id=f"monthly_reminder_{next_workday.strftime('%Y%m%d')}"
    )

    logging.info(
        f"Наступне нагадування заплановано на {next_workday.strftime('%Y-%m-%d %H:%M')} за київським часом."
    )

# Функція для налаштування щомісячного нагадування

def schedule_monthly_reminder(scheduler):
    """
    Додає щомісячну задачу нагадування в планувальник на найближчий робочий день після 1-го числа місяця.
    Якщо цей день уже минув — задача не додається.
    """
    now = datetime.now(timezone('Europe/Kiev'))
    first_day_of_month = datetime(now.year, now.month, 1, 10, 23, tzinfo=timezone('Europe/Kiev'))
    next_workday = get_next_workday(first_day_of_month)

    if next_workday > now:
        scheduler.add_job(
            send_reminder_to_all_users,
            'date',
            run_date=next_workday,
            misfire_grace_time=60,
            timezone='Europe/Kiev',
            id=f"monthly_reminder_{next_workday.strftime('%Y%m%d')}"
        )

        logging.info(
            f"✅ Нагадування про закриття місяця заплановано на {next_workday.strftime('%Y-%m-%d %H:%M')} за Києвом."
        )
    else:
        logging.warning(
            f"⚠️ Нагадування не було заплановано, бо найближчий робочий день ({next_workday.strftime('%Y-%m-%d')}) вже в минулому."
        )

    # Автоматичне переналаштування після виконання задачі
    scheduler.add_listener(
        lambda event: reschedule_next_month(scheduler) if event.job_id.startswith("monthly_reminder_") else None,
        EVENT_JOB_EXECUTED
    )

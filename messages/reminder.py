import logging
import os
from datetime import datetime, timedelta
from telegram import Bot
from db import get_active_users
from pytz import timezone

# --- Налаштування ---
KEY = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=KEY)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Державні свята України (формат: "MM-DD"); за потреби наповни
HOLIDAYS = []


# --- Допоміжні функції дат ---
def kyiv_now():
    """Поточний час у київському часовому поясі."""
    return datetime.now(timezone('Europe/Kiev'))


def is_holiday_or_weekend(dt: datetime) -> bool:
    """
    Перевіряє, чи дата є вихідним (сб/нд) або святом.
    Очікує TZ-aware datetime у Europe/Kiev.
    """
    return dt.weekday() >= 5 or dt.strftime("%m-%d") in HOLIDAYS


def first_workday_of_month(dt: datetime) -> datetime:
    """
    Повертає datetime 09:10 у перший робочий день місяця для місяця dt.
    Очікує TZ-aware datetime у Europe/Kiev.
    """
    # Починаємо з 1 числа о 09:10
    start = dt.replace(day=1, hour=9, minute=10, second=0, microsecond=0)
    # Якщо 1 число вихідне/свято — переносимо вперед
    while is_holiday_or_weekend(start):
        start = start + timedelta(days=1)
    return start


def is_first_workday_today(now: datetime | None = None) -> bool:
    """
    Чи є сьогодні першим робочим днем місяця (за Києвом)?
    """
    now = now or kyiv_now()
    fwd = first_workday_of_month(now)
    return now.date() == fwd.date()


# --- Формування тексту ---
def get_previous_month_name(now: datetime | None = None) -> str:
    now = now or kyiv_now()
    current_month = now.month
    previous_month = 12 if current_month == 1 else current_month - 1
    months_ua = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
    ]
    return months_ua[previous_month - 1]


def build_reminder_message(now: datetime | None = None) -> str:
    now = now or kyiv_now()
    previous_month_name = get_previous_month_name(now)
    reminder_date = f"07.{now.strftime('%m')}"
    return (
        f"🔔 Нагадування!\n"
        f"Колеги, закриваємо {previous_month_name.upper()} місяць 💪\n"
        f"Прошу усіх в термін до {reminder_date} включно, завершити свої угоди в Експедиторі.\n\n"
        "Продуктивного дня."
    )


# --- Відправка ---
async def send_reminder_to_all_users():
    users = get_active_users()
    message = build_reminder_message()

    sent = 0
    for user in users:
        try:
            await bot.send_message(chat_id=user['telegram_id'], text=message)
            sent += 1
        except Exception as e:
            logging.error(f"Помилка при відправці повідомлення користувачу {user.get('telegram_name', '<?>')}: {e}")

    logging.info(f"Нагадування відправлено {sent} користувачам.")


# --- Щоденна перевірка ---
async def daily_first_workday_check():
    """
    Викликається щодня о 09:10 за Києвом.
    Якщо сьогодні перший робочий день місяця — шлемо нагадування.
    """
    now = kyiv_now()
    if is_first_workday_today(now):
        logging.info("[Reminder] Сьогодні перший робочий день місяця — надсилаємо повідомлення.")
        await send_reminder_to_all_users()
    else:
        logging.info("[Reminder] Сьогодні НЕ перший робочий день місяця — нічого не робимо.")




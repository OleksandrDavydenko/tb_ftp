import logging
import os
from datetime import datetime

import requests
from openai import AsyncOpenAI
from pytz import timezone
from telegram import Bot

from auth import get_power_bi_token
from db import get_active_users, log_birthday_greeting
from utils.name_aliases import display_name

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

bot = Bot(token=TELEGRAM_BOT_TOKEN)
gpt = AsyncOpenAI(api_key=OPENAI_API_KEY)


def get_today_work_anniversaries():
    """
    Повертає список співробітників, у яких сьогодні річниця роботи в компанії.
    Вибираються лише ті, хто працює 1+ рік.
    """
    token = get_power_bi_token()
    if not token:
        return []

    kyiv_now = datetime.now(timezone('Europe/Kiev'))
    today_month = kyiv_now.month
    today_day = kyiv_now.day
    today_year = kyiv_now.year
    logging.info(f"[Anniversary] Перевірка дати (Kyiv): {kyiv_now.strftime('%Y-%m-%d')}")

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    query = f"""
        EVALUATE
        SELECTCOLUMNS(
            FILTER(
                Employees,
                NOT ISBLANK(Employees[hireDate]) &&
                MONTH(Employees[hireDate]) = {today_month} &&
                DAY(Employees[hireDate]) = {today_day} &&
                YEAR(Employees[hireDate]) < {today_year}
            ),
            \"Employee\", Employees[Employee],
            \"hireDate\", Employees[hireDate],
            \"YearsInCompany\", DATEDIFF(Employees[hireDate], DATE({today_year}, {today_month}, {today_day}), YEAR)
        )
    """

    try:
        response = requests.post(
            url,
            headers=headers,
            json={
                "queries": [{"query": query}],
                "serializerSettings": {"includeNulls": True}
            },
            timeout=60,
        )

        if response.status_code != 200:
            logging.error(f"Помилка при запиті Power BI: {response.status_code}, {response.text}")
            return []

        payload = response.json()
        result = payload.get('results', [{}])[0]
        if result.get('error'):
            logging.error(f"Помилка DAX запиту річниць: {result.get('error')}")
            return []

        rows = result.get('tables', [{}])[0].get('rows', [])
        logging.info(f"[Anniversary] Знайдено записів по річницях: {len(rows)}")
        return [
            {
                "Employee": row.get("[Employee]"),
                "hireDate": row.get("[hireDate]"),
                "YearsInCompany": row.get("[YearsInCompany]")
            }
            for row in rows
            if row.get("[Employee]")
        ]
    except Exception as e:
        logging.error(f"Помилка при отриманні річниць роботи: {e}")
        return []


async def generate_ai_work_anniversary_greeting(real_name: str, years: int | None) -> str:
    shown_name = display_name(real_name)
    years_text = f"{years} рік" if years == 1 else f"{years} роки" if years and years < 5 else f"{years} років" if years else "ще один важливий рік"

    prompt = (
        f"Привітай співробітника на ім'я {shown_name} з річницею роботи в компанії ТОВ 'ФТП'. "
        f"Сьогодні виповнюється {years_text} роботи в компанії. "
        "Напиши коротке щире привітання українською мовою (2-4 речення), "
        "звертайся на 'ти', додай мотивації, подяки та теплих емоцій. "
        "Вкажи кількість років у компанії, якщо ця інформація є. "
        "Додай 1-2 емодзі."
    )
    query = f"Привітання {real_name} з річницею роботи. "

    try:
        response = await gpt.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.85,
        )
        message = response.choices[0].message.content.strip()
        log_birthday_greeting(real_name, query, message)
        return f"🤖 {message}"
    except Exception as e:
        logging.error(f"Помилка генерації привітання з річницею для {real_name}: {e}")
        years_fallback = f"{years} р." if years else "черговою річницею"
        return (
            f"🤖 🎉 {shown_name}, вітаю з {years_fallback} у компанії ФТП! "
            "Дякую за твою роботу, відповідальність і внесок у спільний результат!"
        )


async def send_work_anniversary_greetings():
    logging.info("Перевіряємо, чи є сьогодні річниці роботи...")
    anniversary_people = get_today_work_anniversaries()
    if not anniversary_people:
        logging.info("Сьогодні немає річниць роботи.")
        return

    users = get_active_users()
    users_dict = {
        (u.get("employee_name") or "").strip(): u["telegram_id"]
        for u in users
        if u.get("employee_name") and u.get("telegram_id")
    }

    for person in anniversary_people:
        name = (person.get("Employee") or "").strip()
        years = person.get("YearsInCompany")
        telegram_id = users_dict.get(name)

        if telegram_id:
            try:
                message = await generate_ai_work_anniversary_greeting(name, years)
                await bot.send_message(chat_id=telegram_id, text=message)
                logging.info(f"🎉 Відправлено привітання з річницею для {name}")
            except Exception as e:
                logging.error(f"Не вдалося відправити привітання з річницею для {name}: {e}")
        else:
            logging.info(f"👤 {name} не знайдено у базі активних користувачів.")

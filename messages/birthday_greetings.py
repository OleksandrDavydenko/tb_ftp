
import logging
from datetime import datetime
import requests
from telegram import Bot
from auth import get_power_bi_token
from db import get_active_users
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Функція для отримання списку іменинників з Power BI
def get_today_birthdays():
    token = get_power_bi_token()
    if not token:
        return []

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    today = datetime.today().strftime("%m-%d")
    query = f"""
        EVALUATE 
        SELECTCOLUMNS(
            FILTER(
                Employees,
                FORMAT(Employees[birthdayDate], "MM-dd") = "{today}"
            ),
            "Employee", Employees[Employee],
            "birthdayDate", Employees[birthdayDate]
        )
    """

    response = requests.post(url, headers=headers, json={
        "queries": [{"query": query}],
        "serializerSettings": {"includeNulls": True}
    })

    if response.status_code == 200:
        rows = response.json()['results'][0]['tables'][0].get('rows', [])
        return [{"Employee": row.get("[Employee]")} for row in rows]
    else:
        logging.error(f"Помилка при запиті Power BI: {response.status_code}, {response.text}")
        return []

# Функція для генерації побажання
def generate_birthday_greeting(name):
    return (
        f"🎉 З Днем народження, {name}!\n\n"
        "Нехай цей рік буде сповнений нових досягнень, натхнення та позитивних моментів. "
        "Бажаємо міцного здоров’я, любові, радості й фінансового достатку! 🎂🥳"
    )

# Основна функція для перевірки і відправки привітань
async def send_birthday_greetings():
    logging.info("Перевіряємо, чи є сьогодні іменинники...")
    birthday_people = get_today_birthdays()
    if not birthday_people:
        logging.info("Сьогодні немає іменинників.")
        return

    users = get_active_users()
    users_dict = {u["employee_name"]: u["telegram_id"] for u in users}

    for person in birthday_people:
        name = person.get("Employee")
        telegram_id = users_dict.get(name)

        if telegram_id:
            message = generate_birthday_greeting(name)
            try:
                await bot.send_message(chat_id=telegram_id, text=message)
                logging.info(f"Відправлено привітання {name}")
            except Exception as e:
                logging.error(f"Не вдалося відправити повідомлення {name}: {e}")
        else:
            logging.info(f"{name} не знайдено у базі активних користувачів.")


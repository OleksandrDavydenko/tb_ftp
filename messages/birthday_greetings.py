import logging
import os
import requests
from datetime import datetime
from telegram import Bot
from auth import get_power_bi_token
from db import get_active_users
from openai import AsyncOpenAI
from db import log_birthday_greeting

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ініціалізація ботів та API
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

bot = Bot(token=TELEGRAM_BOT_TOKEN)
gpt = AsyncOpenAI(api_key=OPENAI_API_KEY)

# 🎂 Отримати іменинників з Power BI
def get_today_birthdays():
    token = get_power_bi_token()
    if not token:
        return []

    today = datetime.today().strftime("%m-%d")
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

# 🤖 Генерація AI-привітання
async def generate_ai_birthday_greeting(name: str) -> str:
    prompt = (
        f"Привітай українця на ім'я {name} з Днем народження. "
        "Він працює в компанії ТОВ 'ФТП'. Напиши коротке (2–4 речення) привітання українською мовою. "
        "Звертайся до нього на 'ти'. Додай трохи емоцій, побажай добра, радості, мотивації, фінансового достатку та емодзі. "
        "Будь лаконічним, але щирим."
    )
    query = (
        f"Привітання {name} з Днем народження. "
    )

    try:
        response = await gpt.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.85
        )
        message = response.choices[0].message.content.strip()
        # Логування після генерації
        log_birthday_greeting(name, query, message)
        
        return f"🤖 {message}"
    except Exception as e:
        logging.error(f"Помилка генерації привітання для {name}: {e}")
        return f"🤖 🎉 {name}, з Днем народження! Бажаю щастя, здоров'я та натхнення! 🎂"

# 📬 Основна функція розсилки привітань
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
            try:
                message = await generate_ai_birthday_greeting(name)
                await bot.send_message(chat_id=telegram_id, text=message)
                logging.info(f"🎉 Відправлено AI-привітання для {name}")
            except Exception as e:
                logging.error(f"Не вдалося відправити повідомлення {name}: {e}")
        else:
            logging.info(f"👤 {name} не знайдено у базі активних користувачів.")

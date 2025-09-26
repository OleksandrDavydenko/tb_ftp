import os
import logging
import asyncio
from telegram import Bot
from db import get_active_users

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_message_to_users():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_send_message_to_users())

async def async_send_message_to_users():
    """ Відправляє повідомлення всім активним користувачам. """
    users = get_active_users()

    message = (
        "📢 <b>Важливо: оновлення головного меню</b>\n\n"
        "У частини користувачів замість розділу <b>💼 Зарплата</b> ще може відображатися стара кнопка "
        "<b>Розрахунковий лист</b>.\n\n"
        "Щоб оновити головне меню:\n"
        "1️⃣ Перейдіть у будь-який інший розділ (наприклад, <b>📊 Аналітика</b> або <b>🧾 Кадровий облік</b>).\n"
        "2️⃣ Поверніться назад або натисніть <b>Головне меню</b>.\n\n"
        "Після цього кнопка <b>Розрахунковий лист</b> зміниться на <b>💼 Зарплата</b>.\n\n"
    )







    for user in users:
        telegram_id = user.get('telegram_id')
        employee_name = user.get('employee_name')
        if telegram_id:
            try:
                await bot.send_message(chat_id=telegram_id, text=message, parse_mode='HTML')
                logging.info(f"✅ Повідомлення відправлено: {employee_name} (Telegram ID: {telegram_id})")
            except Exception as e:
                logging.error(f"❌ Помилка при відправці повідомлення {employee_name}: {e}")
        else:
            logging.warning(f"⚠️ Відсутній Telegram ID для користувача: {employee_name}")

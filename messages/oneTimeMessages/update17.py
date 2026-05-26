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
        "🔄 <b>Оновлення системи!</b> 📢\n\n"
        "1️⃣ <b>Дебіторка (AR) та Аналітика</b> 📊 тепер відображаються лише для користувачів, у яких є дані. Якщо даних немає — розділи не показуються.\n\n"
        "2️⃣ Оновлено звіти <b>Аналітика по роках та місяцях</b> 📅: тепер ви бачите <b>Розумну місячну картку</b> 🧠 та <b>Дашборд за рік</b> 📈.\n\n"
        "3️⃣ Графіки в <b>дебіторській заборгованості</b> 💰 отримали новий дизайн і показують <b>топ-10 боржників</b> 🔟, а решту групують.\n\n"
        "🚀 Перевірте оновлення вже зараз!"
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

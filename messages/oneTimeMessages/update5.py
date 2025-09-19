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
        "📢 <b>Оновлення!</b>\n\n"
        "Створено новий розділ — <b>💼 Зарплата</b>.\n\n"
        "У цьому розділі доступно:\n"
        "1️⃣ <b>Оклади</b> — ваш оклад + KPI, нарахування та виплати. "
        "Відображається у вигляді <i>розрахункового листа</i>.\n"
        "2️⃣ <b>Відомість бонусів</b> — бот надсилає Excel-файл із детальною інформацією "
        "про <i>нарахування та виплати бонусів</i>.\n\n"
        "ℹ️ <i>Зверніть увагу:</i> наразі у продавців <b>не відображаються суми штрафів та конкурсу</b>. "
        "Це буде додано пізніше.\n\n"
        "Спробуйте новий розділ у меню: <b>💼 Зарплата</b>.\n\n"
        "Якщо ви знайдете помилки або неточності — будь ласка, "
        "пишіть на мою пошту: <b>od@ftp.com</b> 📧\n\n"
        "Дякуємо, що користуєтесь ботом ❤️"
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

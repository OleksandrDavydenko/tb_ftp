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
        "У розділі <b>💼 Зарплата</b> з’явилися нові можливості:\n\n"
        "➕ Додано кнопки:\n"
        "1️⃣ <b>Бонуси</b> — відображаються <i>фактичні нарахування та виплати</i> по місяцях.\n"
        "2️⃣ <b>Премії керівникам</b> — можна переглянути <i>нарахування та виплати премій</i> у помісячному розрізі.\n\n"
        "⚙️ Також внесено зміни в інші розділи:\n"
        "• У меню <b>📊 Дебіторська заборгованість</b> тепер можуть перейти лише користувачі, "
        "в яких реально існує дебіторка.\n"
        "• У повідомленнях про виплату тепер вказується <b>вид виплати</b>: "
        "<i>Оклад / Бонуси / Премія</i>.\n\n"
        "Спробуйте нові можливості вже зараз у меню <b>💼 Зарплата</b>.\n\n"
        "Якщо ви знайдете помилки або неточності — будь ласка, "
        "пишіть на мою пошту: <b>od@ftpua.com</b> 📧\n\n"
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

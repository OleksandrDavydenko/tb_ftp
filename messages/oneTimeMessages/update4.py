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
        "Додано нову кнопку — <b>📘 Довідка</b> у розділі <b>Інформація</b>.\n\n"
        "Це зручна інструкція, яка пояснює:\n"
        "• які розділи доступні у боті<br>"
        "• як працює <b>Штучний Інтелект</b> 🤖<br>"
        "• коли приходять автоматичні нагадування<br>"
        "• як користуватись функціями: <i>зарплата, аналітика, борги, кадровий облік</i>\n\n"
        "Натисніть <b>Інформація → 📘 Довідка</b>, щоб ознайомитись.\n\n"
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

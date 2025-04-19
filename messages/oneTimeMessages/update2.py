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
    """ Відправляє повідомлення тільки Олександру Давиденку. """
    users = get_active_users()
    target_name = "Давиденко Олександр"

    message = (
        "⚠️ <b>Друзі, хочемо внести ясність!</b>\n\n"
        "У п’ятницю ввечері багато з вас отримали серію повідомлень про виплати 💸. "
        "Це сталося через помилку, яка виникла під час розробки нового функціоналу бота.\n\n"
        "Ми вже все виправили 🛠️\n\n"
        "Відтепер у повідомленнях буде вказано <b>період, за який здійснено оплату</b>, "
        "тож інформація стане зрозумілішою і зручнішою.\n\n"
        "Дякуємо за ваше терпіння, підтримку і розуміння 🙏\n"
    )

    for user in users:
        if user.get('employee_name') == target_name:
            telegram_id = user.get('telegram_id')
            if telegram_id:
                try:
                    await bot.send_message(chat_id=telegram_id, text=message, parse_mode='HTML')
                    logging.info(f"✅ Повідомлення надіслано: {target_name} (Telegram ID: {telegram_id})")
                except Exception as e:
                    logging.error(f"❌ Помилка при відправці повідомлення {target_name}: {e}")
            else:
                logging.warning(f"⚠️ Відсутній Telegram ID для користувача: {target_name}")
            break

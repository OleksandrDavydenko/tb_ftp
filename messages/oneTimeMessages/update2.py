import os
import logging
import asyncio
from telegram import Bot
from db import get_active_users

# Ініціалізація бота
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_message_to_users():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_send_message_to_users())

async def async_send_message_to_users():
    """ Відправляє повідомлення тільки Давиденку Олександру. """
    target_name = "Давиденко Олександр"
    users = get_active_users()

    message = (
        "⚠️ Друже, маємо невеличке пояснення щодо повідомлень від бота.\n\n"
        "У п’ятницю ввечері ви могли отримати значну кількість повідомлень про виплати 💸. "
        "Це сталося через технічну помилку, що виникла при впровадженні нового функціоналу.\n\n"
        "😇 Ми вже все виправили та вдосконалили логіку сповіщень. Відтепер у повідомленнях про виплати буде вказано "
        "*період, за який здійснено оплату*, щоб уникнути плутанини.\n\n"
        "Дякуємо за розуміння та терпіння 🙏\n"
    )

    for user in users:
        if user.get('employee_name') == target_name:
            telegram_id = user.get('telegram_id')
            if telegram_id:
                try:
                    await bot.send_message(chat_id=telegram_id, text=message, parse_mode='HTML')
                    logging.info(f"✅ Повідомлення надіслано {target_name} (Telegram ID: {telegram_id})")
                except Exception as e:
                    logging.error(f"❌ Помилка при відправці повідомлення: {e}")
            break

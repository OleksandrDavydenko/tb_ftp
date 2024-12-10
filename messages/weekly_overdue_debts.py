from telegram import Bot
import logging
import os
from db import get_all_users

# Налаштування Telegram Bot Token
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Налаштування логування
logging.basicConfig(filename='debts_log.log', level=logging.INFO, format='%(asctime)s - %(message)s')

async def send_test_message():
    if not TELEGRAM_BOT_TOKEN:
        logging.error("Telegram Bot Token не знайдено!")
        return

    # Отримуємо список користувачів
    users = get_all_users()

    # Фільтруємо тільки "Давиденко Олександр"
    for user in users:
        manager_name = user.get('employee_name')
        telegram_id = user.get('telegram_id')

        if manager_name == "Давиденко Олександр":
            try:
                # Формуємо тестове повідомлення
                message = f"Привіт, {manager_name}! Це тестове повідомлення від бота."
                await bot.send_message(chat_id=telegram_id, text=message)  # Використовуємо await
                logging.info(f"Тестове повідомлення успішно відправлено менеджеру {manager_name} (Telegram ID: {telegram_id})")
            except Exception as e:
                logging.error(f"Не вдалося відправити тестове повідомлення менеджеру {manager_name}. Помилка: {e}")
            return  # Виходимо з циклу після відправлення повідомлення
    logging.warning("Користувача 'Давиденко Олександр' не знайдено у базі даних.")

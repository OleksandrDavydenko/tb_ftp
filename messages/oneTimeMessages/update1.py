import os
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from telegram import Bot
from db import get_db_connection, get_active_users

# Отримання токена бота з змінних середовища
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#def get_telegram_id_by_name(employee_name):
#    """ Отримує Telegram ID користувача за його ім'ям. """
#    conn = get_db_connection()
#    cursor = conn.cursor()
#    cursor.execute("SELECT telegram_id FROM users WHERE employee_name = %s", (employee_name,))
#    user_data = cursor.fetchone()
#    conn.close()
#    return user_data[0] if user_data else None

import asyncio

def send_message_to_users():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_send_message_to_users())

async def async_send_message_to_users():
    """ Відправляє повідомлення користувачам 'Давиденко Олександр' і 'Ступа Олександр'. """
    #employee_names = ["Давиденко Олександр", "Ступа Олександр"]
    users = get_active_users()
    message = """
    🚀 <b>Оновлення Telegram-бота!</b> 🚀

    Якщо Міністерство тільки розмірковує, як впровадити штучний інтелект у Дію, то ми вже це зробили! 🤖💡

    <b>Наш бот отримав нового помічника, який допоможе вам:</b>

    <b>▪</b> Розібратися з обліковою політикою 📚
    <b>▪</b> Дізнатись, коли буде виплата заробітної плати 💸
    <b>▪</b> Отримати підказки по функціоналу бота 🤖
    <b>▪</b> Знайти відповіді на ваші питання швидше та ефективніше ⏳

    🎯 <b>Ви можете взяти участь у покращенні відповідей бота!</b>

    Просто напишіть своє запитання в чат, і бот одразу спробує вам допомогти! Чим більше питань – тим розумнішим стає наш помічник!

    ⚠️ <i>Це тестовий режим, тож можливі невеликі неточності, але ми працюємо над вдосконаленням!</i> 💪"""

    
    for user in users:
        telegram_id = user['telegram_id']
        employee_name = user['employee_name']
        
        if telegram_id:
            try:
                await bot.send_message(chat_id=telegram_id, text=message, parse_mode='HTML')
                logging.info(f"Повідомлення відправлено користувачу {employee_name} (Telegram ID: {telegram_id})")
            except Exception as e:
                logging.error(f"Помилка при відправці повідомлення користувачу {employee_name}: {e}")
        else:
            logging.warning(f"Користувач {employee_name} не знайдений у базі даних.")
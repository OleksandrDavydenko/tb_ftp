import os
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from telegram import Bot
from db import get_db_connection

# Отримання токена бота з змінних середовища
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_telegram_id_by_name(employee_name):
    """ Отримує Telegram ID користувача за його ім'ям. """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT telegram_id FROM users WHERE employee_name = %s", (employee_name,))
    user_data = cursor.fetchone()
    conn.close()
    return user_data[0] if user_data else None

import asyncio

def send_message_to_users():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_send_message_to_users())

async def async_send_message_to_users():
    """ Відправляє повідомлення користувачам 'Давиденко Олександр' і 'Ступа Олександр'. """
    employee_names = ["Давиденко Олександр", "Ступа Олександр"]
    message = ("""
        🚀 <b>Оновлення Telegram-бота!</b> 🚀<br><br>
        Якщо Міністерство тільки розмірковує, як впровадити штучний інтелект у Дію, то ми вже це зробили! 🤖💡<br><br>
        <b>Наш бот отримав нового помічника, який допоможе вам:</b><br>
        - Розібратися з обліковою політикою 📚<br>
        - Дізнатись, коли буде виплата заробітної плати 💸<br>
        - Отримати підказки по функціоналу бота 🤖<br>
        - Знайти відповіді на ваші питання швидше та ефективніше ⏳<br><br>
        🎯 <b>Ви можете взяти участь у покращенні відповідей бота!</b><br>
        Просто ставте питання, які вас цікавлять, і це допоможе зробити помічника ще розумнішим!<br><br>
        ⚠️ <i>Це тестовий режим, тож можливі невеликі неточності, але ми працюємо над вдосконаленням!</i> 💪<br>
    """)

    
    for employee_name in employee_names:
        telegram_id = get_telegram_id_by_name(employee_name)
        
        if telegram_id:
            try:
                await bot.send_message(chat_id=telegram_id, text=message, parse_mode='HTML')
                logging.info(f"Повідомлення відправлено користувачу {employee_name} (Telegram ID: {telegram_id})")
            except Exception as e:
                logging.error(f"Помилка при відправці повідомлення користувачу {employee_name}: {e}")
        else:
            logging.warning(f"Користувач {employee_name} не знайдений у базі даних.")
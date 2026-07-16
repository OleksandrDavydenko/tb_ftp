import os
import logging
import asyncio
from telegram import Bot

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=TELEGRAM_BOT_TOKEN)

TARGET_TELEGRAM_ID = 203148640

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_message_to_users():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_send_message_to_users())

async def async_send_message_to_users():
    """ Відправляє повідомлення лише користувачу з TARGET_TELEGRAM_ID. """
    message = (
        "💰 <b>ВЕЛИКЕ ПОЛЮВАННЯ РОЗПОЧАТО!</b> 📢\n\n"
        "Тепер ваша уважність та ваші ідеї приносять реальні гроші:\n\n"
        "1️⃣ <b>Bug Bounty</b> 🐞 — знайшли баг у роботі бота? "
        "Отримайте <b>500 грн</b> за кожен підтверджений баг! "
        "Баг — це логічна помилка самого бота (зайве меню, кнопка показує не те). "
        "Невірні дані з облікової системи, тимчасові збої сервера та відповіді "
        "ШІ-помічника багами не вважаються.\n\n"
        "2️⃣ <b>Полювання за ідеями</b> 💡 — запропонуйте ідею нової фічі для бота, "
        "і якщо її буде затверджено та прийнято до виконання — "
        "це ще <b>500 грн</b> для вас!\n\n"
        "3️⃣ <b>Як повідомити?</b> ✉️ Напишіть листа на <b>od@ftpua.com</b>, "
        "у темі вкажіть <b>«баг»</b> або <b>«ідея»</b> і опишіть деталі "
        "(скріншот — величезний плюс 📸).\n\n"
        "⚡ <b>Хто перший — того й винагорода:</b> якщо про один і той самий баг "
        "чи ідею напишуть кілька людей, 500 грн отримує той, чий лист надійшов першим.\n\n"
        "📋 Повні умови конкурсів шукайте в розділі <b>ℹ️ Інформація</b> — "
        "кнопки <b>«💡 Нові ідеї»</b> та <b>«🐞 Bug Bounty»</b>.\n\n"
        "🚀 Полюйте уважно — і заробляйте!"
    )

    try:
        await bot.send_message(chat_id=TARGET_TELEGRAM_ID, text=message, parse_mode='HTML')
        logging.info(f"✅ Повідомлення відправлено (Telegram ID: {TARGET_TELEGRAM_ID})")
    except Exception as e:
        logging.error(f"❌ Помилка при відправці повідомлення (Telegram ID: {TARGET_TELEGRAM_ID}): {e}")

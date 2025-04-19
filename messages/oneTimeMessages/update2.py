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

#    message = (
#        "<b>⚠️ Маємо невеличке пояснення щодо повідомлень від бота.</b>\n\n"
#        "У п’ятницю ввечері багато з вас отримали серію повідомлень про виплати 💸. "
#        "Це сталося через помилку, яка виникла під час розробки нового функціоналу бота.\n\n"
#        "Ми вже все виправили 🛠️\n\n"
#        "Відтепер у повідомленнях буде вказано <b>період, за який здійснено оплату</b>, "
#        "тож інформація стане зрозумілішою і зручнішою.\n\n"
#        "Дякуємо за ваше терпіння, підтримку і розуміння 🙏\n"
#    )
    message = (
        "🌸 <b>Христос Воскрес!</b> 🌸\n\n"
        "Вітаємо вас зі <b>світлим святом Великодня</b>! ✨\n"
        "Нехай у вашому серці завжди буде <b>віра</b>, у душі — <b>спокій</b>,\n"
        "у домі — <b>затишок</b>, а поруч — <b>найрідніші люди</b> 💛\n\n"
        "Нехай ця Пасха принесе вам багато <b>радісних моментів</b> 🙏\n\n"
        "🥚🐣 <i>Смачної паски, добрих свят і мирного неба над головою!</i>\n\n"
        "З найщирішими побажаннями, <b>@FreightTransportPartnerBot</b> 🤍"
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

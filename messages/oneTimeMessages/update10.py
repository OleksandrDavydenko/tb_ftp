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
        "🎉 <b>ФТП — 14 років!</b>\n\n"
        "Сьогодні ми відзначаємо особливу дату — <b>14 років успіхів, перемог і розвитку</b>. "
        "Це шлях, який ми пройшли разом, перетворюючи сміливі ідеї на реальні досягнення.\n\n"
        "👥 За цей час ми виросли у велику та згуртовану команду з <b>166 професіоналів</b>. "
        "Кожен із вас щодня робить внесок у стабільність та силу компанії. "
        "Саме завдяки вашій праці, енергії та відповідальності ФТП стала тим, ким є сьогодні.\n\n"
        "Наш фундамент:\n"
        "• <b>Команда</b> — разом ми долаємо виклики, підтримуємо одне одного та рухаємося вперед.\n"
        "• <b>Розвиток</b> — ми не зупиняємось, навчаємось і шукаємо нові можливості.\n"
        "• <b>Клієнти</b> — довіра, яку ми щодня заслужено виборюємо.\n"
        "• <b>Стабільність</b> — впевненість у завтрашньому дні та міцна опора для кожного.\n"
        "• <b>Інновації</b> — сучасні рішення, які роблять нас сильнішими за конкурентів.\n\n"
        "💡 <b>Кожен із вас — частина історії ФТП</b>. Разом ми довели, що навіть найамбітніші цілі стають реальністю, "
        "якщо діяти єдиною командою. Попереду ще більше викликів, але й більше перемог, можливостей і нових горизонтів.\n\n"
        "Нехай робота тут приносить не лише результати, а й натхнення. "
        "Хай кожен день у ФТП додає гордості за те, що ми робимо, і впевненості у майбутньому. "
        "Пам’ятаймо: <b>успіх компанії — це успіх кожного з нас</b>. 🚀\n\n"
        "💙💛 Разом ми створюємо стабільність сьогодні і відкриваємо нові двері для завтра.\n\n"
        "З Днем народження, ФТП! 14 років — і це лише початок великої історії! 🥂✨\n"
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

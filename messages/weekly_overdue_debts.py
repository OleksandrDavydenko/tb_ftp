from telegram import Bot
import logging
import datetime
from db import get_all_users
from auth import get_user_debt_data
import os
import asyncio

# Налаштування Telegram Bot Token
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("Telegram Bot Token не знайдено в змінних середовища!")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Налаштування логування
logging.basicConfig(filename='debts_log.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Поточна дата
current_date = datetime.datetime.now().date()

# Асинхронна перевірка прострочених боргів і відправка повідомлень
async def check_overdue_debts():
    users = get_all_users()

    for user in users:
        manager_name = user.get('employee_name')
        telegram_id = user.get('telegram_id')

        if not manager_name or not telegram_id:
            logging.warning(f"Менеджер або Telegram ID не знайдено у записі: {user}")
            continue

        debts = get_user_debt_data(manager_name)

        if debts:
            overdue_debts = []
            for debt in debts:
                plan_date_pay_str = debt.get('[PlanDatePay]', '')

                if not plan_date_pay_str or plan_date_pay_str == '1899-12-30T00:00:00':
                    continue

                try:
                    plan_date_pay = datetime.datetime.strptime(plan_date_pay_str.split('T')[0], '%Y-%m-%d').date()
                except ValueError:
                    continue

                if plan_date_pay < current_date:
                    overdue_days = (current_date - plan_date_pay).days
                    overdue_debts.append({
                        'Client': debt.get('[Client]', 'Не вказано'),
                        'Account': debt.get('[Account]', 'Не вказано'),
                        'Sum_$': debt.get('[Sum_$]', 'Не вказано'),
                        'OverdueDays': overdue_days
                    })

            if overdue_debts:
                try:
                    # Формування повідомлення
                    message = f"📋 *Звіт про протерміновану дебіторську заборгованість*\n\n*Менеджер*: {manager_name}\n\n"
                    message += "Ваші протерміновані рахунки:\n\n"

                    for overdue in overdue_debts:
                        client = overdue['Client']
                        account = overdue['Account']
                        days = overdue['OverdueDays']
                        sum_usd = overdue['Sum_$']

                        message += (
                            f"▫️ *Клієнт:* {client}\n"
                            f"   *Рахунок:* {account}\n"
                            f"   *Днів протерміновано:* {days}\n"
                            f"   *Сума ($):* {sum_usd}\n\n"
                        )

                    message += "🚨 *Будь ласка, зверніть увагу на ці рахунки.*"

                    # Екранування спеціальних символів для MarkdownV2
                    message = (
                        message.replace('|', '\\|')
                        .replace('-', '\\-')
                        .replace('_', '\\_')
                        .replace('.', '\\.')
                        .replace('(', '\\(')
                        .replace(')', '\\)')
                    )

                    logging.info(f"Формуємо повідомлення для {manager_name}: {message}")

                    # Відправка повідомлення
                    await bot.send_message(chat_id=telegram_id, text=message, parse_mode="MarkdownV2")
                    logging.info(f"Повідомлення відправлено менеджеру {manager_name} (Telegram ID: {telegram_id})")

                except Exception as e:
                    logging.error(f"Не вдалося відправити повідомлення менеджеру {manager_name}. Помилка: {e}")
        else:
            logging.info(f"У менеджера {manager_name} немає протермінованих боргів.")


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

# Функція для форматування дати у ДД.ММ.РР
def format_date(date_str):
    try:
        date = datetime.datetime.strptime(date_str.split('T')[0], '%Y-%m-%d').date()
        return date.strftime('%d.%m.%y')
    except ValueError:
        return 'Не вказано'

# Функція для поділу довгого тексту
def split_message(text, max_length=4096):
    parts = []
    while len(text) > max_length:
        split_point = text.rfind('\n', 0, max_length)
        if split_point == -1:
            split_point = max_length
        parts.append(text[:split_point])
        text = text[split_point:].strip()
    parts.append(text)
    return parts

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
                        'Deal': debt.get('[Deal]', 'Не вказано'),
                        'Account': debt.get('[Account]', 'Не вказано'),
                        'Sum_$': debt.get('[Sum_$]', 'Не вказано'),
                        'PlanDatePay': format_date(plan_date_pay_str),
                        'AccountDate': format_date(debt.get('[AccountDate]', 'Не вказано')),
                        'OverdueDays': overdue_days
                    })

            if overdue_debts:
                try:
                    # Формування повідомлення
                    message = f"📋 *Звіт про протерміновану дебіторську заборгованість*\n\n*Менеджер*: {manager_name}\n\n"
                    message += "Ваші протерміновані рахунки:\n\n"

                    for overdue in overdue_debts:
                        client = overdue['Client']
                        deal = overdue['Deal']
                        account = overdue['Account']
                        days = overdue['OverdueDays']
                        sum_usd = overdue['Sum_$']
                        account_date = overdue['AccountDate']
                        plan_date_pay = overdue['PlanDatePay']

                        message += (
                            f"▫️ *Клієнт:* {client}\n"
                            f"   *Угода:* {deal}\n"
                            f"   *Рахунок:* {account}\n"
                            f"   *Дата рахунку:* {account_date}\n"
                            f"   *Планова дата оплати:* {plan_date_pay}\n"
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

                    # Поділ повідомлення на частини, якщо потрібно
                    messages = split_message(message)
                    for part in messages:
                        await bot.send_message(chat_id=telegram_id, text=part, parse_mode="MarkdownV2")

                    logging.info(f"Повідомлення відправлено менеджеру {manager_name} (Telegram ID: {telegram_id})")

                except Exception as e:
                    logging.error(f"Не вдалося відправити повідомлення менеджеру {manager_name}. Помилка: {e}")
        else:
            logging.info(f"У менеджера {manager_name} немає протермінованих боргів.")









# Функція по натисканюю на кнопку
            

async def send_overdue_debts_by_request(update, context):
    """Формує і повертає протерміновану дебіторську заборгованість для поточного користувача."""
    telegram_id = update.message.chat_id
    user_data = next((u for u in get_all_users() if u['telegram_id'] == telegram_id), None)

    if not user_data:
        return "❗ Вас не знайдено в базі користувачів."

    manager_name = user_data['employee_name']
    debts = get_user_debt_data(manager_name)

    if debts:
        overdue_debts = []
        for debt in debts:
            plan_date_pay_str = debt.get('[PlanDatePay]', '')
            if not plan_date_pay_str or plan_date_pay_str == '1899-12-30T00:00:00':
                continue

            plan_date_pay = datetime.datetime.strptime(plan_date_pay_str.split('T')[0], '%Y-%m-%d').date()
            if plan_date_pay < datetime.datetime.now().date():
                overdue_days = (datetime.datetime.now().date() - plan_date_pay).days
                overdue_debts.append({
                    'Client': debt.get('[Client]', 'Не вказано'),
                    'Deal': debt.get('[Deal]', 'Не вказано'),
                    'Account': debt.get('[Account]', 'Не вказано'),
                    'Sum_$': debt.get('[Sum_$]', 'Не вказано'),
                    'PlanDatePay': format_date(plan_date_pay_str),
                    'AccountDate': format_date(debt.get('[AccountDate]', 'Не вказано')),
                    'OverdueDays': overdue_days
                })

        if overdue_debts:
            message = f"📋 *Протермінована дебіторська заборгованість для {manager_name}:*\n\n"
            for overdue in overdue_debts:
                message += (
                    f"▫️ *Клієнт:* {overdue['Client']}\n"
                    f"   *Угода:* {overdue['Deal']}\n"
                    f"   *Рахунок:* {overdue['Account']}\n"
                    f"   *Дата рахунку:* {overdue['AccountDate']}\n"
                    f"   *Планова дата оплати:* {overdue['PlanDatePay']}\n"
                    f"   *Днів протерміновано:* {overdue['OverdueDays']}\n"
                    f"   *Сума ($):* {overdue['Sum_$']}\n\n"
                )
            return message
        else:
            return "✅ У вас немає протермінованої дебіторської заборгованості."
    else:
        return "ℹ️ Дані для вас відсутні."

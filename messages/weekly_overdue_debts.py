from telegram import Bot
import logging
import datetime
from db import get_all_users
from auth import get_user_debt_data

# Налаштування Telegram Bot Token
TELEGRAM_BOT_TOKEN = "Ваш_Telegram_Bot_Token"
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Налаштування логування
logging.basicConfig(filename='debts_log.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Поточна дата
current_date = datetime.datetime.now().date()

# Перевірка прострочених боргів і відправка повідомлень
def check_overdue_debts():
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

                # Ігноруємо некоректну дату
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
                    message = f"📋 *Звіт про протерміновані рахунки*\n\n*Менеджер*: {manager_name}\n\n"
                    message += "Ваші протерміновані рахунки:\n\n"
                    message += "┌──────────────────────────────────┐\n"
                    message += "│ Клієнт       │ Рахунок   │ Днів   │ Сума ($) │\n"
                    message += "├──────────────┼───────────┼────────┼──────────┤\n"

                    for overdue in overdue_debts:
                        client = overdue['Client'][:12]
                        account = overdue['Account'][:10]
                        days = str(overdue['OverdueDays'])
                        sum_usd = str(overdue['Sum_$'])

                        message += f"│ {client:<12} │ {account:<9} │ {days:<6} │ {sum_usd:<8} │\n"

                    message += "└──────────────────────────────────┘\n"
                    message += "\n*Будь ласка, зверніть увагу на ці рахунки.*"

                    logging.info(f"Формуємо повідомлення для {manager_name}: {message}")

                    # Відправка повідомлення
                    bot.send_message(chat_id=telegram_id, text=message, parse_mode="MarkdownV2")
                    logging.info(f"Повідомлення відправлено менеджеру {manager_name}")

                except Exception as e:
                    logging.error(f"Не вдалося відправити повідомлення менеджеру {manager_name}. Помилка: {e}")
        else:
            logging.info(f"У менеджера {manager_name} немає протермінованих боргів.")

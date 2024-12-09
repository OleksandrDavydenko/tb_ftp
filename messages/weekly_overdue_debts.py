import logging
import datetime
from db import get_all_users
from auth import get_user_debt_data

# Налаштування логування
logging.basicConfig(filename='debts_log.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Отримуємо поточну дату
current_date = datetime.datetime.now().date()

# Функція для перевірки прострочених боргів
def check_overdue_debts():
    # Отримуємо всіх користувачів з бази даних
    users = get_all_users()

    for user in users:
        manager_name = user['employee_name']
        
        # Отримуємо дані про дебіторську заборгованість для менеджера
        debts = get_user_debt_data(manager_name)
        
        if debts:
            # Перевіряємо кожен борг, чи є він простроченим
            for debt in debts:
                plan_date_pay = datetime.datetime.strptime(debt.get('PlanDatePay', ''), '%Y-%m-%d').date()
                
                if plan_date_pay > current_date:
                    # Виводимо інформацію про прострочений борг в лог
                    client = debt.get('Client', 'Не вказано')
                    amount = debt.get('Sum_$', 'Не вказано')
                    logging.info(f"Менеджер: {manager_name}, Клієнт: {client}, Сума: {amount}, Планова дата платежу: {plan_date_pay}")

# Викликаємо функцію для перевірки прострочених боргів


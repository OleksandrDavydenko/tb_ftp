import logging
import datetime
from db import get_all_users
from auth import get_user_debt_data

# Налаштування логування
logging.basicConfig(filename='debts_log.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Поточна дата
current_date = datetime.datetime.now().date()

# Перевірка прострочених боргів
def check_overdue_debts():
    users = get_all_users()

    for user in users:
        manager_name = user.get('employee_name')

        if not manager_name:
            logging.warning(f"Менеджер не знайдений у записі: {user}")
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
                    # Видаляємо час із дати
                    plan_date_pay = datetime.datetime.strptime(plan_date_pay_str.split('T')[0], '%Y-%m-%d').date()
                except ValueError:
                    continue

                # Перевірка на простроченість
                if plan_date_pay < current_date:
                    overdue_debts.append({
                        'Client': debt.get('[Client]', 'Не вказано'),
                        'Sum_$': debt.get('[Sum_$]', 'Не вказано'),
                        'PlanDatePay': plan_date_pay
                    })

            # Логування прострочених сум для кожного менеджера
            if overdue_debts:
                logging.info(f"Менеджер: {manager_name}")
                for overdue in overdue_debts:
                    logging.info(f"  Сума: {overdue['Sum_$']}, Клієнт: {overdue['Client']}, Дата: {overdue['PlanDatePay']}")
        else:
            # Якщо немає боргів, нічого не виводимо
            pass


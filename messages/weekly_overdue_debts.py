import logging
import datetime
from db import get_all_users
from auth import get_user_debt_data

# Налаштування логування
logging.basicConfig(filename='debts_log.log', level=logging.DEBUG, format='%(asctime)s - %(message)s')

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
        logging.info(f"Борги менеджера {manager_name}: {debts}")

        if debts:
            overdue_debts = []
            for debt in debts:
                plan_date_pay_str = debt.get('PlanDatePay', '')

                if not plan_date_pay_str:
                    logging.warning(f"Пропущена або порожня дата платежу для боргу: {debt}")
                    continue

                try:
                    plan_date_pay = datetime.datetime.fromisoformat(plan_date_pay_str.split('T')[0]).date()
                except ValueError:
                    logging.error(f"Некоректна дата платежу: {plan_date_pay_str} для боргу: {debt}")
                    continue

                # Ігноруємо дати до 1900 року
                if plan_date_pay.year < 1900:
                    logging.warning(f"Технічна дата платежу для боргу: {debt}")
                    continue

                # Діагностичне логування
                logging.debug(f"Дата платежу: {plan_date_pay}, Поточна дата: {current_date}, Прострочено: {plan_date_pay < current_date}")

                # Перевірка на простроченість
                if plan_date_pay < current_date:
                    overdue_debts.append({
                        'Client': debt.get('Client', 'Не вказано'),
                        'Sum_$': debt.get('Sum_$', 'Не вказано'),
                        'PlanDatePay': plan_date_pay
                    })

            if overdue_debts:
                logging.info(f"Менеджер: {manager_name} має прострочені борги:")
                for overdue in overdue_debts:
                    logging.info(f"  Клієнт: {overdue['Client']}, Сума: {overdue['Sum_$']}, Планова дата платежу: {overdue['PlanDatePay']}")
            else:
                logging.info(f"У менеджера {manager_name} немає прострочених боргів.")
        else:
            logging.info(f"У менеджера {manager_name} немає боргів.")


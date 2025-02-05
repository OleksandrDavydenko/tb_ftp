import requests
import logging
from auth import get_power_bi_token
from datetime import datetime

# Налаштовуємо logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для отримання нарахувань за рік і місяць
def get_salary_data(employee_name, year, month):
    logging.info(f"Запит на отримання даних для: {employee_name}, рік: {year}, місяць: {month}")
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None
    
    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    # Словник для відображення місяців у числа
    months_mapping = {
        "Січень": 1, "Лютий": 2, "Березень": 3, "Квітень": 4,
        "Травень": 5, "Червень": 6, "Липень": 7, "Серпень": 8,
        "Вересень": 9, "Жовтень": 10, "Листопад": 11, "Грудень": 12
    }
    
    # Перетворення назви місяця на його номер
    month_number = months_mapping.get(month, None)
    if month_number is None:
        logging.error(f"Неправильний місяць: {month}")
        return None

    # Форматування місяця для SQL-запиту (двозначний формат)
    formatted_month = f"{month_number:02d}"

    # Запит до Power BI
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            EmployeeSalary,
                            EmployeeSalary[Employee] = "{employee_name}" && 
                            FORMAT(EmployeeSalary[Date], "YYYY-MM") = "{year}-{formatted_month}"
                        ),
                        "Нараховано Оклад UAH", EmployeeSalary[НарахованоОкладГрн],
                        "Додаткові нарахування UAH", EmployeeSalary[ДодатковіНарахуванняUAH],
                        "Додаткові нарахування USD", EmployeeSalary[ДодатковіНарахуванняUSD],
                        "Нараховано Премії USD", EmployeeSalary[НарахованоПреміїUSD],
                        "Нараховано Премії UAH", EmployeeSalary[НарахованоПреміїUAH],
                        "Нараховано Оклад USD", EmployeeSalary[НарахованоОкладUSD]
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    logging.info(f"Виконуємо запит до Power BI для користувача {employee_name}.")
    response = requests.post(power_bi_url, headers=headers, json=query_data)
    
    if response.status_code == 200:
        logging.info("Запит до Power BI успішний.")
        data = response.json()
        logging.info(f"Відповідь від Power BI: {data}")
        rows = data['results'][0]['tables'][0].get('rows', [])
        logging.info(f"Отримано {len(rows)} рядків.")
        return rows
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None

# Функція для отримання інформації про виплати за рік і місяць
def get_salary_payments(employee_name, year, month):
    logging.info(f"Запит на отримання платежів для: {employee_name}, рік: {year}, місяць: {month}")
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None
    
    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Форматування місяця для SQL-запиту
    months_mapping = {
        "Січень": 1, "Лютий": 2, "Березень": 3, "Квітень": 4,
        "Травень": 5, "Червень": 6, "Липень": 7, "Серпень": 8,
        "Вересень": 9, "Жовтень": 10, "Листопад": 11, "Грудень": 12
    }
    
    month_number = months_mapping.get(month, None)
    if month_number is None:
        logging.error(f"Неправильний місяць: {month}")
        return None

    formatted_month = f"{month_number:02d}"

    # Запит для отримання платежів
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            SalaryPayment,
                            SalaryPayment[Employee] = "{employee_name}" &&
                            FORMAT(DATEVALUE(SalaryPayment[МісяцьНарахування]), "YYYY-MM") = "{year}-{formatted_month}"
                        ),
                        "Дата платежу", SalaryPayment[DocDate],
                        "Документ", SalaryPayment[DocNumber],
                        "Сума UAH", SalaryPayment[SUM_UAH],
                        "Сума USD", SalaryPayment[SUM_USD],
                        "Разом в USD", SalaryPayment[SUMINUSD],
                        "PaymentType", SalaryPayment[payment_type],
                        "Character", SalaryPayment[character]
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    logging.info(f"Виконуємо запит до Power BI для платежів користувача {employee_name}.")
    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        logging.info("Запит до Power BI для платежів успішний.")
        data = response.json()
        logging.info(f"Відповідь на запит платежів від Power BI: {data}")
        rows = data['results'][0]['tables'][0].get('rows', [])
        logging.info(f"Отримано {len(rows)} платежів.")
        return rows
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None


def get_bonuses(employee_name, year, month):
    """
    Функція для отримання бонусів з таблиці BonusesTable.
    """
    logging.info(f"Запит на отримання бонусів для: {employee_name}, рік: {year}, місяць: {month}")
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Форматування місяця для SQL-запиту
    months_mapping = {
        "Січень": 1, "Лютий": 2, "Березень": 3, "Квітень": 4,
        "Травень": 5, "Червень": 6, "Липень": 7, "Серпень": 8,
        "Вересень": 9, "Жовтень": 10, "Листопад": 11, "Грудень": 12
    }

    month_number = months_mapping.get(month, None)
    if month_number is None:
        logging.error(f"Неправильний місяць: {month}")
        return None

    formatted_month = f"{month_number:02d}"

    # Запит до Power BI
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            BonusesTable,
                            BonusesTable[Employee] = "{employee_name}" &&
                            FORMAT(BonusesTable[Date], "YYYY-MM") = "{year}-{formatted_month}"
                        ),
                        "ManagerRole", BonusesTable[ManagerRole],
                        "TotalAccrued", BonusesTable[TotalAccrued]
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        logging.info("Запит на бонуси успішний.")
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        logging.info(f"Отримано бонусів: {len(rows)}. Дані: {rows}")
        return rows
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None




def format_salary_table(rows, employee_name, year, month, payments, bonuses):

    """
    Форматування таблиці зарплати з урахуванням бонусів.
    """

    table = "-" * 41 + "\n"

    # Зарплатна частина
    if rows:
        table += f"{'Нарахування':<18}{'UAH':<8}  {'USD':<8}\n"
        table += "-" * 41 + "\n"

        total_uah = 0
        total_usd = 0

        for row in rows:
            оклад_uah = float(row.get("[Нараховано Оклад UAH]", 0))
            оклад_usd = float(row.get("[Нараховано Оклад USD]", 0)) if оклад_uah == 0 else 0.0
            премії_uah = float(row.get("[Нараховано Премії UAH]", 0))
            премії_usd = float(row.get("[Нараховано Премії USD]", 0))
            додат_uah = float(row.get("[Додаткові нарахування UAH]", 0))
            додат_usd = float(row.get("[Додаткові нарахування USD]", 0))

            total_uah += оклад_uah + премії_uah + додат_uah
            total_usd += оклад_usd + додат_usd  + премії_usd

            table += f"{'Оклад':<18}{оклад_uah:<8.2f}  {оклад_usd:<8.2f}\n"
            table += f"{'Премії':<18}{премії_uah:<8.2f}  {премії_usd:<8.2f}\n"
            table += f"{'Додаткові':<18}{додат_uah:<8.2f}  {додат_usd:<8.2f}\n"

        table += "-" * 41 + "\n"
        table += f"{'Всього нараховано: ':<18}{total_uah:<8.2f}  {total_usd:<8.2f}\n"
    else:
        table += "Немає даних про нарахування.\n"

    # Ініціалізація змінних
        
    total_payment_uah = 0
    total_payment_usd = 0

# Виплати
    if payments:
        table += "\nВиплата ЗП:\n"
        table += f"{'Дата':<10}{'Документ':<10} {'UAH':<8}  {'USD':<8}\n"
        table += "-" * 41 + "\n"

        total_payment_uah = 0
        total_payment_usd = 0

        # Обробка та сортування платежів за датою
        formatted_payments = []
        for payment in payments:
            if payment.get("[PaymentType]", "").strip().lower() != "zp":
                continue  # Пропускаємо платежі, які не є зарплатою

            дата_платежу = payment.get("[Дата платежу]", "")
            try:
                дата = datetime.strptime(дата_платежу, "%Y-%m-%d")
                formatted_date = дата.strftime("%d.%m.%y")  # Формат дати
            except ValueError:
                continue  # Пропускаємо некоректні дати

            doc_number = payment.get("[Документ]", "")
            character = payment.get("[Character]", "").strip().lower()

            if character == 'prize':
                сума_uah = 0
                сума_usd = float(payment.get("[Разом в USD]", 0))
            else:
                сума_uah = float(payment.get("[Сума UAH]", 0))
                сума_usd = float(payment.get("[Сума USD]", 0))

            total_payment_uah += сума_uah
            total_payment_usd += сума_usd

            formatted_payments.append((дата, formatted_date, doc_number, сума_uah, сума_usd))

        # **Сортування за датою (від найдавнішої до найновішої)**
        formatted_payments.sort(key=lambda x: x[0])

        # **Оновлюємо таблицю лише один раз**
        for _, formatted_date, doc_number, сума_uah, сума_usd in formatted_payments:
            table += f"{formatted_date:<10}{doc_number:<10} {сума_uah:<8.2f}  {сума_usd:<8.2f}\n"

        table += "-" * 41 + "\n"
        table += f"{'Всього виплачено:':<18}{total_payment_uah:<8.2f}  {total_payment_usd:<8.2f}\n\n"

    # **Розрахунок залишку лише один раз**
    remaining_uah = total_uah - total_payment_uah
    remaining_usd = total_usd - total_payment_usd
    table += f"{'Невиплачений залишок: ':<18}{remaining_uah:<8.2f}  {remaining_usd:<8.2f}\n\n\n"

    # Бонуси
    if bonuses:
            table += "\nБонуси:\n"
            table += "-" * 41 + "\n"
            table += f"{'Нарахування Бонусів':<26}{'USD':<8}\n"
            table += "-" * 41 + "\n"

            total_bonuses = 0
            bonuses_summary = {
                "Сейлс": 0,
                "Оперативний менеджер": 0,
                "Відсоток ОМ": 0
            }

            # Очищення ключів бонусів
            cleaned_bonuses = [{key.strip("[]"): value for key, value in bonus.items()} for bonus in bonuses]

            for bonus in cleaned_bonuses:
                role = bonus.get("ManagerRole", "")
                amount = float(bonus.get("TotalAccrued", 0))

                if role == "Сейлс":
                    bonuses_summary["Сейлс"] += amount
                elif role == "Оперативний менеджер":
                    bonuses_summary["Оперативний менеджер"] += amount
                elif role == "Відсоток ОМ":
                    bonuses_summary["Відсоток ОМ"] += amount

                total_bonuses += amount

            table += f"{'Бонуси Сейлс':<26}{bonuses_summary['Сейлс']:<8.2f}\n"
            table += f"{'Бонуси ОМ':<26}{bonuses_summary['Оперативний менеджер']:<8.2f}\n"
            table += f"{'Відсоток ОМ':<26}{bonuses_summary['Відсоток ОМ']:<8.2f}\n"

            table += "-" * 41 + "\n"
            table += f"{'Всього нараховано бонусів: ':<26}{total_bonuses:<8.2f}\n"
    

    # **Виплата бонусів**
    bonus_payments = [p for p in payments if p.get("[PaymentType]", "").strip().lower() == "bonus"]

    if bonus_payments:
        table += "\nВиплата бонусів:\n"
        table += f"{'Дата':<10}{'Документ':<10} {'USD':<8}\n"
        table += "-" * 41 + "\n"

        total_bonus_usd = 0
        formatted_bonus_payments = []

        for payment in bonus_payments:
            дата_платежу = payment.get("[Дата платежу]", "")
            try:
                дата = datetime.strptime(дата_платежу, "%Y-%m-%d")
                formatted_date = дата.strftime("%d.%m.%y")
            except ValueError:
                continue

            doc_number = payment.get("[Документ]", "")
            сума_usd = float(payment.get("[Разом в USD]", 0))

            total_bonus_usd += сума_usd
            formatted_bonus_payments.append((дата, formatted_date, doc_number, сума_usd))

        # **Сортування виплат бонусів**
        formatted_bonus_payments.sort(key=lambda x: x[0])

        # **Додаємо в таблицю виплати бонусів**
        for _, formatted_date, doc_number, сума_usd in formatted_bonus_payments:
            table += f"{formatted_date:<10}{doc_number:<10} {сума_usd:<8.2f}\n"

        table += "-" * 41 + "\n"
        table += f"{'Всього виплачено бонусів: ':<18}{total_bonus_usd:<8.2f}\n"




    logging.info("Формування таблиці завершено.")
    return table



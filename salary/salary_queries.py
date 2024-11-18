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
                        "Разом в USD", SalaryPayment[SUMINUSD]
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

""" def format_salary_table(rows, employee_name, year, month, payments):
    # Заголовок таблиці з іменем користувача, місяцем і роком
    table = f"Розрахунковий лист: \n{employee_name} за {month} {year}:\n"
    table += "-" * 45 + "\n"

    # Перевірка наявності нарахувань
    if rows:
        table += f"{'Нарахування':<30}{'UAH':<10}{'USD':<10}\n"
        table += "-" * 45 + "\n"

        total_uah = 0
        total_usd = 0

        # Обробка кожного рядка і додавання даних у таблицю
        for row in rows:
            оклад_uah = float(row.get("[Нараховано Оклад UAH]", 0))
            оклад_usd = float(row.get("[Нараховано Оклад USD]", 0)) if оклад_uah == 0 else 0.0  # Використовуємо USD, якщо UAH дорівнює 0
            премії_uah = float(row.get("[Нараховано Премії UAH]", 0))  # Премії в UAH
            премії_usd = float(row.get("[Нараховано Премії USD]", 0))
            додат_uah = float(row.get("[Додаткові нарахування UAH]", 0))
            додат_usd = float(row.get("[Додаткові нарахування USD]", 0))

            total_uah += оклад_uah + премії_uah + додат_uah
            total_usd += оклад_usd + додат_usd + премії_usd

            # Додаємо рядки до таблиці
            table += f"{'Нараховано Оклад':<30}{оклад_uah:<10}{оклад_usd:<10}\n"
            table += f"{'Нараховано Премії':<30}{премії_uah:<10}{премії_usd:<10}\n"
            table += f"{'Додаткові нарахування':<30}{додат_uah:<10}{додат_usd:<10}\n"

        # Підсумки таблиці
        table += "-" * 45 + "\n"
        table += f"{'Всього нараховано:':<30}{total_uah:<10}{total_usd:<10}\n"
    else:
        table += "Немає даних про нарахування для цього періоду.\n"

    # Додаємо секцію виплат, якщо є дані про виплати
    if payments:
        table += "\nВиплата ЗП:\n"
        table += f"{'Дата':<15}{'Документ':<15}{'UAH':<10}{'USD':<10}\n"
        table += "-" * 45 + "\n"

        total_payment_uah = 0
        total_payment_usd = 0

        for payment in payments:
            дата = payment.get("[Дата платежу]", "")
            doc_number = payment.get("[Документ]", "")
            сума_uah = float(payment.get("[Сума UAH]", 0))
            сума_usd = float(payment.get("[Сума USD]", 0))

            total_payment_uah += сума_uah
            total_payment_usd += сума_usd

            table += f"{дата:<15}{doc_number:<15}{сума_uah:<10}{сума_usd:<10}\n"

        table += "-" * 45 + "\n"
        table += f"{'Всього виплачено ЗП:':<30}{total_payment_uah:<10}{total_payment_usd:<10}\n"
    else:
        table += "Немає даних про виплати для цього періоду.\n"

    logging.info("Формування таблиці завершено.")
    return table """



def format_salary_table(rows, employee_name, year, month, payments):
    # Заголовок таблиці з іменем користувача, місяцем і роком
    table = f"Розрахунковий лист: \n{employee_name} за {month} {year}:\n"
    table += "-" * 41 + "\n"  # Довжина рисок налаштована

    # Перевірка наявності нарахувань
    if rows:
        table += f"{'Нарахування':<18}{'UAH':<8}  {'USD':<8}\n"  # Вирівнювання UAH та USD під нижньою таблицею з додатковим пробілом
        table += "-" * 41 + "\n"

        total_uah = 0
        total_usd = 0

        # Обробка кожного рядка і додавання даних у таблицю
        for row in rows:
            оклад_uah = float(row.get("[Нараховано Оклад UAH]", 0))
            оклад_usd = float(row.get("[Нараховано Оклад USD]", 0)) if оклад_uah == 0 else 0.0
            премії_uah = float(row.get("[Нараховано Премії UAH]", 0))
            премії_usd = float(row.get("[Нараховано Премії USD]", 0))
            додат_uah = float(row.get("[Додаткові нарахування UAH]", 0))
            додат_usd = float(row.get("[Додаткові нарахування USD]", 0))

            total_uah += оклад_uah + премії_uah + додат_uah
            total_usd += оклад_usd + додат_usd + премії_usd

            # Додаємо рядки до таблиці з відступами
            table += f"{'Оклад':<18}{оклад_uah:<8.2f}  {оклад_usd:<8.2f}\n"
            table += f"{'Премії':<18}{премії_uah:<8.2f}  {премії_usd:<8.2f}\n"
            table += f"{'Додаткові':<18}{додат_uah:<8.2f}  {додат_usd:<8.2f}\n"

        # Підсумки таблиці
        table += "-" * 41 + "\n"
        table += f"{'Всього нараховано: ':<18}{total_uah:<8.2f}  {total_usd:<8.2f}\n"
    else:
        table += "Немає даних про нарахування.\n"

    # Додаємо секцію виплат, якщо є дані про виплати
    if payments:
        table += "\nВиплата ЗП:\n"
        table += f"{'Дата':<10}{'Документ':<10} {'UAH':<8}  {'USD':<8}\n"  # Зменшено відстань між документом та UAH, збільшено відстань між UAH і USD
        table += "-" * 41 + "\n"

        total_payment_uah = 0
        total_payment_usd = 0

        for payment in payments:
            дата = payment.get("[Дата платежу]", "")
            дата = datetime.strptime(дата, "%Y-%m-%d").strftime("%d.%m.%y")  # Форматування дати
            doc_number = payment.get("[Документ]", "")
            сума_uah = float(payment.get("[Сума UAH]", 0))
            сума_usd = float(payment.get("[Сума USD]", 0))

            total_payment_uah += сума_uah
            total_payment_usd += сума_usd

            # Додаємо пробіл між номером документа і сумою UAH, відстань між UAH та USD на 1 пробіл більше
            table += f"{дата:<10}{doc_number:<10} {сума_uah:<8.2f}  {сума_usd:<8.2f}\n"

        table += "-" * 41 + "\n"
        table += f"{'Всього виплачено:':<18}{total_payment_uah:<8.2f}  {total_payment_usd:<8.2f}\n"
    else:
        table += "Немає даних про виплати.\n"

    # Розрахунок невиплаченого залишку
    remaining_uah = total_uah - total_payment_uah
    remaining_usd = total_usd - total_payment_usd
    table += f"{'Невиплачений залишок: ':<18}{remaining_uah:<8.2f}  {remaining_usd:<8.2f}\n"

    logging.info("Формування таблиці завершено.")
    return table



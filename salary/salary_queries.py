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
                        "Character", SalaryPayment[character],
                        "МісяцьНарахування", SalaryPayment[МісяцьНарахування]
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
    from datetime import datetime

    total_uah = 0.0
    total_usd = 0.0
    total_payment_uah = 0.0
    total_payment_usd = 0.0

    table = "-" * 41 + "\n"

    # ===== НАРАХУВАННЯ =====
    if rows:
        table += f"{'Нарахування':<18}{'UAH':<8}  {'USD':<8}\n"
        table += "-" * 41 + "\n"
        for row in rows:
            оклад_uah = float(row.get("[Нараховано Оклад UAH]", 0))
            оклад_usd = float(row.get("[Нараховано Оклад USD]", 0)) if оклад_uah == 0 else 0.0
            премії_uah = float(row.get("[Нараховано Премії UAH]", 0))
            премії_usd = float(row.get("[Нараховано Премії USD]", 0))
            додат_uah = float(row.get("[Додаткові нарахування UAH]", 0))
            додат_usd = float(row.get("[Додаткові нарахування USD]", 0))

            total_uah += оклад_uah + премії_uah + додат_uah
            total_usd += оклад_usd + премії_usd + додат_usd

            table += f"{'Оклад':<18}{оклад_uah:<8.2f}  {оклад_usd:<8.2f}\n"
            table += f"{'Премії':<18}{премії_uah:<8.2f}  {премії_usd:<8.2f}\n"
            table += f"{'Додаткові':<18}{додат_uah:<8.2f}  {додат_usd:<8.2f}\n"

        table += "-" * 41 + "\n"
        table += f"{'Всього нараховано: ':<18}{total_uah:<8.2f}  {total_usd:<8.2f}\n"
    else:
        table += "Немає даних про нарахування.\n"

    # ===== ВИПЛАТА ЗП (salary + prize) =====
    salary_payments = [
        p for p in payments
        if p.get("[Character]", "").strip().lower() in ["salary", "prize"]
    ]

    if salary_payments:
        table += "\nВиплата ЗП:\n"
        table += f"{'Дата':<10}{'Документ':<10} {'UAH':<8}  {'USD':<8}\n"
        table += "-" * 41 + "\n"

        formatted = []
        for payment in salary_payments:
            дата_платежу = payment.get("[Дата платежу]", "")
            try:
                дата = datetime.strptime(дата_платежу, "%Y-%m-%d")
                formatted_date = дата.strftime("%d.%m.%y")
            except ValueError:
                continue

            doc_number = payment.get("[Документ]", "")
            сума_uah = float(payment.get("[Сума UAH]", 0))
            сума_usd = float(payment.get("[Сума USD]", 0))

            total_payment_uah += сума_uah
            total_payment_usd += сума_usd
            formatted.append((дата, formatted_date, doc_number, сума_uah, сума_usd))

        formatted.sort(key=lambda x: x[0])
        for _, formatted_date, doc_number, сума_uah, сума_usd in formatted:
            table += f"{formatted_date:<10}{doc_number:<10} {сума_uah:<8.2f}  {сума_usd:<8.2f}\n"

        table += "-" * 41 + "\n"
        table += f"{'Всього виплачено:':<18}{total_payment_uah:<8.2f}  {total_payment_usd:<8.2f}\n\n"

    # ===== НЕВИПЛАЧЕНИЙ ЗАЛИШОК =====
    remaining_uah = total_uah - total_payment_uah
    remaining_usd = total_usd - total_payment_usd
    table += f"{'Невиплачений залишок: ':<18}{remaining_uah:<8.2f}  {remaining_usd:<8.2f}\n"

    # ===== НАРАХОВАНІ БОНУСИ =====
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

        cleaned_bonuses = [{k.strip("[]"): v for k, v in bonus.items()} for bonus in bonuses]

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

 # ===== ВИПЛАТА БОНУСІВ (з деталізацією по періодах) =====
    bonus_payments = [
        p for p in payments
        if p.get("[Character]", "").strip().lower() == "bonus"
    ]

    if bonus_payments:
        table += "\nВиплата бонусів:\n"
        table += "-" * 41 + "\n"
        table += f"{'Документ':<15}{'USD':<8}\n"
        table += "-" * 41 + "\n"

        from collections import defaultdict
        grouped = defaultdict(list)
        total_bonus_usd = 0.0

        # Групуємо по даті платежу та номеру документа
        for p in bonus_payments:
            doc_number = p.get("[Документ]", "Невідомо")
            дата_платежу = p.get("[Дата платежу]", "1970-01-01")
            try:
                дата = datetime.strptime(дата_платежу, "%Y-%m-%d")
            except ValueError:
                continue

            key = (дата, doc_number)
            сума_usd = float(p.get("[Разом в USD]", p.get("[Сума USD]", 0)))
            період = p.get("[МісяцьНарахування]", "Невідомо")
            grouped[key].append((сума_usd, період))
            total_bonus_usd += сума_usd

        # Сортуємо по даті
        for (дата, doc_number) in sorted(grouped.keys()):
            рядки = grouped[(дата, doc_number)]
            сума_по_документу = sum(r[0] for r in рядки)

            table += f"{doc_number:<15}{сума_по_документу:<8.2f}\n"
            for сума, період in рядки:
                table += f"{'':<3}→ {сума:<7.2f} — {період}\n"

        table += "-" * 41 + "\n"
        table += f"{'Всього виплачено бонусів: ':<26}{total_bonus_usd:<8.2f}\n"
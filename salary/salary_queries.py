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
    


def get_bonus_payments(employee_name, year, month):
    """
    Функція для отримання виплат бонусів за датою платежу.
    """
    logging.info(f"Запит на отримання виплат бонусів для: {employee_name}, рік: {year}, місяць: {month}")
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

    # Запит для отримання виплат бонусів за датою платежу
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            SalaryPayment,
                            SalaryPayment[Employee] = "{employee_name}" &&
                            SalaryPayment[character] = "bonus" &&
                            YEAR(SalaryPayment[AccrualDateFromDoc]) = {year} &&
                            MONTH(SalaryPayment[AccrualDateFromDoc]) = {int(formatted_month)}
                        ),
                        "Дата платежу", SalaryPayment[DocDate],
                        "Документ", SalaryPayment[DocNumber],
                        "Сума USD", SalaryPayment[SUM_USD],
                        "Разом в USD", SalaryPayment[SUMINUSD],
                        "МісяцьНарахування", SalaryPayment[МісяцьНарахування]
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
        logging.info("Запит на виплати бонусів успішний.")
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        logging.info(f"Отримано виплат бонусів: {len(rows)}. Дані: {rows}")
        return rows
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None




def format_salary_table(rows, employee_name, year, month, payments, bonuses, bonus_payments):
    from datetime import datetime
    from collections import defaultdict

    #main_table = f"Розрахунковий лист:\n{employee_name} за {month} {year}:\n"
    main_table = "-" * 41 + "\n"

    total_uah, total_usd, total_payment_uah, total_payment_usd = 0.0, 0.0, 0.0, 0.0

    # Відображення нарахувань
    if rows:
        main_table += f"{'Нарахування':<18}{'UAH':<8}  {'USD':<8}\n"
        main_table += "-" * 41 + "\n"
        for row in rows:
            оклад_uah = float(row.get("[Нараховано Оклад UAH]", 0))
            оклад_usd = float(row.get("[Нараховано Оклад USD]", 0)) if оклад_uah == 0 else 0.0
            премії_uah = float(row.get("[Нараховано Премії UAH]", 0))
            премії_usd = float(row.get("[Нараховано Премії USD]", 0))
            додат_uah = float(row.get("[Додаткові нарахування UAH]", 0))
            додат_usd = float(row.get("[Додаткові нарахування USD]", 0))

            total_uah += оклад_uah + премії_uah + додат_uah
            total_usd += оклад_usd + премії_usd + додат_usd

            main_table += f"{'Оклад':<18}{оклад_uah:<8.2f}  {оклад_usd:<8.2f}\n"
            main_table += f"{'Премії':<18}{премії_uah:<8.2f}  {премії_usd:<8.2f}\n"
            main_table += f"{'Додаткові':<18}{додат_uah:<8.2f}  {додат_usd:<8.2f}\n"

        main_table += "-" * 41 + "\n"
        main_table += f"{'Всього нараховано: ':<18}{total_uah:<8.2f}  {total_usd:<8.2f}\n"

    # Відображення виплат навіть за відсутності нарахувань
    salary_payments = [p for p in payments if p.get("[Character]", "").strip().lower() in ["salary", "prize"]]

    if salary_payments:
        main_table += "\nВиплата ЗП:\n"
        main_table += f"{'Дата':<10}{'Документ':<10} {'UAH':<8}  {'USD':<8}\n"
        main_table += "-" * 41 + "\n"
        for payment in sorted(salary_payments, key=lambda x: x["[Дата платежу]"]):
            дата = datetime.strptime(payment["[Дата платежу]"], "%Y-%m-%d").strftime("%d.%m.%y")
            doc_number = payment["[Документ]"]
            сума_uah = float(payment["[Сума UAH]"])
            сума_usd = float(payment["[Сума USD]"])

            total_payment_uah += сума_uah
            total_payment_usd += сума_usd

            main_table += f"{дата:<10}{doc_number:<10} {сума_uah:<8.2f}  {сума_usd:<8.2f}\n"

        main_table += "-" * 41 + "\n"
        main_table += f"{'Всього виплачено:':<18}{total_payment_uah:<8.2f}  {total_payment_usd:<8.2f}\n\n"

    # Розрахунок залишку
    remaining_uah = total_uah - total_payment_uah
    remaining_usd = total_usd - total_payment_usd
    main_table += f"{'Невиплачений залишок: ':<18}{remaining_uah:<8.2f}  {remaining_usd:<8.2f}\n"

    # Відображення бонусів
    bonus_table = "-" * 41 + "\n"

    # --- Нарахування ---
    if bonuses:
        bonus_table += f"{'Нарахування Бонусів':<26}{'USD':<8}\n"
        bonus_table += "-" * 41 + "\n"

        total_bonuses = 0
        bonuses_summary = {
            "Сейлс": 0,
            "Оперативний менеджер": 0,
            "Відсоток ОМ": 0
        }

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

        bonus_table += f"{'Бонуси Сейлс':<26}{bonuses_summary['Сейлс']:<8.2f}\n"
        bonus_table += f"{'Бонуси ОМ':<26}{bonuses_summary['Оперативний менеджер']:<8.2f}\n"
        bonus_table += f"{'Відсоток ОМ':<26}{bonuses_summary['Відсоток ОМ']:<8.2f}\n"
        bonus_table += "-" * 41 + "\n"
        bonus_table += f"{'Всього нараховано бонусів: ':<26}{total_bonuses:<8.2f}\n\n"
    else:
        bonus_table += "Нарахування бонусів відсутні.\n\n"

# --- Виплати ---
    if bonus_payments:

        bonus_table += "\nВиплата бонусів\n"
        bonus_table += "-" * 41 + "\n"
        from collections import defaultdict

        # Групуємо виплати за документом
        grouped = defaultdict(list)
        for payment in bonus_payments:
            doc_number = payment["[Документ]"]
            doc_date = payment["[Дата платежу]"]
            grouped[doc_number].append(payment)

        total_bonus_paid = 0

        for doc_number, items in grouped.items():
            total_by_doc = sum(float(p["[Разом в USD]"]) for p in items)
            total_bonus_paid += total_by_doc
            bonus_table += f"Документ: {doc_number} від {doc_date} Загальна сума: {total_by_doc:.2f} USD\n"

            for item in items:
                місяць = datetime.strptime(item["[МісяцьНарахування]"], "%Y-%m-%d").strftime("%B %Y")
                сума = float(item["[Разом в USD]"])
                bonus_table += f"   → {місяць} — {сума:.2f} USD\n"

            bonus_table += "\n"

        bonus_table += f"Всього виплачено бонусів: {total_bonus_paid:.2f} USD\n"
    else:
        bonus_table += "Виплати бонусів відсутні.\n"


    return main_table, bonus_table.strip()
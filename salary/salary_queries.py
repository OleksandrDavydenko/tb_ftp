import requests
import logging
from auth import get_power_bi_token
from datetime import datetime


from utils.name_aliases import display_name 

# Налаштовуємо logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ──────────────────────────────────────────────────────────────────────────────
# КОРОТКІ ЗАПИТИ ДЛЯ ПОБУДОВИ СПИСКІВ РОКІВ/МІСЯЦІВ
# ──────────────────────────────────────────────────────────────────────────────

MONTHS_UA = ["Січень","Лютий","Березень","Квітень","Травень","Червень",
             "Липень","Серпень","Вересень","Жовтень","Листопад","Грудень"]

def _pbi_exec(query: str):
    token = get_power_bi_token()
    if not token:
        logging.error("PBI token missing")
        return []
    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    body = {"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}}
    r = requests.post(url, headers=headers, json=body, timeout=60)
    if r.status_code != 200:
        logging.error(f"PBI {r.status_code}: {r.text}")
        return []
    return r.json()['results'][0]['tables'][0].get('rows', [])

# --- ОКЛАД / ЗП (employee salary + salary payments(character='salary'))
def get_available_years_salary(employee_name: str) -> list[str]:
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(FILTER(EmployeeSalary, EmployeeSalary[Employee] = "{employee_name}"),
                "Y", YEAR(EmployeeSalary[Date])),
  SELECTCOLUMNS(
    FILTER(SalaryPayment, SalaryPayment[Employee] = "{employee_name}" && LOWER(SalaryPayment[character]) = "salary"),
    "Y", YEAR(DATEVALUE(SalaryPayment[МісяцьНарахування]))
  )
)
"""
    rows = _pbi_exec(dax)
    years = sorted({int(r.get("[Y]", 0)) for r in rows if r.get("[Y]")})
    return [str(y) for y in years]

def get_available_months_salary(employee_name: str, year: str) -> list[str]:
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(
    FILTER(EmployeeSalary,
      EmployeeSalary[Employee] = "{employee_name}" && YEAR(EmployeeSalary[Date]) = {year}),
    "M", MONTH(EmployeeSalary[Date])
  ),
  SELECTCOLUMNS(
    FILTER(SalaryPayment,
      SalaryPayment[Employee] = "{employee_name}" &&
      LOWER(SalaryPayment[character]) = "salary" &&
      YEAR(DATEVALUE(SalaryPayment[МісяцьНарахування])) = {year}),
    "M", MONTH(DATEVALUE(SalaryPayment[МісяцьНарахування]))
  )
)
"""
    rows = _pbi_exec(dax)
    mm = sorted({int(r.get("[M]", 0)) for r in rows if r.get("[M]") and 1 <= int(r.get("[M]", 0)) <= 12})
    return [MONTHS_UA[i-1] for i in mm]

# --- БОНУСИ (BonusesTable + salary payments(character='bonus'))
def get_available_years_bonuses(employee_name: str) -> list[str]:
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(FILTER(BonusesTable, BonusesTable[Employee] = "{employee_name}"),
                "Y", YEAR(BonusesTable[Date])),
  SELECTCOLUMNS(
    FILTER(SalaryPayment, SalaryPayment[Employee] = "{employee_name}" && LOWER(SalaryPayment[character]) = "bonus"),
    "Y", YEAR(SalaryPayment[AccrualDateFromDoc])
  )
)
"""
    rows = _pbi_exec(dax)
    years = sorted({int(r.get("[Y]", 0)) for r in rows if r.get("[Y]")})
    return [str(y) for y in years]

def get_available_months_bonuses(employee_name: str, year: str) -> list[str]:
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(
    FILTER(BonusesTable,
      BonusesTable[Employee] = "{employee_name}" && YEAR(BonusesTable[Date]) = {year}),
    "M", MONTH(BonusesTable[Date])
  ),
  SELECTCOLUMNS(
    FILTER(SalaryPayment,
      SalaryPayment[Employee] = "{employee_name}" &&
      LOWER(SalaryPayment[character]) = "bonus" &&
      YEAR(SalaryPayment[AccrualDateFromDoc]) = {year}),
    "M", MONTH(SalaryPayment[AccrualDateFromDoc])
  )
)
"""
    rows = _pbi_exec(dax)
    mm = sorted({int(r.get("[M]", 0)) for r in rows if r.get("[M]") and 1 <= int(r.get("[M]", 0)) <= 12})
    return [MONTHS_UA[i-1] for i in mm]

# --- ПРЕМІЇ КЕРІВНИКІВ (accruals у EmployeeSalary + payments(character='prize'))
def get_available_years_prizes(employee_name: str) -> list[str]:
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(
    FILTER(EmployeeSalary,
      EmployeeSalary[Employee] = "{employee_name}" &&
      (EmployeeSalary[НарахованоПреміїUAH] <> 0 || EmployeeSalary[НарахованоПреміїUSD] <> 0)
    ),
    "Y", YEAR(EmployeeSalary[Date])
  ),
  SELECTCOLUMNS(
    FILTER(SalaryPayment, SalaryPayment[Employee] = "{employee_name}" && LOWER(SalaryPayment[character]) = "prize"),
    "Y", YEAR(SalaryPayment[AccrualDateFromDoc])
  )
)
"""
    rows = _pbi_exec(dax)
    years = sorted({int(r.get("[Y]", 0)) for r in rows if r.get("[Y]")})
    return [str(y) for y in years]

def get_available_months_prizes(employee_name: str, year: str) -> list[str]:
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(
    FILTER(EmployeeSalary,
      EmployeeSalary[Employee] = "{employee_name}" &&
      YEAR(EmployeeSalary[Date]) = {year} &&
      (EmployeeSalary[НарахованоПреміїUAH] <> 0 || EmployeeSalary[НарахованоПреміїUSD] <> 0)
    ),
    "M", MONTH(EmployeeSalary[Date])
  ),
  SELECTCOLUMNS(
    FILTER(SalaryPayment,
      SalaryPayment[Employee] = "{employee_name}" &&
      LOWER(SalaryPayment[character]) = "prize" &&
      YEAR(SalaryPayment[AccrualDateFromDoc]) = {year}),
    "M", MONTH(SalaryPayment[AccrualDateFromDoc])
  )
)
"""
    rows = _pbi_exec(dax)
    mm = sorted({int(r.get("[M]", 0)) for r in rows if r.get("[M]") and 1 <= int(r.get("[M]", 0)) <= 12})
    return [MONTHS_UA[i-1] for i in mm]




def get_employee_accounts_3330_3320(employee_name: str) -> set[str]:
    """
    Повертає множину кодів рахунків {'3330', '3320'} для співробітника
    починаючи з 01.01.2025. Якщо даних немає — повертає порожню множину.
    """
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return set()

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    dax = f"""
    EVALUATE
    DISTINCT(
      SELECTCOLUMNS(
        FILTER(
          '3330/3320',
          '3330/3320'[RegistrDate] >= DATE(2025,1,1)
            && '3330/3320'[Subconto1Emp] = "{employee_name}"
        ),
        "AccountCode", '3330/3320'[AccountCode]
      )
    )
    """

    body = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    try:
        r = requests.post(url, headers=headers, json=body, timeout=60)
        r.raise_for_status()
        rows = r.json()['results'][0]['tables'][0].get('rows', [])
        # У Power BI значення приходять у вигляді {"[AccountCode]": 3330} або "3330"
        codes = set()
        for row in rows:
            val = row.get("[AccountCode]")
            if val is None: 
                continue
            code = str(val).strip()
            # нормалізуємо до 4-значного коду
            if code.startswith("33"):
                codes.add(code[:4])
        return codes
    except Exception as e:
        logging.exception(f"Помилка запиту до 3330/3320: {e}")
        return set()


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
    

def get_prize_payments(employee_name, year, month):
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
                            SalaryPayment[character] = "prize" &&
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




def format_salary_table(rows, employee_name, year, month, payments, bonuses, bonus_payments, prize_payments):
    
    from datetime import datetime
    from collections import defaultdict

    main_table = "-" * 41 + "\n"
    bonus_table = "-" * 41 + "\n"
    prize_table = "-" * 41 + "\n"

    total_uah, total_usd, total_payment_uah, total_payment_usd = 0.0, 0.0, 0.0, 0.0

    # --- Основна таблиця (без премій) ---
    if rows:
        main_table += f"{'Нарахування':<18}{'UAH':<8}  {'USD':<8}\n"
        main_table += "-" * 41 + "\n"

        for row in rows:
            оклад_uah = float(row.get("[Нараховано Оклад UAH]", 0))
            оклад_usd = float(row.get("[Нараховано Оклад USD]", 0)) if оклад_uah == 0 else 0.0
            додат_uah = float(row.get("[Додаткові нарахування UAH]", 0))
            додат_usd = float(row.get("[Додаткові нарахування USD]", 0))

            total_uah += оклад_uah + додат_uah
            total_usd += оклад_usd + додат_usd

            main_table += f"{'Оклад':<18}{оклад_uah:<8.2f}  {оклад_usd:<8.2f}\n"
            main_table += f"{'Додаткові':<18}{додат_uah:<8.2f}  {додат_usd:<8.2f}\n"

        main_table += "-" * 41 + "\n"
        main_table += f"{'Всього нараховано: ':<18}{total_uah:<8.2f}  {total_usd:<8.2f}\n"

    # --- Виплата зарплати ---
    salary_payments = [p for p in payments if p.get("[Character]", "").strip().lower() == "salary"]

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

    # --- Розрахунок залишку ---
    remaining_uah = total_uah - total_payment_uah
    remaining_usd = total_usd - total_payment_usd
    main_table += f"{'Невиплачений залишок: ':<18}{remaining_uah:<8.2f}  {remaining_usd:<8.2f}\n"

    # --- Бонуси ---
    if bonuses:
        bonus_table += f"{'Нарахування Бонусів':<26}{'USD':<8}\n"
        bonus_table += "-" * 41 + "\n"

        total_bonuses = 0
        bonuses_summary = {"Сейлс": 0, "Оперативний менеджер": 0, "Відсоток ОМ": 0}

        cleaned_bonuses = [{key.strip("[]"): value for key, value in bonus.items()} for bonus in bonuses]

        for bonus in cleaned_bonuses:
            role = bonus.get("ManagerRole", "")
            amount = float(bonus.get("TotalAccrued", 0))

            if role in bonuses_summary:
                bonuses_summary[role] += amount

            total_bonuses += amount

        bonus_table += f"{'Бонуси Сейлс':<26}{bonuses_summary['Сейлс']:<8.2f}\n"
        bonus_table += f"{'Бонуси ОМ фікса':<26}{bonuses_summary['Оперативний менеджер']:<8.2f}\n"
        bonus_table += f"{'Відсоток ОМ 2/5':<26}{bonuses_summary['Відсоток ОМ']:<8.2f}\n"
        bonus_table += "-" * 41 + "\n"
        bonus_table += f"{'Всього нараховано бонусів: ':<26}{total_bonuses:<8.2f}\n\n"

    if bonus_payments:
        bonus_table += "\nВиплата бонусів\n"
        prize_table += "-" * 41 + "\n"
        from collections import defaultdict

        grouped = defaultdict(list)
        for payment in bonus_payments:
            doc_number = payment["[Документ]"]
            doc_date = payment["[Дата платежу]"]
            grouped[doc_number].append(payment)

        total_bonus_paid = 0

        for doc_number, items in grouped.items():
            doc_date = items[0]["[Дата платежу]"]
            bonus_table += f"Документ: {doc_number} від {doc_date}\n"

            total_by_doc = 0
            for item in items:
                місяць = datetime.strptime(item["[МісяцьНарахування]"], "%Y-%m-%d").strftime("%B %Y")
                сума = float(item["[Разом в USD]"])
                total_by_doc += сума
                bonus_table += f"   → {місяць} — {сума:.2f} USD\n"

            bonus_table += f"Сума: {total_by_doc:.2f} USD\n\n"
            total_bonus_paid += total_by_doc

    # --- Премії ---
    has_prizes = any(float(row.get("[Нараховано Премії UAH]", 0)) > 0 or float(row.get("[Нараховано Премії USD]", 0)) > 0 for row in rows or [])

    if has_prizes or prize_payments:
        prize_table += f"{'Нарахування Премій':<26}{'UAH':<8}  {'USD':<8}\n"
        prize_table += "-" * 41 + "\n"

        total_prize_uah = 0
        total_prize_usd = 0

        for row in rows:
            премія_uah = float(row.get("[Нараховано Премії UAH]", 0))
            премія_usd = float(row.get("[Нараховано Премії USD]", 0))

            if премія_uah != 0 or премія_usd != 0:
                prize_table += f"{'Премія':<26}{премія_uah:<8.2f}  {премія_usd:<8.2f}\n"
                total_prize_uah += премія_uah
                total_prize_usd += премія_usd

        prize_table += "-" * 41 + "\n"
        prize_table += f"{'Всього нараховано премій: ':<18}{total_prize_uah:<8.2f}  {total_prize_usd:<8.2f}\n\n"

        if prize_payments:
            prize_table += "\nВиплата премій\n"
            prize_table += "-" * 41 + "\n"
            from collections import defaultdict

            grouped = defaultdict(list)
            for payment in prize_payments:
                doc_number = payment["[Документ]"]
                doc_date = payment["[Дата платежу]"]
                grouped[doc_number].append(payment)

            total_paid = 0
            for doc_number, items in grouped.items():
                doc_date = items[0]["[Дата платежу]"]  # Візьмемо дату з першого рядка
                prize_table += f"Документ: {doc_number} від {doc_date}\n"

                total_by_doc = 0
                for item in items:
                    місяць = datetime.strptime(item["[МісяцьНарахування]"], "%Y-%m-%d").strftime("%B %Y")
                    сума = float(item["[Разом в USD]"])
                    total_by_doc += сума
                    prize_table += f"   → {місяць} — {сума:.2f} USD\n"

                prize_table += f"Сума: {total_by_doc:.2f} USD\n\n"
                total_paid += total_by_doc

            prize_table += f"Всього виплачено премій: {total_paid:.2f} USD\n"

    return main_table.strip(), "", ""#, bonus_table.strip(), prize_table.strip() """ Тимчасово прибираю вивід премій та бонусів """

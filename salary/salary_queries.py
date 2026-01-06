import requests
import logging
from datetime import datetime
from auth import get_power_bi_token
from utils.name_aliases import display_name

# Налаштовуємо logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ──────────────────────────────────────────────────────────────────────────────
# КОНСТАНТИ / ДОВІДНИКИ
# ──────────────────────────────────────────────────────────────────────────────

DATASET_ID = "8b80be15-7b31-49e4-bc85-8b37a0d98f1c"
PBI_URL = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"

MIN_YEAR = 2025
MIN_DATE_DAX = f"DATE({MIN_YEAR},1,1)"  # для DAX фільтрів

MONTHS_UA = [
    "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
    "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
]
MONTHS_MAPPING = {name: i + 1 for i, name in enumerate(MONTHS_UA)}


def month_ua_to_int(month_name: str) -> int | None:
    if not month_name:
        return None
    return MONTHS_MAPPING.get(month_name)


def month_int_to_ua(month_number: int) -> str:
    if 1 <= month_number <= 12:
        return MONTHS_UA[month_number - 1]
    return str(month_number)


# ──────────────────────────────────────────────────────────────────────────────
# POWER BI EXEC
# ──────────────────────────────────────────────────────────────────────────────

def _pbi_exec(query: str) -> list[dict]:
    token = get_power_bi_token()
    if not token:
        logging.error("❌ PBI token missing")
        return []

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}}

    try:
        r = requests.post(PBI_URL, headers=headers, json=body, timeout=60)
        if r.status_code != 200:
            logging.error(f"❌ PBI {r.status_code}: {r.text}")
            return []
        data = r.json()
        return data["results"][0]["tables"][0].get("rows", [])
    except Exception as e:
        logging.exception(f"❌ PBI request failed: {e}")
        return []


# ──────────────────────────────────────────────────────────────────────────────
# СПИСКИ РОКІВ/МІСЯЦІВ (ДЛЯ МЕНЮ)
# ──────────────────────────────────────────────────────────────────────────────

# --- ОКЛАД / ЗП (EmployeeSalary + SalaryPayment(character='salary'))
def get_available_years_salary(employee_name: str) -> list[str]:
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(
    FILTER(EmployeeSalary, EmployeeSalary[Employee] = "{employee_name}"),
    "Y", YEAR(EmployeeSalary[Date])
  ),
  SELECTCOLUMNS(
    FILTER(
      SalaryPayment,
      SalaryPayment[Employee] = "{employee_name}" &&
      LOWER(SalaryPayment[character]) = "salary"
    ),
    "Y", YEAR(DATEVALUE(SalaryPayment[МісяцьНарахування]))
  )
)
"""
    rows = _pbi_exec(dax)
    years = sorted({int(r.get("[Y]", 0)) for r in rows if r.get("[Y]")})
    years = [y for y in years if y >= MIN_YEAR]
    return [str(y) for y in years]


def get_available_months_salary(employee_name: str, year: str) -> list[str]:
    """
    Функція для отримання доступних місяців для вказаного року та співробітника.
    """
    dax = f"""
    EVALUATE
    UNION(
        SELECTCOLUMNS(
            FILTER(
                EmployeeSalary,
                EmployeeSalary[Employee] = "{employee_name}" &&
                YEAR(EmployeeSalary[Date]) = {year}
            ),
            "M", MONTH(EmployeeSalary[Date])
        ),
        SELECTCOLUMNS(
            FILTER(
                SalaryPayment,
                SalaryPayment[Employee] = "{employee_name}" &&
                LOWER(SalaryPayment[character]) = "salary" &&
                YEAR(DATEVALUE(SalaryPayment[МісяцьНарахування])) = {year}
            ),
            "M", MONTH(DATEVALUE(SalaryPayment[МісяцьНарахування]))
        )
    )
    """
    logging.info(f"Запит для отримання місяців для {employee_name} за {year} рік: {dax}")

    # Відправляємо запит до Power BI
    rows = _pbi_exec(dax)

    # Логування результатів
    logging.info(f"Результати запиту для {employee_name} за {year} рік: {rows}")

    # Фільтруємо і сортуємо місяці
    mm = sorted({
        int(r.get("[M]", 0))
        for r in rows
        if r.get("[M]") and 1 <= int(r.get("[M]", 0)) <= 12
    })

    # Логування знайдених місяців
    logging.info(f"Знайдені місяці для {employee_name} за {year} рік: {mm}")

    # Якщо не знайдено жодного місяця, то повідомляємо про відсутність даних
    if not mm:
        logging.warning(f"Не знайдено місяців для {employee_name} за {year} рік.")
        return []

    # Повертаємо місяці у форматі українських назв
    return [month_int_to_ua(m) for m in mm]




# --- БОНУСИ (ПРАВКА): списки років/місяців беремо з BonusesDetails[Period] (DATE),
# щоб бачити періоди коригувань/перерахунків. Не раніше 01.01.2025.
def get_available_years_bonuses(employee_name: str) -> list[str]:
    dax = f"""
EVALUATE
DISTINCT(
  SELECTCOLUMNS(
    FILTER(
      BonusesDetails,
      BonusesDetails[Employee] = "{employee_name}" &&
      BonusesDetails[Period] >= {MIN_DATE_DAX}
    ),
    "Y", YEAR(BonusesDetails[Period])
  )
)
"""
    rows = _pbi_exec(dax)
    years = sorted({int(r.get("[Y]", 0)) for r in rows if r.get("[Y]")})
    years = [y for y in years if y >= MIN_YEAR]
    return [str(y) for y in years]


def get_available_months_bonuses(employee_name: str, year: str) -> list[str]:
    dax = f"""
EVALUATE
DISTINCT(
  SELECTCOLUMNS(
    FILTER(
      BonusesDetails,
      BonusesDetails[Employee] = "{employee_name}" &&
      YEAR(BonusesDetails[Period]) = {year} &&
      BonusesDetails[Period] >= {MIN_DATE_DAX}
    ),
    "M", MONTH(BonusesDetails[Period])
  )
)
"""
    rows = _pbi_exec(dax)
    mm = sorted({
        int(r.get("[M]", 0))
        for r in rows
        if r.get("[M]") and 1 <= int(r.get("[M]", 0)) <= 12
    })
    return [month_int_to_ua(m) for m in mm]


# --- ПРЕМІЇ КЕРІВНИКІВ (EmployeeSalary accruals + SalaryPayment(character='prize'))
def get_available_years_prizes(employee_name: str) -> list[str]:
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(
    FILTER(
      EmployeeSalary,
      EmployeeSalary[Employee] = "{employee_name}" &&
      (EmployeeSalary[НарахованоПреміїUAH] <> 0 || EmployeeSalary[НарахованоПреміїUSD] <> 0)
    ),
    "Y", YEAR(EmployeeSalary[Date])
  ),
  SELECTCOLUMNS(
    FILTER(
      SalaryPayment,
      SalaryPayment[Employee] = "{employee_name}" &&
      LOWER(SalaryPayment[character]) = "prize"
    ),
    "Y", YEAR(SalaryPayment[AccrualDateFromDoc])
  )
)
"""
    rows = _pbi_exec(dax)
    years = sorted({int(r.get("[Y]", 0)) for r in rows if r.get("[Y]")})
    years = [y for y in years if y >= MIN_YEAR]
    return [str(y) for y in years]


def get_available_months_prizes(employee_name: str, year: str) -> list[str]:
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(
    FILTER(
      EmployeeSalary,
      EmployeeSalary[Employee] = "{employee_name}" &&
      YEAR(EmployeeSalary[Date]) = {year} &&
      (EmployeeSalary[НарахованоПреміїUAH] <> 0 || EmployeeSalary[НарахованоПреміїUSD] <> 0)
    ),
    "M", MONTH(EmployeeSalary[Date])
  ),
  SELECTCOLUMNS(
    FILTER(
      SalaryPayment,
      SalaryPayment[Employee] = "{employee_name}" &&
      LOWER(SalaryPayment[character]) = "prize" &&
      YEAR(SalaryPayment[AccrualDateFromDoc]) = {year}
    ),
    "M", MONTH(SalaryPayment[AccrualDateFromDoc])
  )
)
"""
    rows = _pbi_exec(dax)
    mm = sorted({
        int(r.get("[M]", 0))
        for r in rows
        if r.get("[M]") and 1 <= int(r.get("[M]", 0)) <= 12
    })
    return [month_int_to_ua(m) for m in mm]


# ──────────────────────────────────────────────────────────────────────────────
# 3330/3320
# ──────────────────────────────────────────────────────────────────────────────

def get_employee_accounts_3330_3320(employee_name: str) -> set[str]:
    """
    Повертає множину кодів рахунків {'3330', '3320'} для співробітника
    починаючи з 01.01.2025. Якщо даних немає — повертає порожню множину.
    """
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return set()

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    dax = f"""
EVALUATE
DISTINCT(
  SELECTCOLUMNS(
    FILTER(
      '3330/3320',
      '3330/3320'[RegistrDate] >= DATE(2025,1,1) &&
      '3330/3320'[Subconto1Emp] = "{employee_name}"
    ),
    "AccountCode", '3330/3320'[AccountCode]
  )
)
"""
    body = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}

    try:
        r = requests.post(PBI_URL, headers=headers, json=body, timeout=60)
        if r.status_code != 200:
            logging.error(f"❌ PBI {r.status_code}: {r.text}")
            return set()

        rows = r.json()["results"][0]["tables"][0].get("rows", [])
        codes = set()

        for row in rows:
            val = row.get("[AccountCode]")
            if val is None:
                continue
            code = str(val).strip()
            if code.startswith("33"):
                codes.add(code[:4])

        return codes
    except Exception as e:
        logging.exception(f"Помилка запиту до 3330/3320: {e}")
        return set()


# ──────────────────────────────────────────────────────────────────────────────
# ДАНІ ПО ЗП / БОНУСАХ / ВИПЛАТАХ
# ──────────────────────────────────────────────────────────────────────────────

def get_salary_data(employee_name, year, month):
    logging.info(f"Запит на отримання даних для: {employee_name}, рік: {year}, місяць: {month}")

    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    month_number = month_ua_to_int(month)
    if month_number is None:
        logging.error(f"Неправильний місяць: {month}")
        return None

    formatted_month = f"{month_number:02d}"

    query_data = {
        "queries": [{
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
        }],
        "serializerSettings": {"includeNulls": True}
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(PBI_URL, headers=headers, json=query_data, timeout=60)

    if response.status_code == 200:
        data = response.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
        logging.info(f"Отримано {len(rows)} рядків.")
        return rows

    logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
    return None


def get_salary_payments(employee_name, year, month):
    logging.info(f"Запит на отримання платежів для: {employee_name}, рік: {year}, місяць: {month}")

    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    month_number = month_ua_to_int(month)
    if month_number is None:
        logging.error(f"Неправильний місяць: {month}")
        return None
    formatted_month = f"{month_number:02d}"

    query_data = {
        "queries": [{
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
        }],
        "serializerSettings": {"includeNulls": True}
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(PBI_URL, headers=headers, json=query_data, timeout=60)

    if response.status_code == 200:
        data = response.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
        logging.info(f"Отримано {len(rows)} платежів.")
        return rows

    logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
    return None


def get_bonuses(employee_name, year, month):
    """
    Функція для отримання бонусів з таблиці BonusesTable.
    (Залишено як було; змінені тільки списки періодів для меню)
    """
    logging.info(f"Запит на отримання бонусів для: {employee_name}, рік: {year}, місяць: {month}")

    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    month_number = month_ua_to_int(month)
    if month_number is None:
        logging.error(f"Неправильний місяць: {month}")
        return None
    formatted_month = f"{month_number:02d}"

    query_data = {
        "queries": [{
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
        }],
        "serializerSettings": {"includeNulls": True}
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(PBI_URL, headers=headers, json=query_data, timeout=60)

    if response.status_code == 200:
        data = response.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
        logging.info(f"Отримано бонусів: {len(rows)}. Дані: {rows}")
        return rows

    logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
    return None


def get_bonus_payments(employee_name, year, month):
    logging.info(f"Запит на отримання виплат бонусів для: {employee_name}, рік: {year}, місяць: {month}")

    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    month_number = month_ua_to_int(month)
    if month_number is None:
        logging.error(f"Неправильний місяць: {month}")
        return None

    query_data = {
        "queries": [{
            "query": f"""
EVALUATE
SELECTCOLUMNS(
  FILTER(
    SalaryPayment,
    SalaryPayment[Employee] = "{employee_name}" &&
    SalaryPayment[character] = "bonus" &&
    YEAR(SalaryPayment[AccrualDateFromDoc]) = {year} &&
    MONTH(SalaryPayment[AccrualDateFromDoc]) = {month_number}
  ),
  "Дата платежу", SalaryPayment[DocDate],
  "Документ", SalaryPayment[DocNumber],
  "Сума USD", SalaryPayment[SUM_USD],
  "Разом в USD", SalaryPayment[SUMINUSD],
  "МісяцьНарахування", SalaryPayment[МісяцьНарахування]
)
"""
        }],
        "serializerSettings": {"includeNulls": True}
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(PBI_URL, headers=headers, json=query_data, timeout=60)

    if response.status_code == 200:
        data = response.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
        logging.info(f"Отримано виплат бонусів: {len(rows)}. Дані: {rows}")
        return rows

    logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
    return None


def get_prize_payments(employee_name, year, month):
    logging.info(f"Запит на отримання виплат премій для: {employee_name}, рік: {year}, місяць: {month}")

    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    month_number = month_ua_to_int(month)
    if month_number is None:
        logging.error(f"Неправильний місяць: {month}")
        return None

    query_data = {
        "queries": [{
            "query": f"""
EVALUATE
SELECTCOLUMNS(
  FILTER(
    SalaryPayment,
    SalaryPayment[Employee] = "{employee_name}" &&
    SalaryPayment[character] = "prize" &&
    YEAR(SalaryPayment[AccrualDateFromDoc]) = {year} &&
    MONTH(SalaryPayment[AccrualDateFromDoc]) = {month_number}
  ),
  "Дата платежу", SalaryPayment[DocDate],
  "Документ", SalaryPayment[DocNumber],
  "Сума USD", SalaryPayment[SUM_USD],
  "Разом в USD", SalaryPayment[SUMINUSD],
  "МісяцьНарахування", SalaryPayment[МісяцьНарахування]
)
"""
        }],
        "serializerSettings": {"includeNulls": True}
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(PBI_URL, headers=headers, json=query_data, timeout=60)

    if response.status_code == 200:
        data = response.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
        logging.info(f"Отримано виплат премій: {len(rows)}. Дані: {rows}")
        return rows

    logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# ФОРМАТУВАННЯ ТАБЛИЦІ (залишив як у вас)
# ──────────────────────────────────────────────────────────────────────────────

def format_salary_table(rows, employee_name, year, month, payments, bonuses, bonus_payments, prize_payments):
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
    salary_payments = [p for p in (payments or []) if p.get("[Character]", "").strip().lower() == "salary"]

    if salary_payments:
        main_table += "\nВиплата ЗП:\n"
        main_table += f"{'Дата':<10}{'Документ':<10} {'UAH':<8}  {'USD':<8}\n"
        main_table += "-" * 41 + "\n"

        for payment in sorted(salary_payments, key=lambda x: x.get("[Дата платежу]", "")):
            дата = datetime.strptime(payment["[Дата платежу]"], "%Y-%m-%d").strftime("%d.%m.%y")
            doc_number = payment["[Документ]"]
            сума_uah = float(payment.get("[Сума UAH]", 0))
            сума_usd = float(payment.get("[Сума USD]", 0))

            total_payment_uah += сума_uah
            total_payment_usd += сума_usd

            main_table += f"{дата:<10}{doc_number:<10} {сума_uah:<8.2f}  {сума_usd:<8.2f}\n"

        main_table += "-" * 41 + "\n"
        main_table += f"{'Всього виплачено:':<18}{total_payment_uah:<8.2f}  {total_payment_usd:<8.2f}\n\n"

    # --- Розрахунок залишку ---
    remaining_uah = total_uah - total_payment_uah
    remaining_usd = total_usd - total_payment_usd
    main_table += f"{'Невиплачений залишок: ':<18}{remaining_uah:<8.2f}  {remaining_usd:<8.2f}\n"

    # Тимчасово прибираю вивід премій та бонусів (як у вас)
    return main_table.strip(), "", ""

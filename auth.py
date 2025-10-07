import os
import re
import json
import logging
from datetime import datetime, timezone
import requests

from db import (
    add_telegram_user,
    get_user_status,
    get_employee_name,
    delete_user_payments,
    update_user_joined_at,
)

# ---------------------------
# НОРМАЛІЗАЦІЯ НОМЕРІВ
# ---------------------------
def normalize_phone_number(phone_number: str) -> str:
    """
    Приводить номер до уніфікованого формату E.164 без плюса (для порівнянь):
      - лишає лише цифри
      - українські формати 0XXXXXXXXX / 380XXXXXXXXX → 380XXXXXXXXX
      - якщо номер уже міжнародний (наприклад 447... 1...), повертаємо як є (лише цифри)
    ВАЖЛИВО: повертаємо БЕЗ "+" (щоб ключі в мапах були однакові).
    """
    if not phone_number:
        return ""

    digits = re.sub(r"\D", "", str(phone_number))

    # Якщо 10 цифр і починається з "0" (місцевий UA) → додамо 380
    if len(digits) == 10 and digits.startswith("0"):
        return f"380{digits[1:]}"

    # Якщо 9 цифр (без коду) → вважаємо UA і додаємо 380
    if len(digits) == 9:
        return f"380{digits}"

    # Якщо 12 і починається з 380 → вже нормальний UA
    if len(digits) == 12 and digits.startswith("380"):
        return digits

    # Інакше повертаємо як є (наприклад UK 447..., US 1...)
    return digits


# ---------------------------
# POWER BI TOKEN
# ---------------------------
def get_power_bi_token() -> str | None:
    """
    Отримує токен для Power BI (ROPC). Пароль беремо з ENV: PASSWORD
    """
    client_id = os.getenv("PBI_CLIENT_ID", "706d72b2-a9a2-4d90-b0d8-b08f58459ef6")
    username = os.getenv("PBI_USERNAME", "od@ftpua.com")
    password = os.getenv("PASSWORD")
    if not password:
        logging.error("❌ Не задано PASSWORD у змінних оточення.")
        return None

    url = "https://login.microsoftonline.com/common/oauth2/token"
    body = {
        "grant_type": "password",
        "resource": "https://analysis.windows.net/powerbi/api",
        "client_id": client_id,
        "username": username,
        "password": password,
    }

    try:
        resp = requests.post(url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("access_token")
        logging.error(f"❌ Error getting token: {resp.status_code}, {resp.text}")
    except Exception as e:
        logging.exception(f"❌ Exception getting PBI token: {e}")
    return None


# ---------------------------
# POWER BI HELPERS
# ---------------------------
PBI_DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")
PBI_EXEC_URL = f"https://api.powerbi.com/v1.0/myorg/datasets/{PBI_DATASET_ID}/executeQueries"
PBI_HEADERS_BASE = {"Content-Type": "application/json"}


def _pbi_post(query_obj: dict) -> dict | None:
    token = get_power_bi_token()
    if not token:
        return None
    headers = {**PBI_HEADERS_BASE, "Authorization": f"Bearer {token}"}
    try:
        resp = requests.post(PBI_EXEC_URL, headers=headers, json=query_obj, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        logging.error(f"❌ Помилка Power BI {resp.status_code}: {resp.text}")
        return None
    except Exception as e:
        logging.exception(f"❌ Виняток при зверненні до Power BI: {e}")
        return None


def get_employee_directory_from_power_bi() -> dict[str, dict]:
    """
    Будує "еталон" із PBI:
      { employee_name: { "phone": "<normalized>", "status": "<Статус>", "raw_phone": "<як у PBI>" } }
    Якщо у PBI кілька рядків по співробітнику — пріоритезуємо рядок зі статусом "Активний".
    """
    query = {
        "queries": [{
            "query": """
                EVALUATE
                SELECTCOLUMNS(
                    Employees,
                    "Employee", Employees[Employee],
                    "Phone", Employees[PhoneNumberTelegram],
                    "Status", Employees[Status]
                )
            """
        }],
        "serializerSettings": {"includeNulls": True},
    }

    data = _pbi_post(query)
    if not data:
        return {}

    rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
    directory: dict[str, dict] = {}

    for r in rows:
        emp = (r.get("[Employee]") or "").strip()
        phone_raw = (r.get("[Phone]") or "").strip()
        status = (r.get("[Status]") or "").strip()
        phone_norm = normalize_phone_number(phone_raw) if phone_raw else ""

        if emp not in directory:
            directory[emp] = {"phone": phone_norm, "status": status, "raw_phone": phone_raw}
        else:
            # Якщо вже є запис, але новий статус "Активний" — підміняємо
            if status == "Активний":
                directory[emp] = {"phone": phone_norm, "status": status, "raw_phone": phone_raw}
    return directory


def is_phone_number_in_power_bi(phone_number: str) -> tuple[bool, str | None, str | None]:
    """
    Перевіряє наявність номера в PBI ТІЛЬКИ по конкретному номеру (оптимізовано):
    Повертає: (is_active, employee_name, status_from_pbi)
    """
    normalized = normalize_phone_number(phone_number)
    # Фільтруємо в PBI за нормалізованим номером (без плюса)
    # Беремо ВСІ телефони з Employees[PhoneNumberTelegram], нормалізуємо на стороні DAX важко,
    # тому відфільтруємо простішим способом: потягнемо невелику підмножину за текстовим співпадінням
    # (якщо у вас у PBI зберігається "плюс" — прибирайте його в Power Query або зберігайте без плюса).
    query = {
        "queries": [{
            "query": f"""
                EVALUATE
                VAR T =
                    SELECTCOLUMNS(
                        Employees,
                        "Employee", Employees[Employee],
                        "Phone", Employees[PhoneNumberTelegram],
                        "Status", Employees[Status]
                    )
                RETURN
                    FILTER(T, SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE([Phone], " ", ""), "-", ""), "(", ""), ")", "") = "{normalized}")
            """
        }],
        "serializerSettings": {"includeNulls": True},
    }

    data = _pbi_post(query)
    if not data:
        return False, None, None

    rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
    # Якщо кілька — візьмемо перший, пріоритезувати Активний
    if not rows:
        logging.warning(f"🚫 Номер {normalized} не знайдено в PBI.")
        return False, None, None

    # Якщо є кілька рядків, шукаємо "Активний"
    row = None
    for r in rows:
        if (r.get("[Status]") or "").strip() == "Активний":
            row = r
            break
    if row is None:
        row = rows[0]

    employee_name = (row.get("[Employee]") or "").strip()
    status = (row.get("[Status]") or "").strip()
    is_active = status == "Активний"
    logging.info(f"✅ PBI: {employee_name} / {status} для {normalized}")
    return is_active, employee_name or None, status or None


# ---------------------------
# HIGH-LEVEL OPS
# ---------------------------
def verify_and_add_user(phone_number: str, telegram_id: int | str, telegram_name: str):
    """
    Викликається при логіні в бота:
      - перевіряє номер у PBI
      - за потреби оновлює joined_at, статус, ім'я
      - додає/оновлює запис про користувача через add_telegram_user
    """
    is_active, employee_name_pbi, status_from_pbi = is_phone_number_in_power_bi(phone_number)
    logging.info(
        f"📊 PBI для {phone_number}: is_active={is_active}, employee={employee_name_pbi}, status={status_from_pbi}"
    )

    # fallback: якщо PBI не дав name — спробуємо з БД
    employee_name = employee_name_pbi or get_employee_name(phone_number)

    new_status = "active" if status_from_pbi == "Активний" else "deleted"
    current_status = get_user_status(phone_number)
    logging.info(f"🛠️ БД статус: {current_status} → новий: {new_status}")

    # Якщо вже був deleted — чистимо платежі (політика безпеки)
    if current_status == "deleted":
        logging.info(f"🧹 Видаляємо платежі для {phone_number}, бо статус був 'deleted'.")
        delete_user_payments(phone_number)

    if current_status != new_status:
        if current_status == "deleted" and new_status == "active":
            now_utc = datetime.now(timezone.utc)
            update_user_joined_at(phone_number, now_utc)
            logging.info(f"🔄 Повернення користувача. joined_at → {now_utc.isoformat()}")

        add_telegram_user(phone_number, telegram_id, telegram_name, employee_name, new_status)
        logging.info(f"✅ Статус оновлено: {phone_number} → {new_status}")
    else:
        # все одно оновимо ім'я/telegram, щоб не відставали
        add_telegram_user(phone_number, telegram_id, telegram_name, employee_name, new_status)
        logging.info(f"✅ Статус без змін: {phone_number} → {current_status}")


def get_user_debt_data(manager_name: str):
    """
    Повертає рядки для дебіторки конкретного менеджера з PBI.
    """
    query = {
        "queries": [{
            "query": f"""
                EVALUATE 
                SELECTCOLUMNS(
                    FILTER(
                        Deb,
                        Deb[Manager] = "{manager_name}" && Deb[Inform] <> 1
                    ),
                    "Client", Deb[Client],
                    "Sum_$", Deb[Sum_$],
                    "Manager", Deb[Manager],
                    "PlanDatePay", Deb[PlanDatePay],
                    "Account", Deb[Account],
                    "Deal", Deb[Deal],
                    "AccountDate", Deb[AccountDate]
                )
            """
        }],
        "serializerSettings": {"includeNulls": True}
    }

    data = _pbi_post(query)
    if not data:
        return None
    rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
    return rows

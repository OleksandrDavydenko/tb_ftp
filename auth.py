import os
import re
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
    Приводить номер до формату для порівнянь: лише цифри (без '+').
    UA: 0XXXXXXXXX/380XXXXXXXXX/XXXXXXXXX → 380XXXXXXXXX
    Інші міжнародні: лиш цифри.
    """
    if not phone_number:
        return ""
    digits = re.sub(r"\D", "", str(phone_number))

    if len(digits) == 10 and digits.startswith("0"):
        return f"380{digits[1:]}"
    if len(digits) == 9:
        return f"380{digits}"
    if len(digits) == 12 and digits.startswith("380"):
        return digits
    return digits


# ---------------------------
# POWER BI TOKEN
# ---------------------------
def get_power_bi_token() -> str | None:
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
    except Exception as e:
        logging.exception(f"❌ Виняток при зверненні до Power BI: {e}")
    return None


def get_employee_directory_from_power_bi() -> dict[str, dict]:
    """
    Повертає мапу:
      { employee_name: { "phone": "<normalized>", "status": "<Статус>", "raw_phone": "<як у PBI>" } }
    Якщо кілька рядків по співробітнику — беремо той, де статус "Активний".
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
            if status == "Активний":
                directory[emp] = {"phone": phone_norm, "status": status, "raw_phone": phone_raw}
    return directory


def is_phone_number_in_power_bi(phone_number: str) -> tuple[bool, str | None, str | None]:
    """
    Шукаємо номер в PBI: тягнемо Employee/Phone/Status і ПОРІВНЮЄМО вже в Python
    з використанням normalize_phone_number(), щоб збігались правила (UA 0XXXXXXXXX → 380XXXXXXXXX і т.д.).
    """
    target = normalize_phone_number(phone_number)

    query = {
        "queries": [{
            "query": """
                EVALUATE
                SELECTCOLUMNS(
                    FILTER(Employees, NOT ISBLANK(Employees[PhoneNumberTelegram])),
                    "Employee", Employees[Employee],
                    "Phone",    Employees[PhoneNumberTelegram],
                    "Status",   Employees[Status]
                )
            """
        }],
        "serializerSettings": {"includeNulls": True},
    }

    data = _pbi_post(query)
    if not data:
        return False, None, None

    rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
    if not rows:
        return False, None, None

    # шукаємо всі збіги по нормалізованому номеру
    matches = []
    for r in rows:
        phone_raw = (r.get("[Phone]") or "").strip()
        if not phone_raw:
            continue
        if normalize_phone_number(phone_raw) == target:
            matches.append(r)

    if not matches:
        # корисно лишити детальніший лог для діагностики
        logging.warning(f"🚫 Номер (raw={phone_number}, norm={target}) не знайдено серед {len(rows)} записів PBI.")
        return False, None, None

    # віддаємо активний, якщо є; інакше перший
    row = next((m for m in matches if (m.get("[Status]") or "").strip() == "Активний"), matches[0])
    employee_name = (row.get("[Employee]") or "").strip() or None
    status = (row.get("[Status]") or "").strip() or None
    is_active = status == "Активний"

    logging.info(f"✅ PBI: {employee_name} / {status} для {target} (знайдено {len(matches)} збіг(ів))")
    return is_active, employee_name, status



# ---------------------------
# HIGH-LEVEL OPS
# ---------------------------
def verify_and_add_user(phone_number: str, telegram_id: int | str, telegram_name: str):
    """
    При логіні:
      - перевіряємо номер у PBI
      - при БУДЬ-ЯКІЙ зміні статусу оновлюємо joined_at
      - додаємо/оновлюємо запис
    """
    is_active, employee_name_pbi, status_from_pbi = is_phone_number_in_power_bi(phone_number)
    logging.info(
        f"📊 PBI для {phone_number}: is_active={is_active}, employee={employee_name_pbi}, status={status_from_pbi}"
    )

    employee_name = employee_name_pbi or get_employee_name(phone_number)
    new_status = "active" if status_from_pbi == "Активний" else "deleted"
    current_status = get_user_status(phone_number)
    logging.info(f"🛠️ БД статус: {current_status} → новий: {new_status}")

    # Якщо вже був deleted — чистимо платежі (політика безпеки)
    if current_status == "deleted":
        logging.info(f"🧹 Видаляємо платежі для {phone_number}, бо статус був 'deleted'.")
        delete_user_payments(phone_number)

    if current_status != new_status:
        # ОНОВЛЮЄМО joined_at ПРИ БУДЬ-ЯКІЙ ЗМІНІ СТАТУСУ
        now_utc = datetime.now(timezone.utc)
        update_user_joined_at(phone_number, now_utc)
        logging.info(f"🔄 Зміна статусу. joined_at → {now_utc.isoformat()}")

        add_telegram_user(phone_number, telegram_id, telegram_name, employee_name, new_status)
        logging.info(f"✅ Статус оновлено: {phone_number} → {new_status}")
    else:
        add_telegram_user(phone_number, telegram_id, telegram_name, employee_name, new_status)
        logging.info(f"✅ Статус без змін: {phone_number} → {current_status}")


def get_user_debt_data(manager_name: str):
    query = {
        "queries": [{
            "query": f"""
                EVALUATE 
                SELECTCOLUMNS(
                    FILTER(
                        Deb,
                        (Deb[Manager] = "{manager_name}" || Deb[Seller] = "{manager_name}") && Deb[Inform] <> 1
                    ),
                    "Client", Deb[Client],
                    "Sum_$", Deb[Sum_$],
                    "Manager_or_Seller", IF(Deb[Manager] = "{manager_name}", Deb[Manager], Deb[Seller]),
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

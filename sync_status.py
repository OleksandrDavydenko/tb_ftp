import logging
from datetime import datetime
import requests

from auth import get_power_bi_token, normalize_phone_number
from db import (
    get_all_users,
    update_user_status,
    delete_user_payments,
    update_user_joined_at,
    update_employee_name,
)

DATASET_ID = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
POWER_BI_URL = f'https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries'


def _fetch_power_bi_directory():
    """
    Разовий запит у Power BI: повертає мапу нормалізованих телефонів → (Employee, Status).
    Якщо запит не вдався — повертає None (щоб не ламати статуси локально).
    """
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати токен Power BI — перериваю синхронізацію статусів.")
        return None

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Беремо всіх співробітників з телефоном і статусом (без додаткових фільтрів)
    # Далі вже у коді вирішуємо, хто 'Активний', а хто ні.
    query_data = {
        "queries": [
            {
                "query": """
                    EVALUATE
                    SELECTCOLUMNS(
                        FILTER(
                            Employees,
                            NOT(ISBLANK(Employees[PhoneNumber]))
                        ),
                        "Employee", Employees[Employee],
                        "PhoneNumber", Employees[PhoneNumber],
                        "Status", Employees[Status]
                    )
                """
            }
        ],
        "serializerSettings": {"includeNulls": True}
    }

    try:
        resp = requests.post(POWER_BI_URL, headers=headers, json=query_data, timeout=60)
    except Exception as e:
        logging.exception(f"❌ Помилка мережі при зверненні до Power BI: {e}")
        return None

    if resp.status_code != 200:
        logging.error(f"❌ Power BI повернув {resp.status_code}: {resp.text}")
        return None

    data = resp.json()
    try:
        rows = data["results"][0]["tables"][0].get("rows", [])
    except (KeyError, IndexError):
        rows = []

    directory = {}
    for r in rows:
        # Ключі у відповіді приходять у форматі з дужками: "[PhoneNumber]" тощо
        phone_raw = r.get("[PhoneNumber]", "") or ""
        emp_name = r.get("[Employee]", "") or ""
        status = r.get("[Status]", "") or ""

        norm = normalize_phone_number(phone_raw)
        if norm:
            directory[norm] = (emp_name, status)

    logging.info(f"🗂️ Отримано з Power BI записів: {len(directory)}")
    return directory


def sync_user_statuses():
    """
    Перевіряє статуси користувачів у БД та Power BI:
    - разом витягує довідник телефонів/статусів/імен з Power BI;
    - по кожному користувачу звіряє статус;
    - оновлює статус у БД та чистить платежі для 'deleted';
    - оновлює employee_name за потреби.
    """
    logging.info("🔄 Початок перевірки статусів користувачів...")

    # 1) Разово тягнемо довідник з Power BI
    directory = _fetch_power_bi_directory()
    if directory is None:
        # Не міняємо статуси, якщо Power BI недоступний
        logging.warning("⚠️ Синхронізацію статусів пропущено через недоступність Power BI.")
        return

    # 2) Беремо всіх локальних користувачів
    users = get_all_users()
    updated_users = 0
    deleted_users = 0
    updated_names = 0

    for user in users:
        phone_number = user["phone_number"]
        current_status = user["status"]
        current_employee_name = user.get("employee_name")

        norm_phone = normalize_phone_number(phone_number)
        emp_name_from_pbi = None
        status_from_pbi = None

        # Якщо номер є у довіднику — беремо його статус та ім'я
        if norm_phone in directory:
            emp_name_from_pbi, status_from_pbi = directory[norm_phone]

        # Твоя вихідна логіка: тільки "Активний" = active, інакше — deleted
        new_status = "active" if status_from_pbi == "Активний" else "deleted"

        # Оновлення статусу, якщо він змінився
        if current_status != new_status:
            logging.info(f"🔄 Оновлення статусу для {phone_number}: {current_status} → {new_status}")

            if new_status == "deleted":
                delete_user_payments(phone_number)
                deleted_users += 1

            if current_status == "deleted" and new_status == "active":
                update_user_joined_at(phone_number, datetime.now())

            update_user_status(phone_number, new_status)
            updated_users += 1

        # Оновлення імені, якщо воно є в Power BI і змінилось/було None
        if emp_name_from_pbi and (current_employee_name is None or current_employee_name != emp_name_from_pbi):
            logging.info(f"🔄 Оновлення імені для {phone_number}: {current_employee_name} → {emp_name_from_pbi}")
            update_employee_name(phone_number, emp_name_from_pbi)
            updated_names += 1

    logging.info(
        f"✅ Синхронізація завершена: {updated_users} оновлених статусів, "
        f"{updated_names} оновлених імен, {deleted_users} платежів видалено."
    )

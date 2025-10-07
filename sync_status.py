import logging
from datetime import datetime, timezone
from collections import defaultdict

from auth import (
    is_phone_number_in_power_bi,
    get_employee_directory_from_power_bi,
    normalize_phone_number,
)
from db import (
    get_all_users,
    update_user_status,
    delete_user_payments,
    update_user_joined_at,
    update_employee_name,
)

def sync_user_statuses():
    """
    Синхронізує користувачів між Power BI і БД:
      1) оновлює статуси/імена по кожному номеру (як і було)
      2) ДЕДУП: для кожного employee_name активним лишається лише той телефон, який вказано у PBI.
         Усі інші телефони цього співробітника → deleted + чистка платежів.
    """
    logging.info("🔄 Початок перевірки статусів користувачів...")

    updated_users = 0
    deleted_users = 0
    updated_names = 0

    # (1) Еталон з PBI: employee_name -> {phone, status}
    pbi_dir = get_employee_directory_from_power_bi()
    if not pbi_dir:
        logging.warning("⚠️ Не вдалося отримати довідник з PBI. Продовжимо лише по номеру.")
    else:
        logging.info(f"📒 Довідник PBI завантажено: {len(pbi_dir)} співробітників.")

    users = get_all_users()

    # --- Крок 1: синхронізація по кожному запису (номер → статус/ім'я) ---
    for user in users:
        phone_number = user["phone_number"]
        current_status = user["status"]
        current_employee_name = user.get("employee_name")

        try:
            is_active, employee_name, status_from_pbi = is_phone_number_in_power_bi(phone_number)
            new_status = "active" if status_from_pbi == "Активний" else "deleted"

            # Оновимо ім'я з PBI, якщо його немає або змінилось
            if employee_name and (current_employee_name is None or current_employee_name != employee_name):
                update_employee_name(phone_number, employee_name)
                updated_names += 1
                current_employee_name = employee_name
                logging.info(f"👤 Ім'я оновлено для {phone_number}: {employee_name}")

            # Якщо статус змінився
            if current_status != new_status:
                logging.info(f"🔄 Статус {phone_number}: {current_status} → {new_status}")

                if new_status == "deleted":
                    delete_user_payments(phone_number)
                    deleted_users += 1

                if current_status == "deleted" and new_status == "active":
                    update_user_joined_at(phone_number, datetime.now(timezone.utc))

                update_user_status(phone_number, new_status)
                updated_users += 1

        except Exception as e:
            logging.exception(f"❌ Помилка обробки користувача {phone_number}: {e}")

    # --- Крок 2: ДЕДУП по employee_name відносно PBI ---
    users = get_all_users()  # перечитати після оновлень
    by_emp = defaultdict(list)
    for u in users:
        emp = u.get("employee_name")
        if emp:
            by_emp[emp].append(u)

    for emp, records in by_emp.items():
        try:
            pbi_rec = pbi_dir.get(emp) if pbi_dir else None

            # Якщо у PBI немає такого співробітника — усе по ньому робимо deleted
            if not pbi_rec:
                for r in records:
                    if r["status"] != "deleted":
                        update_user_status(r["phone_number"], "deleted")
                        delete_user_payments(r["phone_number"])
                        deleted_users += 1
                        updated_users += 1
                        logging.info(f"🗑️ {emp}: немає в PBI → {r['phone_number']} → deleted")
                continue

            pbi_phone = normalize_phone_number(pbi_rec.get("phone") or "")
            pbi_active = (pbi_rec.get("status") == "Активний")

            # Якщо у PBI статус не Активний або немає телефону — гасимо всі
            if not pbi_active or not pbi_phone:
                for r in records:
                    if r["status"] != "deleted":
                        update_user_status(r["phone_number"], "deleted")
                        delete_user_payments(r["phone_number"])
                        deleted_users += 1
                        updated_users += 1
                        logging.info(f"🗑️ {emp}: PBI не активний/без телефону → {r['phone_number']} → deleted")
                continue

            # Інакше лишаємо активним лише той запис, що дорівнює pbi_phone
            for r in records:
                ph_norm = normalize_phone_number(r["phone_number"])
                if ph_norm == pbi_phone:
                    if r["status"] != "active":
                        update_user_status(r["phone_number"], "active")
                        updated_users += 1
                        logging.info(f"✅ {emp}: {ph_norm} → active (як у PBI)")
                else:
                    if r["status"] != "deleted":
                        update_user_status(r["phone_number"], "deleted")
                        delete_user_payments(r["phone_number"])
                        deleted_users += 1
                        updated_users += 1
                        logging.info(f"🗑️ {emp}: інший телефон {ph_norm} ≠ {pbi_phone} → deleted")
        except Exception as e:
            logging.exception(f"❌ Помилка дедуплікації для {emp}: {e}")

    logging.info(
        f"✅ Синхронізація завершена: статусів оновлено={updated_users}, імен оновлено={updated_names}, платежів видалено={deleted_users}."
    )

import logging
from datetime import datetime
from auth import is_phone_number_in_power_bi
from db import get_all_users, update_user_status, delete_user_payments, update_user_joined_at

def sync_user_statuses():
    """
    Перевіряє статуси користувачів у БД та Power BI, оновлює їх у БД і видаляє платежі для неактивних.
    """
    logging.info("🔄 Початок перевірки статусів користувачів...")
    
    users = get_all_users()  # Отримуємо всіх користувачів із БД
    updated_users = 0
    deleted_users = 0

    for user in users:
        phone_number = user["phone_number"]
        current_status = user["status"]

        # Отримуємо статус із Power BI
        is_active, employee_name, status_from_power_bi = is_phone_number_in_power_bi(phone_number)
        new_status = "active" if status_from_power_bi == "Активний" else "deleted"

        if current_status != new_status:
            logging.info(f"🔄 Оновлення статусу для {phone_number}: {current_status} → {new_status}")
            
            # Якщо користувач стає "deleted", видаляємо його платежі
            if new_status == "deleted":
                delete_user_payments(phone_number)
                deleted_users += 1
            
            # Якщо користувач повертається з "deleted" в "active", оновлюємо joined_at
            if current_status == "deleted" and new_status == "active":
                update_user_joined_at(phone_number, datetime.now())

            # Оновлюємо статус у БД
            update_user_status(phone_number, new_status)
            updated_users += 1

    logging.info(f"✅ Синхронізація завершена: {updated_users} оновлених користувачів, {deleted_users} платежів видалено.")

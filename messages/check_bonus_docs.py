# messages/check_bonus_docs.py

import os
import requests
from telegram import Bot
from auth import get_power_bi_token
from db import get_db_connection, mark_bonus_docs_notified, get_active_users


BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATASET_ID = os.getenv("PBI_DATASET_ID")
bot = Bot(token=BOT_TOKEN)

def get_unnotified_docs():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT doc_number, period FROM bonus_docs WHERE is_notified = FALSE")
    docs = cursor.fetchall()
    cursor.close()
    conn.close()
    return docs  # [(doc_number, period), ...]

def fetch_employees_for_doc(doc_number):
    dax = f"""
    EVALUATE
    VALUES(
        SELECTCOLUMNS(
            FILTER(BonusesDetails, BonusesDetails[DocNumber] = "{doc_number}"),
            "Employee", BonusesDetails[Employee]
        )
    )
    """
    token = get_power_bi_token()
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}

    r = requests.post(url, headers=headers, json=payload)
    if r.status_code != 200:
        return []

    data = r.json()
    rows = data["results"][0]["tables"][0].get("rows", [])
    employees = set()

    for row in rows:
        key = next((k for k in row if "Employee" in k), None)
        if key and row[key]:
            employees.add(row[key])
    return list(employees)

def notify_employees(telegram_users, doc_number, period):
    message = (
        f"📄 Зʼявився новий документ нарахування бонусів:\n"
        f"• Номер: <b>{doc_number}</b>\n"
        f"• Період: <b>{period}</b>"
    )
    for user in telegram_users:
        try:
            bot.send_message(
                chat_id=user['telegram_id'],
                text=message,
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"❌ Не вдалося надіслати повідомлення {user['employee_name']}: {e}")

async def check_bonus_docs():
    docs_to_check = get_unnotified_docs()
    if not docs_to_check:
        return

    active_users = get_active_users()
    active_map = {user["employee_name"]: user for user in active_users}

    for doc_number, period in docs_to_check:
        employees = fetch_employees_for_doc(doc_number)

        matched_users = [
            active_map[emp] for emp in employees if emp in active_map
        ]

        if matched_users:
            notify_employees(matched_users, doc_number, period)

    # Після повідомлення оновлюємо статус на TRUE
    affected = mark_bonus_docs_notified([doc[0] for doc in docs_to_check])

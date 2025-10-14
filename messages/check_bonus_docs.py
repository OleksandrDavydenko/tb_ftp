import os
import logging
import requests
from telegram import Bot
from auth import get_power_bi_token
from db import get_db_connection, mark_bonus_docs_notified, get_active_users

# Логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Токени
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATASET_ID = os.getenv("PBI_DATASET_ID")

# Отримання не повідомлених документів
def get_unnotified_docs():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT doc_number, period FROM bonus_docs WHERE is_notified = FALSE")
    docs = cursor.fetchall()
    cursor.close()
    conn.close()
    logging.info(f"📄 Знайдено документів з is_notified = FALSE: {len(docs)}")
    return docs

# DAX-запит для отримання унікальних співробітників з документа
def fetch_employees_for_doc(doc_number):
    dax = f'''
    EVALUATE
    SUMMARIZECOLUMNS(
        BonusesDetails[Employee],
        FILTER(
            BonusesDetails,
            BonusesDetails[DocNumber] = "{doc_number}"
        )
    )
    '''

    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати Power BI токен.")
        return []

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}

    logging.info(f"📤 Надсилаю DAX-запит для документа {doc_number}...")
    r = requests.post(url, headers=headers, json=payload)

    if r.status_code != 200:
        logging.error(f"❌ Power BI запит не вдався: {r.status_code} — {r.text}")
        return []

    try:
        data = r.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
        logging.info(f"📦 Отримано {len(rows)} рядків з Power BI для документа {doc_number}")

        employees = set()
        for row in rows:
            # Power BI API іноді повертає ключ без квадратних дужок
            key = next((k for k in row if "Employee" in k), None)
            if key and row[key]:
                employees.add(row[key])

        logging.info(f"👥 Знайдені унікальні співробітники: {list(employees)}")
        return list(employees)

    except Exception as e:
        logging.error(f"❌ Помилка при обробці відповіді Power BI: {e}")
        return []


# Надсилання повідомлення
def send_notification(telegram_id, message):
    try:
        bot = Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=telegram_id, text=message, parse_mode="HTML")
        logging.info(f"✅ Повідомлення надіслано Telegram ID: {telegram_id}")
    except Exception as e:
        logging.error(f"❌ Помилка при надсиланні повідомлення Telegram ID {telegram_id}: {e}")

# Основна функція перевірки
def check_bonus_docs():
    logging.info("📥 Перевірка нових бонус-документів...")
    docs_to_check = get_unnotified_docs()
    if not docs_to_check:
        logging.info("ℹ️ Нових документів немає.")
        return

    active_users = get_active_users()
    logging.info(f"🟢 Активних користувачів у базі: {len(active_users)}")

    # Побудова мапи співробітників
    active_map = {user["employee_name"]: user for user in active_users}

    for doc_number, period in docs_to_check:
        logging.info(f"🔍 Обробка документа: {doc_number} — {period}")
        employees = fetch_employees_for_doc(doc_number)

        matched_users = []
        for emp in employees:
            if emp in active_map:
                matched_users.append(active_map[emp])
                logging.info(f"✅ Знайдено активного співробітника: {emp}")
            else:
                logging.warning(f"⚠️ Співробітника '{emp}' немає серед активних у БД")

        if not matched_users:
            logging.warning(f"⚠️ Для документа {doc_number} не знайдено активних співробітників.")
            continue

        message = (
            f"📄 Зʼявився новий документ нарахування бонусів:\n"
            f"• Номер: <b>{doc_number}</b>\n"
            f"• Період: <b>{period}</b>"
        )

        for user in matched_users:
            send_notification(user["telegram_id"], message)

    affected = mark_bonus_docs_notified([doc[0] for doc in docs_to_check])
    logging.info(f"✅ Оновлено статусів is_notified: {affected}")

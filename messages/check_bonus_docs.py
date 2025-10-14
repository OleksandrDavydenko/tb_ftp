import os
import logging
import requests
from telegram import Bot
from auth import get_power_bi_token
from db import get_db_connection, mark_bonus_docs_notified, get_active_users

# Логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Токени
KEY = os.getenv('TELEGRAM_BOT_TOKEN')
DATASET_ID = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'

if not DATASET_ID:
    logging.error("❌ PBI_DATASET_ID не встановлено у змінних середовища.")

if not KEY:
    logging.warning("⚠️ TELEGRAM_TOKEN порожній: повідомлення не будуть надіслані.")

# Отримання не повідомлених документів
def get_unnotified_docs():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT doc_number, period FROM bonus_docs WHERE is_notified = FALSE")
    docs = cursor.fetchall()
    cursor.close()
    conn.close()
    logging.info(f"📄 Знайдено документів з is_notified = FALSE: {len(docs)}")
    return docs  # [(doc_number, period), ...]

# DAX-запит для отримання унікальних співробітників з документа
def fetch_employees_for_doc(doc_number: str):
    # ВИКОРИСТОВУЄМО ТОЧНО ТЕ, ЩО ПРАЦЮЄ У ТВОЄМУ ТЕСТІ
    safe_doc = doc_number.replace('"', '""')
    dax = f'''
    EVALUATE
    DISTINCT(
        SELECTCOLUMNS(
            FILTER(BonusesDetails, BonusesDetails[DocNumber] = "{safe_doc}"),
            "Employee", BonusesDetails[Employee]
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

    logging.info(f"📤 Надсилаю DAX для {doc_number}: {dax.strip()}")
    r = requests.post(url, headers=headers, json=payload)

    if r.status_code != 200:
        # Показуємо тіло відповіді — в тесті це допомогло
        logging.error(f"❌ Power BI запит не вдався: {r.status_code} — {r.text}")
        return []

    try:
        data = r.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
        logging.info(f"📦 Отримано {len(rows)} рядків з Power BI для документа {doc_number}")

        employees = set()
        for row in rows:
            # Ключ може бути "Employee" або "[Employee]" або "BonusesDetails[Employee]"
            key = next((k for k in row if "Employee" in k), None)
            if key and row[key]:
                employees.add(str(row[key]).strip())

        logging.info(f"👥 Знайдені унікальні співробітники для {doc_number}: {list(employees)}")
        return list(employees)

    except Exception as e:
        logging.error(f"❌ Помилка при обробці відповіді Power BI: {e}")
        return []

# Надсилання повідомлення
def send_notification(telegram_id, message):
    if not KEY:
        logging.error("❌ TELEGRAM_TOKEN відсутній — пропускаю відправку повідомлення.")
        return
    try:
        bot = Bot(token=KEY)
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
    active_map = {str(user["employee_name"]).strip(): user for user in active_users}

    docs_to_mark = []  # ✅ позначимо тільки ті документи, по яких справді надіслали повідомлення

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
            logging.warning(f"⚠️ Для документа {doc_number} не знайдено активних співробітників — статус не оновлюю.")
            continue  # ❗ НЕ позначаємо документ notified

        message = (
            f"📄 Зʼявився новий документ нарахування бонусів:\n"
            f"• Номер: <b>{doc_number}</b>\n"
            f"• Період: <b>{period}</b>"
        )

        for user in matched_users:
            send_notification(user["telegram_id"], message)

        docs_to_mark.append(doc_number)

    if docs_to_mark:
        affected = mark_bonus_docs_notified(docs_to_mark)
        logging.info(f"✅ Оновлено статусів is_notified (тільки по надісланих): {affected}")
    else:
        logging.info("ℹ️ Жоден документ не було оновлено (не було кому надіслати).")

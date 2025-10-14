import os
import logging
import time
import requests
from telegram import Bot
from telegram.error import Forbidden, RetryAfter, TimedOut, NetworkError

from auth import get_power_bi_token
from db import get_db_connection, mark_bonus_docs_notified, get_active_users

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ✅ Єдина узгоджена змінна оточення з токеном
KEY = os.getenv("TELEGRAM_BOT_TOKEN")
DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")

if not KEY:
    logging.warning("⚠️ TELEGRAM_BOT_TOKEN порожній — відправки не буде.")
BOT = Bot(token=KEY) if KEY else None

def get_unnotified_docs():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT doc_number, period FROM bonus_docs WHERE is_notified = FALSE")
    docs = cur.fetchall()
    cur.close(); conn.close()
    logging.info(f"📄 Знайдено документів з is_notified=FALSE: {len(docs)}")
    return docs

def fetch_employees_for_doc(doc_number: str):
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

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code != 200:
        logging.error(f"❌ Power BI запит не вдався: {r.status_code} — {r.text}")
        return []

    data = r.json()
    rows = data["results"][0]["tables"][0].get("rows", [])
    logging.info(f"📦 Отримано {len(rows)} рядків з Power BI для документа {doc_number}")

    employees = set()
    for row in rows:
        key = next((k for k in row if "Employee" in k), None)
        if key and row[key]:
            employees.add(str(row[key]).strip())
    return list(employees)

# ✅ Надсилання з retry + детальними логами та поверненням True/False
def send_notification(telegram_id: int, message: str, retries: int = 2) -> bool:
    if not BOT:
        return False

    attempt = 0
    while attempt <= retries:
        try:
            msg = BOT.send_message(
                chat_id=int(telegram_id),
                text=message,
                parse_mode="HTML",
                disable_notification=False  # хочемо push
            )
            # якщо це PTB v13 – msg об’єкт синхронний; v20 – це корутина (але в тебе вже немає warning-ів)
            mid = getattr(msg, "message_id", None)
            logging.info(f"✅ Надіслано {telegram_id}, message_id={mid}")
            return True

        except RetryAfter as e:
            attempt += 1
            wait_s = int(getattr(e, "retry_after", 2))
            logging.warning(f"⏳ FloodWait {wait_s}s для {telegram_id} (спроба {attempt}/{retries}).")
            time.sleep(wait_s)
        except Forbidden as e:
            logging.error(f"🚫 Forbidden для {telegram_id}: {e} (бот заблокований / нема /start)")
            return False
        except (TimedOut, NetworkError) as e:
            attempt += 1
            logging.warning(f"🌐 Тимчасова помилка для {telegram_id}: {e} (спроба {attempt}/{retries}).")
            time.sleep(1)
        except Exception as e:
            logging.error(f"❌ Помилка відправки {telegram_id}: {e}")
            return False
    return False

def check_bonus_docs():
    logging.info("📥 Перевірка нових бонус-документів...")
    docs_to_check = get_unnotified_docs()
    if not docs_to_check:
        logging.info("ℹ️ Нових документів немає.")
        return

    active_users = get_active_users()
    logging.info(f"🟢 Активних користувачів у базі: {len(active_users)}")
    active_map = {str(u["employee_name"]).strip(): u for u in active_users}

    docs_to_mark = []

    for doc_number, period in docs_to_check:
        logging.info(f"🔍 Обробка документа: {doc_number} — {period}")
        employees = fetch_employees_for_doc(doc_number)

        matched = []
        for emp in employees:
            u = active_map.get(emp)
            if u and u.get("telegram_id"):
                matched.append(u)
                logging.info(f"✅ Активний співробітник: {emp} → {u['telegram_id']}")
            else:
                logging.warning(f"⚠️ '{emp}' відсутній серед активних або без telegram_id")

        if not matched:
            logging.warning(f"⚠️ {doc_number}: немає кому надіслати — статус не оновлюю.")
            continue

        message = (
            "📄 Зʼявився новий документ нарахування бонусів:\n"
            f"• Номер: <b>{doc_number}</b>\n"
            f"• Період: <b>{period}</b>"
        )

        sent_any = False
        for tg_id in {m["telegram_id"] for m in matched}:   # унікальні ID
            ok = send_notification(tg_id, message)
            sent_any = sent_any or ok
            time.sleep(0.03)  # легкий тротлінг

        if sent_any:
            docs_to_mark.append(doc_number)
        else:
            logging.warning(f"⚠️ {doc_number}: усі відправки невдалі — статус не оновлюю.")

    if docs_to_mark:
        affected = mark_bonus_docs_notified(docs_to_mark)
        logging.info(f"✅ Оновлено is_notified по надісланих документах: {affected}")
    else:
        logging.info("ℹ️ Жоден документ не оновлено (успішних відправок не було).")

# -*- coding: utf-8 -*-
"""
Бонус-документи: шукаємо працівників для DocNumber у Power BI
і розсилаємо повідомлення ТІЛЬКИ тим, хто є активним у нашій БД (users)
та має telegram_id. Відправка виконується по відповідності employee_name.

Логи показують:
- скільки неповідомлених документів;
- який DAX шлемо;
- скільки рядків повернув PBI;
- кого співставили з активними;
- кому реально відправилось (з message_id);
- які документи помічені як is_notified.
"""

import os
import time
import logging
import requests
from typing import List, Dict, Any, Iterable

from telegram import Bot
from telegram.error import Forbidden, RetryAfter, TimedOut, NetworkError

from auth import get_power_bi_token
from db import get_db_connection, mark_bonus_docs_notified, get_active_users

# ---------------------------
# ЛОГУВАННЯ
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ---------------------------
# КОНФІГ
# ---------------------------
KEY = os.getenv("TELEGRAM_BOT_TOKEN")
DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")

if not KEY:
    logging.warning("⚠️ TELEGRAM_BOT_TOKEN порожній — відправки не виконуватимуться.")
BOT = Bot(token=KEY) if KEY else None

PBI_URL = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
PBI_HEADERS = {"Content-Type": "application/json"}

# ---------------------------
# БД
# ---------------------------
def get_unnotified_docs() -> List[tuple[str, str]]:
    """Повертає список (doc_number, period) для записів, де is_notified = FALSE."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT doc_number, period FROM bonus_docs WHERE is_notified = FALSE")
    docs = cur.fetchall()
    cur.close()
    conn.close()
    logging.info(f"📄 Знайдено документів з is_notified=FALSE: {len(docs)}")
    return docs


# ---------------------------
# POWER BI
# ---------------------------
def _exec_pbi_dax(dax: str) -> Dict[str, Any] | None:
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати Power BI токен.")
        return None

    headers = {**PBI_HEADERS, "Authorization": f"Bearer {token}"}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    try:
        r = requests.post(PBI_URL, headers=headers, json=payload, timeout=60)
    except Exception as e:
        logging.error(f"❌ Помилка HTTP при зверненні до PBI: {e}")
        return None

    if r.status_code != 200:
        logging.error(f"❌ Power BI запит не вдався: {r.status_code} — {r.text}")
        return None

    try:
        return r.json()
    except Exception as e:
        logging.error(f"❌ Не вдалося розпарсити JSON від PBI: {e}")
        return None


def fetch_employees_for_doc(doc_number: str) -> List[str]:
    """
    Тягне унікальні імена працівників з BonusesDetails для конкретного DocNumber.
    Повертає список імен (Employee).
    """
    safe_doc = doc_number.replace('"', '""')
    dax = f"""
    EVALUATE
    DISTINCT(
        SELECTCOLUMNS(
            FILTER(BonusesDetails, BonusesDetails[DocNumber] = "{safe_doc}"),
            "Employee", BonusesDetails[Employee]
        )
    )
    """.strip()

    logging.info(f"📤 DAX для {doc_number}:\n{dax}")
    data = _exec_pbi_dax(dax)
    if not data:
        return []

    try:
        rows = data["results"][0]["tables"][0].get("rows", [])
    except Exception:
        rows = []

    logging.info(f"📦 Power BI повернув {len(rows)} рядків для {doc_number}")

    employees: set[str] = set()
    for row in rows:
        # Ключ може мати вигляд "Employee" або "[Employee]" або "BonusesDetails[Employee]"
        key = next((k for k in row if "Employee" in k), None)
        if key and row[key]:
            employees.add(str(row[key]).strip())

    return list(employees)


# ---------------------------
# ВІДПРАВКА У TELEGRAM
# ---------------------------
def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def send_notification(telegram_id: int | str, message: str, retries: int = 2) -> bool:
    """
    Надсилає повідомлення з кількома спробами у випадку тимчасових помилок.
    Повертає True, якщо надіслано.
    """
    if not BOT:
        return False

    chat_id = _to_int(telegram_id)
    if chat_id is None:
        logging.error(f"❌ Невалідний telegram_id: {telegram_id!r}")
        return False

    attempt = 0
    while attempt <= retries:
        try:
            msg = BOT.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode="HTML",
                disable_notification=False,
            )
            mid = getattr(msg, "message_id", None)
            logging.info(f"✅ Надіслано {chat_id}, message_id={mid}")
            return True

        except RetryAfter as e:
            attempt += 1
            wait_s = int(getattr(e, "retry_after", 2))
            logging.warning(f"⏳ FloodWait {wait_s}s для {chat_id} (спроба {attempt}/{retries}).")
            time.sleep(wait_s)

        except Forbidden as e:
            logging.error(f"🚫 Forbidden для {chat_id}: {e} (бот заблокований / немає /start)")
            return False

        except (TimedOut, NetworkError) as e:
            attempt += 1
            logging.warning(f"🌐 Тимчасова помилка для {chat_id}: {e} (спроба {attempt}/{retries}).")
            time.sleep(1)

        except Exception as e:
            logging.error(f"❌ Невідома помилка відправки {chat_id}: {e}")
            return False

    return False


# ---------------------------
# ОСНОВНА ЛОГІКА
# ---------------------------
def _unique(values: Iterable[Any]) -> List[Any]:
    seen = set()
    out = []
    for v in values:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out


def check_bonus_docs():
    """
    1) Беремо всі bonus_docs з is_notified = FALSE
    2) Для кожного DocNumber витягуємо з PBI список Employee
    3) Співставляємо з активними users (по employee_name)
    4) Відправляємо повідомлення тільки знайденим
    5) Позначаємо is_notified лише для документів, де була хоч одна успішна відправка
    """
    logging.info("📥 Перевірка нових бонус-документів...")
    docs_to_check = get_unnotified_docs()
    if not docs_to_check:
        logging.info("ℹ️ Нових документів немає.")
        return

    active_users = get_active_users()
    logging.info(f"🟢 Активних користувачів у базі: {len(active_users)}")

    # мапимо за ім’ям
    active_by_name: Dict[str, Dict[str, Any]] = {
        str(u["employee_name"]).strip(): u for u in active_users
    }

    docs_to_mark: List[str] = []

    for doc_number, period in docs_to_check:
        logging.info(f"🔍 Обробка документа: {doc_number} — {period}")

        employees = fetch_employees_for_doc(doc_number)
        if not employees:
            logging.warning(f"⚠️ PBI не повернув жодного співробітника для {doc_number}")
            continue

        matched: List[Dict[str, Any]] = []
        for emp in employees:
            emp_clean = str(emp).strip()
            user = active_by_name.get(emp_clean)
            if user and user.get("telegram_id"):
                matched.append(user)
                logging.info(f"✅ {emp_clean}: знайдено в БД → telegram_id={user['telegram_id']}")
            elif user and not user.get("telegram_id"):
                logging.warning(f"⚠️ {emp_clean}: немає telegram_id у БД")
            else:
                logging.warning(f"⚠️ {emp_clean}: відсутній серед активних у БД")

        if not matched:
            logging.warning(f"⚠️ {doc_number}: немає кому надіслати — статус НЕ оновлюю.")
            continue

        message = (
            "📄 Зʼявився новий документ нарахування бонусів:\n"
            f"• Номер: <b>{doc_number}</b>\n"
            f"• Період: <b>{period}</b>"
        )

        sent_any = False
        # відправляємо лише унікальним telegram_id
        for tg_id in _unique(u["telegram_id"] for u in matched if u.get("telegram_id")):
            ok = send_notification(tg_id, message)
            sent_any = sent_any or ok
            time.sleep(0.03)  # легкий тротлінг проти флад-лімітів

        if sent_any:
            docs_to_mark.append(doc_number)
        else:
            logging.warning(f"⚠️ {doc_number}: усі відправки невдалі — статус НЕ оновлюю.")

    if docs_to_mark:
        affected = mark_bonus_docs_notified(docs_to_mark)
        logging.info(f"✅ Оновлено is_notified по надісланих документах: {affected}")
    else:
        logging.info("ℹ️ Жоден документ не оновлено (успішних відправок не було).")



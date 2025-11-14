import os
import time
import requests

from auth import get_power_bi_token
from db import get_db_connection, mark_bonus_docs_notified, get_active_users

KEY = os.getenv("TELEGRAM_BOT_TOKEN")
DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")
TG_API = f"https://api.telegram.org/bot{KEY}/sendMessage"


# --- helpers ---
def get_unnotified_docs():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT doc_number, period FROM bonus_docs WHERE is_notified = FALSE")
    rows = cur.fetchall()
    cur.close(); conn.close()
    if rows:
        print(f"📦 Отримано {len(rows)} нових документ(ів) з БД")
    return rows


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
        return []

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code != 200:
        return []

    rows = r.json()["results"][0]["tables"][0].get("rows", [])
    if rows:
        print(f"📊 Power BI повернув {len(rows)} співробітників для документа {doc_number}")
    emps = set()
    for row in rows:
        k = next((k for k in row if "Employee" in k), None)
        if k and row[k]:
            emps.add(str(row[k]).strip())
    return list(emps)


def _send(telegram_id: int | str, text: str) -> bool:
    if not KEY:
        return False
    try:
        chat_id = int(telegram_id)
    except Exception:
        return False
    r = requests.post(
        TG_API,
        data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )
    if r.status_code == 200 and r.json().get("ok"):
        return True
    if r.status_code == 429:  # flood control
        wait = int(r.json().get("parameters", {}).get("retry_after", 2))
        time.sleep(wait)
        r = requests.post(
            TG_API,
            data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=15,
        )
        return r.status_code == 200 and r.json().get("ok")
    return False


# --- main ---
def check_bonus_docs():
    docs = get_unnotified_docs()
    if not docs:
        return

    active_map = {str(u["employee_name"]).strip(): u for u in get_active_users()}
    if active_map:
        print(f"🟢 Активних користувачів у базі: {len(active_map)}")

    docs_to_mark = []

    for doc_number, period in docs:
        employees = fetch_employees_for_doc(doc_number)
        if not employees:
            continue

        msg = (
            "📄 Зʼявився новий документ <b>Нарахування бонусів</b>:\n"
            f"• Номер: <b>{doc_number}</b>\n"
            f"• Період: <b>{period}</b>\n\n"
            "Цей документ доступний для перегляду. Для цього перейдіть у розділ <b>Зарплата</b> => <b>Бонуси</b> або <b>Відомість Бонусів</b>, "
            "де ви зможете перевірити нарахування бонусів за вказаний період.\n\n"
            "Якщо у вас виникли запитання або зауваження щодо цього документа, будь ласка, повідомте відповідального менеджера.\n\n"
            "Дякуємо за вашу увагу та співпрацю! 😊"
        )


        sent_any = False
        seen_ids = set()

        for emp in employees:
            u = active_map.get(emp)
            if not u:
                continue
            tg_id = u.get("telegram_id")
            if not tg_id or tg_id in seen_ids:
                continue

            if _send(tg_id, msg):
                print(f"✅ Відправлено: {emp} (tg:{tg_id})")
                sent_any = True
                seen_ids.add(tg_id)

        if sent_any:
            docs_to_mark.append(doc_number)

    if docs_to_mark:
        mark_bonus_docs_notified(docs_to_mark)
        print(f"✅ Оновлено is_notified для {len(docs_to_mark)} документ(ів)")

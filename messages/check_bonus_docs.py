import os
import time
import requests

from auth import get_power_bi_token
from db import get_db_connection, mark_bonus_docs_notified, get_active_users

KEY = os.getenv("TELEGRAM_BOT_TOKEN")
DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")
TG_API = f"https://api.telegram.org/bot{KEY}/sendMessage"


# --- helpers you already have (kept unchanged/compact) ---
def get_unnotified_docs():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT doc_number, period FROM bonus_docs WHERE is_notified = FALSE")
    rows = cur.fetchall()
    cur.close(); conn.close()
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
    emps = set()
    for row in rows:
        k = next((k for k in row if "Employee" in k), None)
        if k and row[k]:
            emps.add(str(row[k]).strip())
    return list(emps)


# --- simple HTTP send (no extra logs) ---
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


# --- the function you asked to rewrite (only prints recipient lines) ---
def check_bonus_docs():
    docs = get_unnotified_docs()
    if not docs:
        return

    # employee_name -> user(row) with telegram_id
    active_map = {str(u["employee_name"]).strip(): u for u in get_active_users()}

    docs_to_mark = []

    for doc_number, period in docs:
        employees = fetch_employees_for_doc(doc_number)
        if not employees:
            continue

        # message once per doc
        msg = (
            "📄 Зʼявився новий документ нарахування бонусів:\n"
            f"• Номер: <b>{doc_number}</b>\n"
            f"• Період: <b>{period}</b>"
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

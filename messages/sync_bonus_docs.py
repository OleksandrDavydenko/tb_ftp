import os
import requests
from auth import get_power_bi_token
from db import bulk_add_bonus_docs

DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")

DAX_QUERY = """
EVALUATE
SUMMARIZE(
    'BonusesDetails',
    'BonusesDetails'[DocNumber],
    'BonusesDetails'[Period]
)
ORDER BY 'BonusesDetails'[Period], 'BonusesDetails'[DocNumber]
"""

def _normalize_period(p):
    if p is None:
        return None
    s = str(p)
    if "T" in s:
        s = s.split("T", 1)[0]  # 'YYYY-MM-DD'
    return s

async def sync_bonus_docs():
    token = get_power_bi_token()
    if not token:
        return

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": DAX_QUERY}], "serializerSettings": {"includeNulls": True}}

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code != 200:
        return

    data = r.json()
    rows = data["results"][0]["tables"][0].get("rows", [])

    # існуючі номери документів у БД
    existing = get_existing_bonus_doc_numbers()

    to_insert = []
    for row in rows:
        # підтримує ключі на кшталт 'BonusesDetails[DocNumber]' / 'BonusesDetails[Period]'
        k_doc = next((k for k in row if "DocNumber" in k), None)
        k_per = next((k for k in row if "Period" in k), None)
        doc = row.get(k_doc)
        per = _normalize_period(row.get(k_per))
        if not doc or not per:
            continue
        # додаємо ТІЛЬКИ якщо такого DocNumber ще немає у БД
        if str(doc) not in existing:
            to_insert.append((str(doc), per))

    if not to_insert:
        return

    bulk_add_bonus_docs(to_insert)

import os
import requests
from auth import get_power_bi_token
from db import bulk_add_swift_payments, get_existing_swift_payment_keys

DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")

# Синхронізуємо і відправляємо лише платежі, створені починаючи з цієї дати
SYNC_START_DATE = "2026-07-28"

DAX_QUERY = """
EVALUATE
FILTER(
    'telegram_swift_payment_info',
    'telegram_swift_payment_info'[DocumentDate] >= DATE(2026, 7, 28)
)
"""

# Назви колонок у PBI-таблиці telegram_swift_payment_info
COLUMNS = [
    "DocumentNumber",
    "DocumentDate",
    "Currency",
    "AmountInCurrency",
    "AmountInUSD",
    "Counterparty",
    "PaymentType",
    "AccountInLocalCurrency",
    "HasSwift",
    "Employee",
]


def _get(row, col_name):
    # ключі мають вигляд 'telegram_swift_payment_info[DocumentNumber]'
    k = next((k for k in row if col_name in k), None)
    return row.get(k) if k else None


def _normalize_date(d):
    if d is None:
        return None
    # PBI повертає ISO-рядок типу '2026-07-24T10:30:00' (іноді з мілісекундами чи 'Z')
    s = str(d).strip().rstrip("Z")[:19]
    return s or None


async def sync_swift_payments():
    token = get_power_bi_token()
    if not token:
        return

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": DAX_QUERY}], "serializerSettings": {"includeNulls": True}}

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code != 200:
        print(f"❌ SWIFT sync: Power BI повернув статус {r.status_code}")
        return

    data = r.json()
    rows = data["results"][0]["tables"][0].get("rows", [])

    # існуючі ключі (doc_number, doc_date) у БД
    existing = get_existing_swift_payment_keys()

    to_insert = []
    for row in rows:
        values = {col: _get(row, col) for col in COLUMNS}
        doc_number = values["DocumentNumber"]
        doc_date = _normalize_date(values["DocumentDate"])
        if not doc_number or not doc_date:
            continue

        # захисний фільтр: не беремо документи, старіші за дату старту
        if doc_date[:10] < SYNC_START_DATE:
            continue

        key = (str(doc_number), doc_date)
        if key in existing:
            continue

        to_insert.append((
            str(doc_number),
            doc_date,
            values["Currency"],
            values["AmountInCurrency"],
            values["AmountInUSD"],
            values["Counterparty"],
            values["PaymentType"],
            values["AccountInLocalCurrency"],
            values["HasSwift"],
            str(values["Employee"]).strip() if values["Employee"] else None,
        ))

    if not to_insert:
        return

    inserted = bulk_add_swift_payments(to_insert)
    print(f"✅ SWIFT sync: додано {inserted} нових платежів")

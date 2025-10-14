# messages/sync_bonus_docs.py

import os
import json
import logging
import requests
from datetime import datetime
from auth import get_power_bi_token
from db import bulk_add_bonus_docs  # або add_bonus_doc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    """Очікуємо дату типу '2025-01-01T00:00:00' або '2025-01-01' → повернемо 'YYYY-MM-DD'."""
    if p is None:
        return None
    s = str(p)
    if "T" in s:
        s = s.split("T", 1)[0]
    return s

async def sync_bonus_docs():
    """
    Тягне DocNumber+Period з Power BI і записує у bonus_docs.
    За замовчуванням is_notified = FALSE (на рівні таблиці і вставки).
    """
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати токен Power BI.")
        return

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": DAX_QUERY}], "serializerSettings": {"includeNulls": True}}

    logging.info(f"➡️ POST {url}")
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        logging.info(f"⬅️ PBI status: {r.status_code}")
        if r.status_code != 200:
            # виведемо до 1000 символів тіла для діагностики
            logging.error(f"Body: {r.text[:1000]}")
            return

        data = r.json()
        # обережно логнемо компактно (не все тіло)
        try:
            # покажемо структуру results/tables/rows без перевантаження логів
            res = data.get("results", [])
            if not res:
                logging.warning("⚠️ results порожній у відповіді.")
            else:
                tbls = res[0].get("tables", [])
                if not tbls:
                    logging.warning("⚠️ results[0].tables порожній у відповіді.")
                else:
                    rows = tbls[0].get("rows", [])
                    logging.info(f"🔢 Отримано рядків: {len(rows)}")
                    if rows:
                        first_keys = list(rows[0].keys())
                        logging.info(f"🔑 Ключі першого рядка: {first_keys}")
                        logging.info(f"🧩 Перший рядок: {json.dumps(rows[0], ensure_ascii=False)[:500]}")
        except Exception as ee:
            logging.warning(f"Не вдалось красиво залогувати data: {ee}")

        # Тепер реально витягнемо rows
        rows = data["results"][0]["tables"][0].get("rows", [])

        # Парсимо з урахуванням префіксів 'BonusesDetails[DocNumber]' / 'BonusesDetails[Period]'
        to_insert = []
        for row in rows:
            k_doc = next((k for k in row.keys() if k.endswith("[DocNumber]") or "DocNumber" in k), None)
            k_per = next((k for k in row.keys() if k.endswith("[Period]")    or "Period"    in k), None)

            doc = row.get(k_doc)
            per = _normalize_period(row.get(k_per))

            if doc and per:
                to_insert.append((str(doc), per))

        logging.info(f"🧾 Підготовлено до вставки: {len(to_insert)}")
        if to_insert:
            logging.info(f"👀 Перші 5 до вставки: {to_insert[:5]}")

        inserted = bulk_add_bonus_docs(to_insert)
        logging.info(f"✅ Синхронізовано документів бонусів (нових): {inserted}")

    except requests.RequestException as e:
        logging.exception(f"❌ HTTP помилка запиту до PBI: {e}")
    except Exception as e:
        logging.exception(f"❌ Помилка синхронізації bonus_docs: {e}")



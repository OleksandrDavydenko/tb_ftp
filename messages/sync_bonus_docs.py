# messages/sync_bonus_docs.py

import requests
import logging
from auth import get_power_bi_token
from db import bulk_add_bonus_docs  # або add_bonus_doc
import os

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

async def sync_bonus_docs():
    """
    Тягне DocNumber+Period з Power BI і записує у bonus_docs.
    За замовчуванням is_notified = FALSE (на рівні таблиці і вставки).
    """
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    payload = {
        "queries": [{"query": DAX_QUERY}],
        "serializerSettings": {"includeNulls": True}
    }

    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code != 200:
            logging.error(f"Помилка PBI {r.status_code}: {r.text}")
            return

        data = r.json()
        rows = data["results"][0]["tables"][0].get("rows", [])

        # Очистимо ключі від квадратних дужок і спакуємо пари (doc_number, period)
        to_insert = []
        for row in rows:
            # У відповіді ключі зазвичай мають вигляд "[DocNumber]" / "[Period]"
            doc = row.get("[DocNumber]") or row.get("DocNumber")
            per = row.get("[Period]") or row.get("Period")

            if doc and per:
                # Нормалізація period (опціонально). Якщо у вас "YYYY-MM", залишаємо як є.
                to_insert.append((str(doc), str(per)))

        # Масово додаємо (is_notified = FALSE за замовчуванням у таблиці)
        bulk_add_bonus_docs(to_insert)
        logging.info(f"✅ Синхронізовано документів бонусів: {len(to_insert)}")

    except Exception as e:
        logging.exception(f"❌ Помилка синхронізації bonus_docs: {e}")

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
DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")


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
import os
import logging
import requests
from auth import get_power_bi_token

DATASET_ID = os.getenv("PBI_DATASET_ID")
BONUSES_TBL = os.getenv("PBI_BONUSES_TABLE", "").strip()  # ← якщо задано, будемо використовувати її

def fetch_employees_for_doc(doc_number: str) -> list[str]:
    """
    Повертає унікальних Employee для DocNumber, пробуючи кілька варіантів DAX.
    Якщо у середовищі задано PBI_BONUSES_TABLE — використовуємо її напряму.
    """
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати Power BI токен (перевір PASSWORD / auth.py).")
        return []

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    safe_doc = doc_number.replace('"', '""')

    # якщо таблицю явно дали через ENV — формуємо рівно по ній
    if BONUSES_TBL:
        dax_env = f"""
        EVALUATE
        DISTINCT(
            SELECTCOLUMNS(
                FILTER('{BONUSES_TBL}', '{BONUSES_TBL}'[DocNumber] = "{safe_doc}"),
                "Employee", '{BONUSES_TBL}'[Employee]
            )
        )
        """
        return _try_dax(url, headers, dax_env, label=f"ENV '{BONUSES_TBL}'")

    # інакше — пробуємо декілька поширених варіантів
    variants = [
        # 1) без лапок (як у тебе локально)
        """
        EVALUATE
        DISTINCT(
            SELECTCOLUMNS(
                FILTER(BonusesDetails, BonusesDetails[DocNumber] = "{doc}"),
                "Employee", BonusesDetails[Employee]
            )
        )
        """,
        # 2) з лапками навколо назви таблиці
        """
        EVALUATE
        DISTINCT(
            SELECTCOLUMNS(
                FILTER('BonusesDetails', 'BonusesDetails'[DocNumber] = "{doc}"),
                "Employee", 'BonusesDetails'[Employee]
            )
        )
        """,
        # 3) SUMMARIZECOLUMNS — інколи стабільніше
        """
        EVALUATE
        SUMMARIZECOLUMNS(
            'BonusesDetails'[Employee],
            FILTER('BonusesDetails', 'BonusesDetails'[DocNumber] = "{doc}")
        )
        """,
        # 4) На випадок іншої назви таблиці (поширені варіанти)
        """
        EVALUATE
        DISTINCT(
            SELECTCOLUMNS(
                FILTER('Bonuses Details', 'Bonuses Details'[DocNumber] = "{doc}"),
                "Employee", 'Bonuses Details'[Employee]
            )
        )
        """,
        """
        EVALUATE
        DISTINCT(
            SELECTCOLUMNS(
                FILTER('3330/3320', '3330/3320'[DocNumber] = "{doc}"),
                "Employee", '3330/3320'[Employee]
            )
        )
        """,
    ]

    for i, tpl in enumerate(variants, start=1):
        dax = tpl.format(doc=safe_doc)
        res = _try_dax(url, headers, dax, label=f"variant #{i}")
        if res:
            return res

    logging.error(
        "❌ Усі варіанти DAX завершилися помилкою/пусто. "
        "Ймовірно, у датасеті інша назва таблиці або колонок. "
        "Можеш задати її явно через ENV PBI_BONUSES_TABLE."
    )
    return []


def _try_dax(url: str, headers: dict, dax: str, label: str) -> list[str] | None:
    logging.info(f"📤 DAX {label}:\n{dax.strip()}")
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
    except Exception as e:
        logging.error(f"❌ HTTP помилка ({label}): {e}")
        return None

    if r.status_code != 200:
        # важливо бачити ТЕКСТ помилки 400 — там часто прямо написано, якої таблиці/колонки не існує
        logging.error(f"❌ PBI {label}: {r.status_code} — {r.text}")
        return None

    try:
        data = r.json()
        rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
        logging.info(f"📦 {label}: отримано {len(rows)} рядків")
        employees = set()

        for row in rows:
            key_emp = next((k for k in row if "Employee" in k), None)
            if not key_emp:
                continue
            val = row.get(key_emp)
            if val:
                employees.add(str(val).strip())

        if employees:
            logging.info(f"👥 {label}: унікальні Employee: {list(employees)}")
            return list(employees)
        else:
            logging.warning(f"⚠️ {label}: співробітників не знайдено.")
            return []
    except Exception as e:
        logging.error(f"❌ Парсинг JSON ({label}): {e}")
        return None

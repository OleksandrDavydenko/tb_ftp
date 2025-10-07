# messages/sync_payments_single_query.py
import requests
import psycopg2
import os
import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple

from auth import get_power_bi_token, normalize_phone_number
from db import add_payment

__all__ = ["sync_payments_single_query"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DATABASE_URL = os.getenv("DATABASE_URL")

DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")
PBI_URL = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"

# Скільки активних користувачів пакуємо в один запит до Power BI
BATCH_SIZE = 120


# ---------------- DB helpers ----------------
def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def fetch_db_payments(phone_number: str, payment_number: str):
    """
    Повертає множину записів по конкретній платіжці:
    {(amount_str_2dec, currency, payment_date_iso, accrual_month)}
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT amount, currency, payment_date, accrual_month
        FROM payments
        WHERE phone_number = %s AND payment_number = %s
        """,
        (phone_number, payment_number),
    )
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return set(
        (f"{float(r[0]):.2f}", r[1], r[2].strftime("%Y-%m-%d"), (r[3] or "").strip())
        for r in records
    )


def delete_payment_records(phone_number: str, payment_number: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            DELETE FROM payments
            WHERE phone_number = %s AND payment_number = %s
            """,
            (phone_number, payment_number),
        )
        conn.commit()
        logging.info(f"🧹 Видалено старі записи по платіжці {payment_number} для {phone_number}")
    except Exception as e:
        logging.error(f"❌ Помилка при видаленні: {e}")
    finally:
        cursor.close()
        conn.close()


# ---------------- Power BI query builder ----------------
def _dax_escape(s: str) -> str:
    """Екранування лапок у DAX-рядках."""
    return (s or "").replace('"', '""')


def _build_users_union_rows(rows: List[Tuple[str, str, datetime]]) -> str:
    """
    Будує вираз UNION( ROW("Employee","..","JoinedAt",DATE(...)), ... )
    rows: list of tuples (phone_number, employee_name, joined_at)
    """
    row_exprs = []
    for _, emp, joined_at in rows:
        if not emp or not joined_at:
            continue
        emp_esc = _dax_escape(emp)
        dt = joined_at.date() if hasattr(joined_at, "date") else joined_at
        y, m, d = dt.year, dt.month, dt.day
        # головна зміна: DATE замість DATETIME
        row_exprs.append(
            f'ROW("Employee","{emp_esc}","JoinedAt",DATE({y},{m},{d}))'
        )
    if not row_exprs:
        return "FILTER(SalaryPayment, FALSE())"
    if len(row_exprs) == 1:
        return row_exprs[0]
    return f"UNION(\n        " + ",\n        ".join(row_exprs) + "\n    )"



def _build_dax_for_batch(batch_rows: List[Tuple[str, str, datetime]]) -> str:
    """
    DAX: натурал-джойн SalaryPayment з Users (Employee, JoinedAt),
    далі фільтр DocDate >= JoinedAt, і потрібні колонки.
    """
    users_tbl = _build_users_union_rows(batch_rows)
    dax = f"""
EVALUATE
VAR Users =
    {users_tbl}
VAR J =
    NATURALINNERJOIN(
        Users,
        SELECTCOLUMNS(
            SalaryPayment,
            "Employee",            SalaryPayment[Employee],
            "DocDate",             SalaryPayment[DocDate],
            "DocNumber",           SalaryPayment[DocNumber],
            "SUM_UAH",             SalaryPayment[SUM_UAH],
            "SUM_USD",             SalaryPayment[SUM_USD],
            "AccrualMonth",        SalaryPayment[МісяцьНарахування]
        )
    )
VAR F =
    FILTER(J, [DocDate] >= [JoinedAt])
RETURN
SELECTCOLUMNS(
    F,
    "Employee",           [Employee],
    "Дата платежу",       [DocDate],
    "Документ",           [DocNumber],
    "Сума UAH",           [SUM_UAH],
    "Сума USD",           [SUM_USD],
    "МісяцьНарахування",  [AccrualMonth]
)
"""
    return dax.strip()


# ---------------- Main sync (single-query batches) ----------------
def sync_payments_single_query():
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати токен Power BI.")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # 1) Заберемо активних юзерів і зробимо мапу employee -> phone
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT phone_number, employee_name, joined_at
        FROM users
        WHERE status = 'active'
        """
    )
    users = cursor.fetchall()
    cursor.close()
    conn.close()

    # нормалізуємо телефони і фільтруємо пусті імена/дати
    cleaned: List[Tuple[str, str, datetime]] = []
    emp_to_phone: dict[str, str] = {}
    for phone, emp, j_at in users:
        if not emp or not j_at:
            continue
        phone_norm = normalize_phone_number(phone)
        emp_to_phone[emp] = phone_norm
        cleaned.append((phone_norm, emp, j_at))

    if not cleaned:
        logging.info("ℹ️ Немає активних користувачів для синхронізації.")
        return

    # 2) Батчимо, щоб не перевищувати ліміт розміру запиту
    total_rows = 0
    for i in range(0, len(cleaned), BATCH_SIZE):
        batch = cleaned[i : i + BATCH_SIZE]
        dax = _build_dax_for_batch(batch)

        body = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}

        try:
            resp = requests.post(PBI_URL, headers=headers, json=body, timeout=60)
            if resp.status_code != 200:
                logging.error(f"❌ Power BI error: {resp.status_code} | {resp.text}")
                continue

            data = resp.json()
            rows = data["results"][0]["tables"][0].get("rows", [])
            total_rows += len(rows)

            # 3) Групуємо по (Employee, Документ), щоб не плутатись, якщо DocNumber не унікальний глобально
            grouped = defaultdict(list)
            for r in rows:
                emp = (r.get("[Employee]") or "").strip()
                doc = (r.get("[Документ]") or "").strip()
                if not emp or not doc:
                    continue
                grouped[(emp, doc)].append(r)

            # 4) Порівнюємо з БД та оновлюємо
            for (emp, payment_number), payments in grouped.items():
                phone_number = emp_to_phone.get(emp)
                if not phone_number:
                    # На випадок, якщо в батч просочилось ім'я без відповідного телефону
                    logging.warning(f"⚠️ Немає телефону для співробітника '{emp}', пропускаю платіж {payment_number}")
                    continue

                # множина з Power BI
                bi_set = set()
                for p in payments:
                    usd = float(p.get("[Сума USD]", 0) or 0)
                    uah = float(p.get("[Сума UAH]", 0) or 0)
                    use_usd = abs(usd) > 0
                    amount = usd if use_usd else uah
                    currency = "USD" if use_usd else "UAH"
                    # DocDate приходить як ISO з часом — відрізамо дату
                    payment_date = str(p.get("[Дата платежу]", "")).split("T")[0]
                    accrual_month = (p.get("[МісяцьНарахування]", "") or "").strip()
                    bi_set.add((f"{amount:.2f}", currency, payment_date, accrual_month))

                # множина з БД
                db_set = fetch_db_payments(phone_number, payment_number)

                if bi_set != db_set:
                    delete_payment_records(phone_number, payment_number)
                    for amount, currency, payment_date, accrual_month in bi_set:
                        try:
                            add_payment(
                                phone_number,
                                float(amount),
                                currency,
                                payment_date,
                                payment_number,
                                accrual_month,
                                False,
                            )
                            logging.info(
                                f"✅ Додано/оновлено платіж: {phone_number} | {amount} {currency} | {accrual_month} | № {payment_number}"
                            )
                        except Exception as e:
                            logging.error(f"❌ Помилка при додаванні: {e}")
                else:
                    logging.info(f"⏭️ Платіж {payment_number} для {phone_number} без змін")

        except Exception as e:
            logging.error(f"❌ Помилка при виконанні батчу [{i}:{i+len(batch)}]: {e}")

    logging.info(f"🔄 Синхронізовано {total_rows} рядків сумарно по {len(cleaned)} користувачах (батчами).")

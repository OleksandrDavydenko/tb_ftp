import requests
import psycopg2
import os
import logging
from datetime import datetime
from auth import get_power_bi_token, normalize_phone_number
from db import add_payment

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def fetch_active_users():
    """phone_number, employee_name, joined_at"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT phone_number, employee_name, joined_at
        FROM users
        WHERE status = 'active'
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def fetch_db_payments_bulk(conn, phone_numbers):
    """
    Витягує всі платежі для набору телефонів однією вибіркою.
    Повертає мапу: { phone -> { payment_number -> set((amount, currency, date, accrual_month)) } }
    """
    if not phone_numbers:
        return {}

    cur = conn.cursor()
    cur.execute("""
        SELECT phone_number, payment_number, amount, currency, payment_date, accrual_month
        FROM payments
        WHERE phone_number = ANY(%s)
    """, (list(phone_numbers),))
    result = {}
    for phone, paynum, amount, currency, paydate, accrual_month in cur.fetchall():
        phone = normalize_phone_number(phone)
        dct = result.setdefault(phone, {})
        st = dct.setdefault(str(paynum), set())
        st.add((
            f"{float(amount):.2f}",
            currency,
            paydate.strftime('%Y-%m-%d'),
            (accrual_month or "").strip()
        ))
    cur.close()
    return result

def delete_payment_records_conn(conn, phone_number, payment_number):
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM payments
        WHERE phone_number = %s AND payment_number = %s
    """, (phone_number, payment_number))
    conn.commit()
    cur.close()
    logging.info(f"🧹 Видалено старі записи по платіжці {payment_number} для {phone_number}")

def _dax_escape(s: str) -> str:
    """Екранує подвійні лапки для DAX-рядків."""
    return (s or "").replace('"', '""')

def build_one_dax_query(active_users):
    """
    active_users: iterable of (phone_number, employee_name, joined_at)
    Формує один DAX, що:
      - будує DATATABLE(Employee, JoinedAt) з активних користувачів;
      - підтягує лише рядки SalaryPayment для цих співробітників починаючи з їх JoinedAt.
    """
    # Рядки DATATABLE: { "Emp Name", DATE(YYYY,MM,DD) }
    rows = []
    for _, emp, joined_at in active_users:
        emp_esc = _dax_escape(emp)
        dt = joined_at.date() if hasattr(joined_at, "date") else joined_at
        rows.append(f'{{ "{emp_esc}", DATE({dt.year},{dt.month},{dt.day}) }}')

    datatable_rows = ",\n        ".join(rows)

    dax = f"""
EVALUATE
VAR Users =
    DATATABLE(
        "Employee", STRING,
        "JoinedAt", DATE,
        {{
        {datatable_rows}
        }}
    )
VAR WithJ =
    ADDCOLUMNS(
        SalaryPayment,
        "JoinedAtX",
            LOOKUPVALUE(Users[JoinedAt], Users[Employee], SalaryPayment[Employee])
    )
VAR F =
    FILTER(
        WithJ,
        NOT ISBLANK([JoinedAtX]) && SalaryPayment[DocDate] >= [JoinedAtX]
    )
RETURN
SELECTCOLUMNS(
    F,
    "Employee",           SalaryPayment[Employee],
    "Дата платежу",       SalaryPayment[DocDate],
    "Документ",           SalaryPayment[DocNumber],
    "Сума UAH",           SalaryPayment[SUM_UAH],
    "Сума USD",           SalaryPayment[SUM_USD],
    "МісяцьНарахування",  SalaryPayment[МісяцьНарахування]
)
"""
    return dax

def sync_payments_single_query():
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати токен Power BI.")
        return

    # 1) Витягуємо активних юзерів
    users = fetch_active_users()
    if not users:
        logging.info("ℹ️ Немає активних користувачів.")
        return

    # Мапи для подальшого групування
    emp_to_phone = {}
    for phone, emp, _ in users:
        emp_to_phone[emp] = normalize_phone_number(phone)

    # 2) Будуємо один DAX
    dax_query = build_one_dax_query(users)

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    body = {
        "queries": [{"query": dax_query}],
        "serializerSettings": {"includeNulls": True}
    }

    # 3) Один HTTP POST до Power BI
    session = requests.Session()
    resp = session.post(power_bi_url, headers=headers, json=body, timeout=120)
    if resp.status_code != 200:
        logging.error(f"❌ Power BI error: {resp.status_code} | {resp.text}")
        return

    data = resp.json()
    rows = data['results'][0]['tables'][0].get('rows', [])

    # 4) Групуємо все з Power BI: phone -> payment_number -> set(...)
    bi_by_phone = {}
    for r in rows:
        emp = (r.get("[Employee]") or "").strip()
        phone = emp_to_phone.get(emp)
        if not phone:
            # захист від неспівпадінь імен, просто пропустимо
            continue

        doc = str(r.get("[Документ]", "") or "")
        usd = float(r.get("[Сума USD]", 0) or 0)
        uah = float(r.get("[Сума UAH]", 0) or 0)
        use_usd = abs(usd) > 0
        amount = usd if use_usd else uah
        currency = "USD" if use_usd else "UAH"
        pay_date = str(r.get("[Дата платежу]", "")).split("T")[0]
        accrual = (r.get("[МісяцьНарахування]", "") or "").strip()

        dct = bi_by_phone.setdefault(phone, {})
        st = dct.setdefault(doc, set())
        st.add((f"{amount:.2f}", currency, pay_date, accrual))

    # 5) Тягнемо всі наявні записи з БД одним запитом
    conn = get_db_connection()
    db_all = fetch_db_payments_bulk(conn, set(emp_to_phone.values()))

    # 6) Порівнюємо і синхронізуємо (логіка як раніше)
    total_changed = 0
    for phone, docs in bi_by_phone.items():
        existing_docs = db_all.get(phone, {})
        for payment_number, bi_set in docs.items():
            db_set = existing_docs.get(payment_number, set())
            if bi_set != db_set:
                delete_payment_records_conn(conn, phone, payment_number)
                for amount, currency, payment_date, accrual_month in bi_set:
                    try:
                        # add_payment всередині використовує своє підключення — як і було
                        add_payment(phone, float(amount), currency, payment_date, payment_number, accrual_month, False)
                        logging.info(f"✅ Додано платіж: {phone} | {amount} {currency} | {accrual_month} | № {payment_number}")
                    except Exception as e:
                        logging.error(f"❌ Помилка при додаванні: {e}")
                total_changed += 1
            else:
                logging.info(f"⏭️ Платіж {payment_number} для {phone} без змін")

    conn.close()
    logging.info(f"🔄 Синхронізовано {len(rows)} рядків (оновлено платіжок: {total_changed})")

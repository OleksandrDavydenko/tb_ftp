import psycopg2
import os
import logging
import requests
from collections import defaultdict
from telegram import Bot
from datetime import datetime
from auth import get_power_bi_token  # <-- додаємо токен для PBI

KEY = os.getenv('TELEGRAM_BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

# Мапи назв типів
TYPE_UA = {
    'salary': 'Оклад',
    'bonus':  'Бонус',
    'prize':  'Премія',
}

# У родовому відмінку для фрази “виплата Окладу/Бонусу/Премії”
TYPE_GENITIVE_UA = {
    'salary': 'Окладу',
    'bonus':  'Бонусу',
    'prize':  'Премії',
}

MONTHS_UA = {
    "01": "Січень", "02": "Лютий", "03": "Березень", "04": "Квітень",
    "05": "Травень", "06": "Червень", "07": "Липень", "08": "Серпень",
    "09": "Вересень", "10": "Жовтень", "11": "Листопад", "12": "Грудень"
}

def _fmt_amount(amount: float) -> str:
    return f"{amount:,.2f}".replace(",", " ")

def _format_month(raw_month: str) -> str:
    # Підтримуємо 'YYYY-MM-DD' і 'YYYY-MM'
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            parsed = datetime.strptime(raw_month, fmt)
            return MONTHS_UA.get(f"{parsed.month:02d}", raw_month)
        except ValueError:
            continue
    return raw_month

def _fetch_pbi_payment_lines(employee_name: str, payment_number: str):
    """
    Тягнемо з Power BI деталізацію по документу:
    - Місяць нарахування
    - Сума UAH/USD
    - Тип (character): salary/bonus/prize
    Повертаємо список рядків-словників або None, якщо не вдалося.
    """
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати токен Power BI у check_new_payments.")
        return None

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Назви колонок у відповіді беруться в квадратні дужки.
    # Важливо: у вашій моделі таблиці SalaryPayment має бути колонка з типом (character/Type).
    # Якщо поле називається інакше — замініть SalaryPayment[character] нижче.
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            SalaryPayment,
                            SalaryPayment[Employee] = "{employee_name}" &&
                            SalaryPayment[DocNumber] = "{payment_number}"
                        ),
                        "МісяцьНарахування", SalaryPayment[МісяцьНарахування],
                        "Сума UAH",          SalaryPayment[SUM_UAH],
                        "Сума USD",          SalaryPayment[SUM_USD],
                        "Тип",               SalaryPayment[character]
                    )
                """
            }
        ],
        "serializerSettings": {"includeNulls": True}
    }

    try:
        resp = requests.post(power_bi_url, headers=headers, json=query_data, timeout=30)
        if resp.status_code != 200:
            logging.error(f"❌ Power BI error {resp.status_code}: {resp.text}")
            return None
        data = resp.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        return rows
    except Exception as e:
        logging.error(f"❌ Помилка запиту до Power BI: {e}")
        return None

async def check_new_payments():
    logging.info("Перевірка нових платежів розпочата.")
    conn = get_db_connection()
    cursor = conn.cursor()

    # Крок 1: знайти унікальні пари payment_number + phone_number
    cursor.execute("""
    SELECT DISTINCT payment_number, phone_number
    FROM payments
    WHERE is_notified = FALSE
    """)
    payment_groups = cursor.fetchall()

    for payment_number, phone_number in payment_groups:
        # Крок 1.1: дістаємо employee_name (потрібен для фільтру в Power BI)
        cursor.execute("SELECT employee_name, telegram_id FROM users WHERE phone_number = %s", (phone_number,))
        u = cursor.fetchone()
        if not u:
            logging.warning(f"Не знайдено користувача (employee_name/telegram_id) для номера: {phone_number}")
            continue
        employee_name, telegram_id = u[0], u[1]

        # Крок 2: дістати записи із БД по цій парі (для fallback і для валюти/дати)
        cursor.execute("""
        SELECT phone_number, amount, currency, payment_date, payment_number, accrual_month
        FROM payments
        WHERE payment_number = %s AND phone_number = %s
        """, (payment_number, phone_number))
        payments = cursor.fetchall()
        if not payments:
            continue

        # Базові атрибути з БД (на випадок, якщо PBI недоступний)
        currency = payments[0][2]
        payment_date = payments[0][3]

        # ---- Основний шлях: отримати з Power BI типи + суми по періодах ----
        pbi_rows = _fetch_pbi_payment_lines(employee_name, payment_number)

        if pbi_rows:
            # Агрегуємо суми за (тип -> місяць)
            amounts_by_type_and_month = defaultdict(lambda: defaultdict(float))
            total_by_type = defaultdict(float)
            any_currency = None
            for r in pbi_rows:
                # Ключі у відповіді з дужками
                month_raw = (r.get("[МісяцьНарахування]", "") or "").strip()
                amount_uah = float(r.get("[Сума UAH]", 0) or 0)
                amount_usd = float(r.get("[Сума USD]", 0) or 0)
                pay_type = (r.get("[Тип]", "") or "").strip().lower()  # 'salary'/'bonus'/'prize'

                # Валюта як у вашій синхронізації: якщо USD != 0 -> USD, інакше UAH
                if abs(amount_usd) > 0:
                    amt, cur = amount_usd, "USD"
                else:
                    amt, cur = amount_uah, "UAH"

                any_currency = any_currency or cur
                amounts_by_type_and_month[pay_type][month_raw] += amt
                total_by_type[pay_type] += amt

            # Якщо Power BI дав нам хоч щось — відправляємо “розумне” повідомлення з типом
            await _send_notification_with_type(
                telegram_id=telegram_id,
                amounts_by_type_and_month=amounts_by_type_and_month,
                total_by_type=total_by_type,
                currency=any_currency or currency,
                payment_number=payment_number,
                payment_date=payment_date
            )
        else:
            # ---- Fallback: як було раніше (без типів) ----
            amounts_by_month = defaultdict(float)
            for p in payments:
                accrual_month = p[5]
                amounts_by_month[accrual_month] += float(p[1])
            await _send_notification_simple(
                telegram_id=telegram_id,
                amounts_by_month=amounts_by_month,
                currency=currency,
                payment_number=payment_number,
                payment_date=payment_date
            )

        # Позначаємо ці платежі як повідомлені
        cursor.execute("""
        UPDATE payments
        SET is_notified = TRUE
        WHERE payment_number = %s AND phone_number = %s
        """, (payment_number, phone_number))

    conn.commit()
    cursor.close()
    conn.close()

async def _send_notification_with_type(telegram_id, amounts_by_type_and_month, total_by_type, currency, payment_number, payment_date):
    """Повідомлення з визначеним типом(ами)."""
    bot = Bot(token=KEY)
    formatted_date = payment_date.strftime('%d.%m.%Y')
    header = f"💸 *Здійснена виплата!*\n📄 *Документ №:* {payment_number} від {formatted_date}\n"

    present_types = [t for t, s in total_by_type.items() if s > 0]
    present_types = [t for t in present_types if t]  # прибрати пусті значення

    if len(present_types) == 1:
        t = present_types[0]
        kind_line = f"🏷️ Це виплата {TYPE_GENITIVE_UA.get(t, 'зарплати')}.\n"

        months_map = amounts_by_type_and_month[t]
        # Відсортований і гарно підписаний список періодів
        lines = []
        total_amount = 0.0
        for m, amt in months_map.items():
            lines.append(f"• {_format_month(m)}: {_fmt_amount(amt)} {currency}")
            total_amount += amt

        body = (
            f"{kind_line}\n"
            f"📅 *Періоди та суми:*\n" + "\n".join(lines) +
            f"\n\n💰 *Загальна сума:* {_fmt_amount(total_amount)} {currency}"
        )
        msg = header + "\n" + body
    else:
        # Кілька типів — розділами
        sections = []
        grand_total = 0.0
        order = ['salary', 'prize', 'bonus']  # стабільний порядок
        for t in order:
            months_map = amounts_by_type_and_month.get(t, {})
            if not months_map:
                continue
            title = f"— *{TYPE_UA.get(t, t.capitalize())}* —"
            lines = [f"• {_format_month(m)}: {_fmt_amount(amt)} {currency}" for m, amt in months_map.items()]
            subtotal = sum(months_map.values())
            grand_total += subtotal
            sections.append(f"{title}\n" + "\n".join(lines) + f"\n_Разом по {TYPE_UA.get(t, t)}:_ *{_fmt_amount(subtotal)} {currency}*")

        if not sections:
            # На випадок, якщо типів немає (усі пусті) — відправляємо технічний заголовок
            sections.append("_Тип виплати не визначено у Power BI._")

        body = (
            "🏷️ У документі є кілька типів виплат.\n\n" +
            "\n\n".join(sections) +
            f"\n\n💰 *Загальна сума по документу:* {_fmt_amount(grand_total)} {currency}"
        )
        msg = header + "\n" + body

    try:
        await bot.send_message(chat_id=telegram_id, text=msg, parse_mode="Markdown")
        logging.info(f"Сповіщення (з типом) відправлено: {msg}")
    except Exception as e:
        logging.error(f"Помилка при відправці сповіщення: {e}")

async def _send_notification_simple(telegram_id, amounts_by_month, currency, payment_number, payment_date):
    """Fallback-повідомлення без типів (як було раніше)."""
    bot = Bot(token=KEY)

    # Гарні назви місяців
    formatted_periods = {}
    for raw_month, amount in amounts_by_month.items():
        try:
            parsed_date = datetime.strptime(raw_month, "%Y-%m-%d")
            month_name = MONTHS_UA.get(f"{parsed_date.month:02d}", raw_month)
            formatted_periods[month_name] = amount
        except ValueError:
            formatted_periods[raw_month] = amount

    details = "\n".join([f"• {month}: {_fmt_amount(amount)} {currency}" for month, amount in formatted_periods.items()])
    total_amount = sum(amounts_by_month.values())
    formatted_date = payment_date.strftime('%d.%m.%Y')

    message = (
        f"💸 *Здійснена виплата!*\n"
        f"📄 *Документ №:* {payment_number} від {formatted_date}\n\n"
        f"📅 *Періоди та суми:*\n"
        f"{details}\n\n"
        f"💰 *Загальна сума:* {_fmt_amount(total_amount)} {currency}"
    )

    try:
        await bot.send_message(chat_id=telegram_id, text=message, parse_mode="Markdown")
        logging.info(f"Сповіщення (fallback) відправлено: {message}")
    except Exception as e:
        logging.error(f"Помилка при відправці сповіщення: {e}")

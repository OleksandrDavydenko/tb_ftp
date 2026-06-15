import psycopg2
import os
from datetime import datetime
import logging

# Отримуємо URL бази даних з змінної середовища Heroku
DATABASE_URL = os.getenv('DATABASE_URL')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    # Підключаємось до бази даних PostgreSQL через URL з Heroku
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Створюємо таблицю користувачів
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        phone_number VARCHAR(20) UNIQUE NOT NULL,
        telegram_id BIGINT NOT NULL,
        telegram_name VARCHAR(50),
        employee_name VARCHAR(50),
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(20) DEFAULT 'active',
        is_blocked BOOLEAN DEFAULT FALSE
    )
    """)
    cursor.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='is_blocked'
        ) THEN
            ALTER TABLE users ADD COLUMN is_blocked BOOLEAN DEFAULT FALSE;
        END IF;
    END$$;
    """)

    # Створюємо таблицю виплат
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id SERIAL PRIMARY KEY,
        phone_number VARCHAR(20) NOT NULL,
        amount NUMERIC(10, 2),
        currency VARCHAR(10),
        payment_date TIMESTAMP,
        payment_number VARCHAR(50),
        is_notified BOOLEAN DEFAULT FALSE
    )
    """)
    cursor.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name='payments' AND column_name='accrual_month'
        ) THEN
            ALTER TABLE payments ADD COLUMN accrual_month VARCHAR(50);
        END IF;
    END$$;
    """)

    # Створюємо таблицю для аналізу девальвації
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DevaluationAnalysis (
        id SERIAL PRIMARY KEY,
        client VARCHAR(255),
        payment_number VARCHAR(50),
        acc_number VARCHAR(50),
        contract_number VARCHAR(50),
        date_from_acc TIMESTAMP,
        date_from_payment TIMESTAMP,
        date_difference_in_days INTEGER,
        currency_from_inform_acc VARCHAR(10),
        exchange_rate_acc_nbu NUMERIC(10, 4),
        exchange_rate_payment_nbu NUMERIC(10, 4),
        devaluation_percentage NUMERIC(5, 2),
        payment_sum NUMERIC(12, 2),
        compensation NUMERIC(12, 2),
        manager VARCHAR(255),
        is_notified BOOLEAN DEFAULT FALSE
    )
    """)

    # Створюємо таблицю курсів валют
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ExchangeRates (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        currency VARCHAR(10) NOT NULL,
        rate NUMERIC(10, 4) NOT NULL
    )
    """)

    # Створюємо таблицю для користувачів LightBot
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS usersLightBot (
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT UNIQUE NOT NULL,
        first_name VARCHAR(100),
        subscribed BOOLEAN DEFAULT FALSE
    )
    """)




    # Створюємо таблицю для логів бота
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bot_logs (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        username VARCHAR(50),
        action TEXT NOT NULL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        message_id BIGINT
    )
    """)

    # Створюємо таблицю для логів GPT-запитів
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS gpt_queries_logs (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        username TEXT,
        query TEXT NOT NULL,
        response TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        message_id BIGINT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bonus_docs (
        doc_number  VARCHAR(50) NOT NULL,
        period      VARCHAR(20) NOT NULL,
        is_notified BOOLEAN     NOT NULL DEFAULT FALSE,
        created_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (doc_number, period)
    )
    """)

    # Унікальність саме за номером документа
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_bonus_docs_doc_number
    ON bonus_docs (doc_number)
    """)

    



    conn.commit()
    cursor.close()
    conn.close()
    logging.info("✅ Усі таблиці створено або оновлено успішно.")

# Викликаємо функцію для створення таблиць при запуску
create_tables()




def add_telegram_user(phone_number, telegram_id, telegram_name, employee_name, status):
    """
    Додає нового користувача або оновлює його дані в таблиці users.
    Статус передається з auth.py ("active" або "deleted").
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO users (phone_number, telegram_id, telegram_name, employee_name, status, joined_at)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (phone_number) DO UPDATE SET
        telegram_id = EXCLUDED.telegram_id,
        telegram_name = EXCLUDED.telegram_name,
        employee_name = EXCLUDED.employee_name,
        status = EXCLUDED.status,
        joined_at = COALESCE(users.joined_at, EXCLUDED.joined_at)
    """, (phone_number, telegram_id, telegram_name, employee_name, status, datetime.now()))

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Користувач {phone_number} доданий/оновлений зі статусом {status}.")



def add_payment(phone_number, amount, currency, payment_date, payment_number, accrual_month, is_notified=False):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO payments (phone_number, amount, currency, payment_date, payment_number, accrual_month, is_notified)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (phone_number, amount, currency, payment_date, payment_number, accrual_month, is_notified))

    conn.commit()
    cursor.close()
    conn.close()



def add_devaluation_record(data):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO DevaluationAnalysis (
        client, payment_number, acc_number, contract_number, date_from_acc,
        date_from_payment, date_difference_in_days, currency_from_inform_acc,
        exchange_rate_acc_nbu, exchange_rate_payment_nbu, devaluation_percentage,
        payment_sum, compensation, manager
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (payment_number) DO NOTHING  -- Перевірка унікальності payment_number
    """, (
        data["Client"], data["PaymentNumber"], data["AccNumber"], data["ContractNumber"],
        data["DateFromAcc"], data["DateFromPayment"], data["DateDifferenceInDays"],
        data["CurrencyFromInformAcc"], data["ExchangeRateAccNBU"], data["ExchangeRatePaymentNBU"],
        data["Devalvation%"], data["PaymentSum"], data["Compensation"], data["Manager"]
    ))

    conn.commit()
    cursor.close()
    conn.close()

def add_exchange_rate(currency, rate):
    """
    Функція для додавання запису про курс валют у таблицю ExchangeRates.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO ExchangeRates (currency, rate)
    VALUES (%s, %s)
    """, (currency, rate))

    conn.commit()
    cursor.close()
    conn.close()

def get_user_joined_at(phone_number):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT joined_at FROM users WHERE phone_number = %s
    """, (phone_number,))
    result = cursor.fetchone()

    conn.close()

    if result:
        return result[0]
    return None





def get_all_users():
    """
    Отримує всіх користувачів із БД із їх статусами та іменами.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT phone_number, status, employee_name FROM users")
    users = [{"phone_number": row[0], "status": row[1], "employee_name": row[2]} for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return users





def get_active_users():
    conn = get_db_connection()
    cursor = conn.cursor()
    #тільки активні користувачі
    cursor.execute("SELECT telegram_id, telegram_name, employee_name FROM users WHERE status = 'active'")
    users = cursor.fetchall()

    conn.close()

    return [{'telegram_id': user[0], 'telegram_name': user[1], 'employee_name': user[2]} for user in users]


def get_test_user():
    conn = get_db_connection()
    cursor = conn.cursor()
    #тільки активні користувачі
    cursor.execute("SELECT telegram_id, telegram_name, employee_name FROM users WHERE telegram_id = 203148640")
    users = cursor.fetchall()

    conn.close()

    return [{'telegram_id': user[0], 'telegram_name': user[1], 'employee_name': user[2]} for user in users]




def get_latest_currency_rates(currencies):
    """
    Отримує останній курс для кожної валюти із бази даних.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT DISTINCT ON (currency) currency, rate
            FROM exchangerates
            WHERE currency = ANY(%s)
            ORDER BY currency, timestamp DESC;
        """
        cursor.execute(query, (currencies,))
        rows = cursor.fetchall()
        conn.close()
        return [{"currency": row[0], "rate": row[1]} for row in rows]
    except Exception as e:
        print(f"Помилка отримання курсів: {e}")
        raise e
    

def get_user_status(phone_number):
    """ Отримує поточний статус користувача з бази """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT status FROM users WHERE phone_number = %s
    """, (phone_number,))
    result = cursor.fetchone()

    conn.close()

    if result:
        return result[0]
    return None  # Якщо користувача немає в базі


def get_employee_name(phone_number):
    """ Отримує ім'я співробітника з бази даних, якщо воно є """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT employee_name FROM users WHERE phone_number = %s
    """, (phone_number,))
    result = cursor.fetchone()

    conn.close()

    if result:
        return result[0]  # Повертаємо ім'я користувача
    return None  # Якщо імені немає в БД




def delete_user_payments(phone_number):
    """
    Видаляє всі платежі користувача з таблиці payments за номером телефону, 
    де дата платежу не дорівнює сьогоднішній.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    today = datetime.today().date()  # Отримуємо сьогоднішню дату
    
    try:
        # Видаляємо платежі, де payment_date не дорівнює сьогоднішній даті
        cursor.execute("""
            DELETE FROM payments 
            WHERE phone_number = %s 
            AND payment_date::date != %s
        """, (phone_number, today))
        conn.commit()
        logging.info(f"❌ Платежі для {phone_number}, де дата не сьогодні, видалено.")
    except Exception as e:
        logging.error(f"⚠️ Помилка видалення платежів для {phone_number}: {e}")
    finally:
        cursor.close()
        conn.close()





def update_user_joined_at(phone_number, new_joined_at):
    """
    Оновлює поле joined_at для користувача.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("UPDATE users SET joined_at = %s WHERE phone_number = %s", (new_joined_at, phone_number))
        conn.commit()
        logging.info(f"📅 Оновлено joined_at для {phone_number}: {new_joined_at}")
    except Exception as e:
        logging.error(f"⚠️ Помилка оновлення joined_at для {phone_number}: {e}")
    finally:
        cursor.close()
        conn.close()


def update_user_status(phone_number, new_status):
    """
    Оновлює статус користувача у БД.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("UPDATE users SET status = %s WHERE phone_number = %s", (new_status, phone_number))
        conn.commit()
        logging.info(f"📌 Оновлено статус для {phone_number}: {new_status}")
    except Exception as e:
        logging.error(f"⚠️ Помилка оновлення статусу {phone_number}: {e}")
    finally:
        cursor.close()
        conn.close()





def log_user_action(user_id, action, message_id=None):
    """
    Логування дій користувача у базу даних, збереження message_id.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Отримуємо username з бази даних за user_id
        cursor.execute("SELECT employee_name FROM users WHERE telegram_id = %s", (user_id,))
        result = cursor.fetchone()
        username = result[0] if result else "Unknown User"

        cursor.execute("""
            INSERT INTO bot_logs (user_id, username, action, message_id)
            VALUES (%s, %s, %s, %s)
        """, (user_id, username, action, message_id))

        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"✅ Записано в логи: {username} (ID: {user_id}) - {action} (message_id: {message_id})")

    except Exception as e:
        logging.error(f"❌ Помилка при записі в логи: {e}")


def update_employee_name(phone_number, employee_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE users SET employee_name = %s WHERE phone_number = %s
    """, (employee_name, phone_number))

    conn.commit()
    cursor.close()
    conn.close()



def save_gpt_query(user_id, username, query, response, message_id):
    """
    Зберігає запит користувача та відповідь GPT у базі даних, включаючи message_id.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO gpt_queries_logs (user_id, username, query, response, message_id)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, username, query, response, message_id))

        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"✅ GPT-запит збережено: {user_id} ({username}) - {query} (message_id: {message_id})")
    except Exception as e:
        logging.error(f"❌ Помилка при збереженні GPT-запиту: {e}")




def get_last_gpt_queries(user_id, limit=3):
    """
    Отримує останні N повідомлень користувача з бази даних.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT query, response
            FROM gpt_queries_logs
            WHERE user_id = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (user_id, limit))

        messages = cursor.fetchall()
        cursor.close()
        conn.close()

        # Формуємо список словників з правильними ключами
        return [
            {"role": "user", "content": q} if q else {"role": "assistant", "content": r}
            for q, r in messages[::-1]  # Перевертаємо, щоб порядок був хронологічним
        ]

    except Exception as e:
        logging.error(f"❌ Помилка при отриманні історії GPT-запитів: {e}")
        return []
    


def get_user_by_telegram_id(telegram_id):
    import logging
    from db import get_db_connection  # якщо ще не імпортовано

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        telegram_id_int = int(telegram_id)
        cursor.execute(
            """
            SELECT phone_number, employee_name 
            FROM users 
            WHERE telegram_id = %s AND status = 'active'
            """,
            (telegram_id_int,)
        )
        user = cursor.fetchone()
        logging.info(f"[DB] 🔍 get_user_by_telegram_id({telegram_id}) => {user}")
        return user
    except Exception as e:
        logging.error(f"[DB] ❌ Помилка в get_user_by_telegram_id: {e}")
        return None
    finally:
        cursor.close()
        conn.close()




def log_birthday_greeting(employee_name, query, response):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Отримати telegram_id та username
    cursor.execute("SELECT telegram_id, employee_name FROM users WHERE employee_name = %s", (employee_name,))
    result = cursor.fetchone()

    if not result:
        logging.warning(f"Не знайдено користувача {employee_name} для логування привітання.")
        return

    user_id, username = result
    timestamp = datetime.now()

    cursor.execute("""
        INSERT INTO gpt_queries_logs (user_id, username, query, response, timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """, (user_id, username, query, response, timestamp))

    conn.commit()
    cursor.close()
    conn.close()




def add_bonus_doc(doc_number: str, period: str, is_notified: bool = False):
    """
    Додає один запис у bonus_docs. Якщо такий DocNumber+Period вже є — ігнорує.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO bonus_docs (doc_number, period, is_notified)
            VALUES (%s, %s, %s)
            ON CONFLICT (doc_number, period) DO NOTHING
        """, (doc_number, period, is_notified))
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def bulk_add_bonus_docs(rows):
    """
    Додає (doc_number, period) лише для нових doc_number.
    Використовує ON CONFLICT (doc_number) DO NOTHING, щоб не чіпати існуючі.
    """
    if not rows:
        return 0
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        args_str = ",".join(cursor.mogrify("(%s,%s,FALSE)", r).decode("utf-8") for r in rows)
        cursor.execute(f"""
            INSERT INTO bonus_docs (doc_number, period, is_notified)
            VALUES {args_str}
            ON CONFLICT (doc_number) DO NOTHING
        """)
        inserted = cursor.rowcount
        conn.commit()
        return inserted
    finally:
        cursor.close()
        conn.close()

def get_existing_bonus_doc_numbers():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT doc_number FROM bonus_docs")
        return {r[0] for r in cursor.fetchall()}
    finally:
        cursor.close()
        conn.close()



def mark_bonus_docs_notified(doc_numbers):
    """
    Встановлює is_notified = TRUE для вказаних номерів документів.
    """
    if not doc_numbers:
        return 0
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE bonus_docs
            SET is_notified = TRUE
            WHERE doc_number = ANY(%s) AND is_notified = FALSE
        """, (list(doc_numbers),))
        affected = cursor.rowcount
        conn.commit()
        return affected
    finally:
        cursor.close()
        conn.close()









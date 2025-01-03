import asyncio
import requests
import psycopg2
import os
import logging
from auth import get_power_bi_token

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

async def async_add_devaluation_record(data):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Очищуємо ключі від квадратних дужок
    cleaned_data = {key.strip("[]"): value for key, value in data.items()}

    try:
        # Логування очищених даних перед вставкою
        logging.info(f"Очищені дані для вставки: {cleaned_data}")

        # Перевірка наявності унікального запису
        cursor.execute("""
            SELECT 1 FROM DevaluationAnalysis
            WHERE client = %s AND payment_number = %s AND acc_number = %s
        """, (cleaned_data.get("Client"), cleaned_data.get("PaymentNumber"), cleaned_data.get("AccNumber")))

        if cursor.fetchone() is None:
            # Вставка даних у таблицю DevaluationAnalysis, якщо запис унікальний
            cursor.execute("""
                INSERT INTO DevaluationAnalysis (
                    client, payment_number, acc_number, contract_number, date_from_acc,
                    date_from_payment, date_difference_in_days, currency_from_inform_acc,
                    exchange_rate_acc_nbu, exchange_rate_payment_nbu, devaluation_percentage,
                    payment_sum, compensation, manager
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                cleaned_data.get("Client"), cleaned_data.get("PaymentNumber"), cleaned_data.get("AccNumber"), cleaned_data.get("ContractNumber"),
                cleaned_data.get("DateFromAcc"), cleaned_data.get("DateFromPayment"), cleaned_data.get("DateDifferenceInDays"),
                cleaned_data.get("CurrencyFromInformAcc"), cleaned_data.get("ExchangeRateAccNBU"), cleaned_data.get("ExchangeRatePaymentNBU"),
                cleaned_data.get("Devalvation%"), cleaned_data.get("PaymentSum"), cleaned_data.get("Compensation"), cleaned_data.get("Manager")
            ))

            conn.commit()
            logging.info(f"Додано новий запис для клієнта: {cleaned_data.get('Client')}, платіж: {cleaned_data.get('PaymentNumber')}.")
        else:
            logging.info(f"Запис для клієнта {cleaned_data.get('Client')}, платіж {cleaned_data.get('PaymentNumber')} вже існує і не буде доданий.")

    except Exception as e:
        logging.error(f"Помилка при додаванні запису: {e}")
    finally:
        cursor.close()
        conn.close()

async def sync_devaluation_data():
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    query_data = {
        "queries": [
            {
                "query": """
                    EVALUATE 
                    SELECTCOLUMNS(
                        DevaluationAnalysis,
                        "Client", DevaluationAnalysis[Client],
                        "PaymentNumber", DevaluationAnalysis[PaymentNumber],
                        "AccNumber", DevaluationAnalysis[AccNumber],
                        "ContractNumber", DevaluationAnalysis[ContractNumber],
                        "DateFromAcc", DevaluationAnalysis[DateFromAcc],
                        "DateFromPayment", DevaluationAnalysis[DateFromPayment],
                        "DateDifferenceInDays", DevaluationAnalysis[DateDifferenceInDays],
                        "CurrencyFromInformAcc", DevaluationAnalysis[CurrencyFromInformAcc],
                        "ExchangeRateAccNBU", DevaluationAnalysis[ExchangeRateAccNBU],
                        "ExchangeRatePaymentNBU", DevaluationAnalysis[ExchangeRatePaymentNBU],
                        "Devalvation%", DevaluationAnalysis[Devalvation%],
                        "PaymentSum", DevaluationAnalysis[PaymentSum],
                        "Compensation", DevaluationAnalysis[Compensation],
                        "Manager", DevaluationAnalysis[Manager]
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    try:
        response = requests.post(power_bi_url, headers=headers, json=query_data)
        if response.status_code == 200:
            data = response.json()
            rows = data['results'][0]['tables'][0].get('rows', [])
            logging.info(f"Отримано {len(rows)} записів з Power BI.")
            
            for record in rows:
                logging.info(f"Дані запису перед вставкою в базу даних: {record}")
                await async_add_devaluation_record(record)

            logging.info(f"Успішно синхронізовано {len(rows)} записів.")
        else:
            logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")

    except Exception as e:
        logging.error(f"Помилка при синхронізації даних девальвації: {e}")

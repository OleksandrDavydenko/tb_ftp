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

    try:
        # Вставка даних у таблицю DevaluationAnalysis
        cursor.execute("""
            INSERT INTO DevaluationAnalysis (
                client, payment_number, acc_number, contract_number, date_from_acc,
                date_from_payment, date_difference_in_days, currency_from_inform_acc,
                exchange_rate_acc_nbu, exchange_rate_payment_nbu, devaluation_percentage,
                payment_sum, compensation, manager
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get("Client"), data.get("PaymentNumber"), data.get("AccNumber"), data.get("ContractNumber"),
            data.get("DateFromAcc"), data.get("DateFromPayment"), data.get("DateDifferenceInDays"),
            data.get("CurrencyFromInformAcc"), data.get("ExchangeRateAccNBU"), data.get("ExchangeRatePaymentNBU"),
            data.get("Devalvation%"), data.get("PaymentSum"), data.get("Compensation"), data.get("Manager")
        ))

        conn.commit()
        logging.info(f"Додано новий запис для клієнта: {data.get('Client')}, платіж: {data.get('PaymentNumber')}.")

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
                        DevaluationData,
                        "Client", DevaluationData[Client],
                        "PaymentNumber", DevaluationData[PaymentNumber],
                        "AccNumber", DevaluationData[AccNumber],
                        "ContractNumber", DevaluationData[ContractNumber],
                        "DateFromAcc", DevaluationData[DateFromAcc],
                        "DateFromPayment", DevaluationData[DateFromPayment],
                        "DateDifferenceInDays", DevaluationData[DateDifferenceInDays],
                        "CurrencyFromInformAcc", DevaluationData[CurrencyFromInformAcc],
                        "ExchangeRateAccNBU", DevaluationData[ExchangeRateAccNBU],
                        "ExchangeRatePaymentNBU", DevaluationData[ExchangeRatePaymentNBU],
                        "Devalvation%", DevaluationData[Devalvation%],
                        "PaymentSum", DevaluationData[PaymentSum],
                        "Compensation", DevaluationData[Compensation],
                        "Manager", DevaluationData[Manager]
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
            for record in rows:
                await async_add_devaluation_record(record)

            logging.info(f"Успішно синхронізовано {len(rows)} записів.")
        else:
            logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")

    except Exception as e:
        logging.error(f"Помилка при синхронізації даних девальвації: {e}")

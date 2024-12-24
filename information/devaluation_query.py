import requests
import logging
from auth import get_power_bi_token

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_devaluation_data(manager_name):
    """
    Виконує запит до Power BI API для отримання даних девальвації.
    """
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'  # Замініть на ваш dataset_id
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Формування запиту
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            DevaluationCheck,
                            DevaluationCheck[Manager] = "{manager_name}"
                        ),
                        "Client", DevaluationCheck[Client],
                        "AccNumber", DevaluationCheck[AccNumber],
                        "DateFromAcc", DevaluationCheck[DateFromAcc],
                        "ContractNumber", DevaluationCheck[ContractNumber],
                        "CurrencyFromInformAcc", DevaluationCheck[CurrencyFromInformAcc],
                        "NBURateOnAccountDate", DevaluationCheck[NBURateOnAccountDate],
                        "NBURateToday", DevaluationCheck[NBURateToday],
                        "Devalvation%", DevaluationCheck[Devalvation%],
                        "Manager", DevaluationCheck[Manager]
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    # Виконання запиту
    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        logging.info("Запит до Power BI успішний.")
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        return rows
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None



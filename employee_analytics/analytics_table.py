import requests
import logging
from auth import get_power_bi_token

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для отримання доходу за місяць і рік для Менеджера та Сейлза
def get_income_data(employee_name, role, year, month):
    logging.info(f"Запит на отримання даних для: {employee_name}, роль: {role}, рік: {year}, місяць: {month}")
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Перетворення місяця на текстовий формат, щоб відповідати формату даних
    months_mapping = {
        "Січень": "січень", "Лютий": "лютий", "Березень": "березень", "Квітень": "квітень",
        "Травень": "травень", "Червень": "червень", "Липень": "липень", "Серпень": "серпень",
        "Вересень": "вересень", "Жовтень": "жовтень", "Листопад": "листопад", "Грудень": "грудень"
    }
    month_name = months_mapping.get(month)

    # Визначення колонки фільтрації за роллю
    role_column = "Manager" if role == "Менеджер" else "Seller"

    # Формування запиту з умовами фільтрації
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SUMMARIZECOLUMNS(
                        'GrossProfitFromDeals'[{role_column}],
                        FILTER(
                            'GrossProfitFromDeals',
                            'GrossProfitFromDeals'[{role_column}] = "{employee_name}"
                            && 'GrossProfitFromDeals'[RegistrDate] = "{month_name} {year} р."
                        ),
                        "Дохід", SUM('GrossProfitFromDeals'[Income])
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    logging.info(f"Виконуємо запит до Power BI для {role} {employee_name} без фільтрації за датою.")
    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        logging.info(f"Запит до Power BI для {role} {employee_name} успішний.")
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        logging.info(f"Отримано {len(rows)} рядків для {role}.")
        return rows
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None

# Форматування таблиці для відображення доходу працівника
def format_income_table(manager_income, sales_income):
    table = "Аналітика працівника:\n"
    table += "-" * 45 + "\n"
    table += f"{'Показник':<25}{'Менеджер':<10}{'Сейлс':<10}\n"
    table += "-" * 45 + "\n"

    # Визначення значень для Менеджера та Сейлза
    manager_income_value = manager_income[0]['Дохід'] if manager_income else 0
    sales_income_value = sales_income[0]['Дохід'] if sales_income else 0

    # Додавання рядків до таблиці
    table += f"{'Дохід':<25}{manager_income_value:<10}{sales_income_value:<10}\n"
    table += "-" * 45 + "\n"

    logging.info("Формування таблиці завершено.")
    return table

import requests
import logging
from auth import get_power_bi_token

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для отримання даних про дохід менеджера та сейлза за рік і місяць
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

    months_mapping = {
        "Січень": 1, "Лютий": 2, "Березень": 3, "Квітень": 4,
        "Травень": 5, "Червень": 6, "Липень": 7, "Серпень": 8,
        "Вересень": 9, "Жовтень": 10, "Листопад": 11, "Грудень": 12
    }
    
    month_number = months_mapping.get(month)
    if month_number is None:
        logging.error(f"Неправильний місяць: {month}")
        return None
    formatted_month = f"{month_number:02d}"

    role_column = "Manager" if role == "Менеджер" else "Seller"

    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SUMMARIZECOLUMNS(
                        'GrossProfitFromDeals'[{role_column}],
                        "Дохід", SUM('GrossProfitFromDeals'[Income])
                    )
                    WHERE 
                        'GrossProfitFromDeals'[{role_column}] = "{employee_name}" &&
                        FORMAT('GrossProfitFromDeals'[Date], "YYYY-MM") = "{year}-{formatted_month}"
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    logging.info(f"Виконуємо запит до Power BI для {role} {employee_name}.")
    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        logging.info(f"Запит до Power BI для {role} {employee_name} успішний.")
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        return rows[0] if rows else None  # Повертаємо перший рядок як результат
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None

# Функція для форматування таблиці аналітики працівника
def format_analytics_table(manager_income, sales_income):
    table = "Аналітика працівника:\n"
    table += "-" * 45 + "\n"
    table += f"{'Показник':<25}{'Менеджер':<10}{'Сейлс':<10}\n"
    table += "-" * 45 + "\n"

    manager_income_value = manager_income.get("Дохід", 0) if manager_income else 0
    sales_income_value = sales_income.get("Дохід", 0) if sales_income else 0

    table += f"{'Дохід':<25}{manager_income_value:<10}{sales_income_value:<10}\n"
    table += "-" * 45 + "\n"
    logging.info("Формування таблиці аналітики завершено.")
    return table

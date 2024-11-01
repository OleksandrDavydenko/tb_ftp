import requests
import logging
from auth import get_power_bi_token

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для отримання даних про дохід для конкретного співробітника за обраний місяць та рік
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

    # Визначення колонки для фільтрації за роллю
    role_column = "Manager" if role == "Менеджер" else "Seller"
    # Формат дати з малої літери
    formatted_date = f"{month.lower()} {year} р."

    # Запит з фільтрацією за користувачем та датою
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SUMMARIZECOLUMNS(
                        'GrossProfitFromDeals'[{role_column}],
                        FILTER(
                            'GrossProfitFromDeals',
                            'GrossProfitFromDeals'[{role_column}] = "{employee_name}" &&
                            'GrossProfitFromDeals'[RegistrDate] = "{formatted_date}"
                        ),
                        "TotalIncome", SUM('GrossProfitFromDeals'[Income])
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    logging.info(f"Виконуємо запит до Power BI для {role} {employee_name} за {formatted_date}.")
    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        logging.info(f"Запит до Power BI для {role} {employee_name} успішний.")
        data = response.json()
        logging.info(f"Повна відповідь від Power BI: {data}")  # Логування повної відповіді
        rows = data['results'][0]['tables'][0].get('rows', [])
        logging.info(f"Отримано {len(rows)} рядків для {role} {employee_name}.")
        return rows[0] if rows else None
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None


# Функція для форматування таблиці аналітики для одного співробітника
def format_analytics_table(manager_income, sales_income):
    table = "Аналітика працівника:\n"
    table += "-" * 45 + "\n"
    table += f"{'Показник':<25}{'Менеджер':<10}{'Сейлс':<10}\n"
    table += "-" * 45 + "\n"

    # Значення доходу для менеджера і сейлза
    manager_income_value = manager_income.get("TotalIncome", 0) if manager_income else 0
    sales_income_value = sales_income.get("TotalIncome", 0) if sales_income else 0

    table += f"{'Дохід':<25}{manager_income_value:<10}{sales_income_value:<10}\n"
    table += "-" * 45 + "\n"
    
    logging.info("Формування таблиці аналітики завершено.")
    return table

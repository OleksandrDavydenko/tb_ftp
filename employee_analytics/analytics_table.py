import requests
import logging
from auth import get_power_bi_token

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для отримання даних про дохід, валовий прибуток та кількість угод для конкретного співробітника за обраний місяць та рік
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

    # Визначення колонки для фільтрування за роллю
    role_column = "Manager" if role == "Менеджер" else "Seller"
    formatted_date = f"{month.lower()} {year} р."

    # Запит з фільтрацією за користувачем, для обчислення доходу, валового прибутку, бонусів та кількості угод
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
                            FORMAT('GrossProfitFromDeals'[RegistrDate], "MMMM yyyy р.") = "{formatted_date}"
                        ),
                        "Sum USD", SUM('GrossProfitFromDeals'[Income]),
                        "Gross Profit", SUM('GrossProfitFromDeals'[GrossProfit]),
                        "Bonuses", SUM('GrossProfitFromDeals'[Bonuses]),
                        "Deal Count", COUNTROWS(SUMMARIZE('GrossProfitFromDeals', 'GrossProfitFromDeals'[DealNumber]))
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
        logging.info(f"Повна відповідь від Power BI: {data}")
        rows = data['results'][0]['tables'][0].get('rows', [])
        logging.info(f"Отримано {len(rows)} рядків для {role} {employee_name}.")
        return rows[0] if rows else None
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None

# Функція для форматування таблиці аналітики для одного співробітника
def format_analytics_table(income_data, employee_name, month, year):
    # Форматування заголовка таблиці
    formatted_date = f"{month.lower()} {year} р."
    table = f"Аналітика {employee_name} за {formatted_date}:\n"
    table += "-" * 30 + "\n"
    table += f"{'Показник':<20}{'Сума USD':<10}\n"
    table += "-" * 30 + "\n"

    # Отримання значень з правильними ключами
    total_income = income_data.get("[Sum USD]", 0) if income_data else 0
    gross_profit = income_data.get("[Gross Profit]", 0) if income_data else 0
    bonuses = income_data.get("[Bonuses]", 0) if income_data else 0
    deal_count = income_data.get("[Deal Count]", 0) if income_data else 0

    # Розрахунок валового прибутку з урахуванням бонусів
    total_gross_profit = gross_profit + bonuses
    # Розрахунок маржинальності
    margin = (total_gross_profit / total_income * 100) if total_income else 0

    table += f"{'Загальний дохід':<20}{total_income:<10}\n"
    table += f"{'Валовий прибуток':<20}{total_gross_profit:<10}\n"
    table += f"{'Кількість угод':<20}{deal_count:<10}\n"
    table += f"{'Маржинальність':<20}{margin:.2f}%\n"
    table += "-" * 30 + "\n"
    
    logging.info("Формування таблиці аналітики завершено.")
    return table

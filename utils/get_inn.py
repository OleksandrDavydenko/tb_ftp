import requests
import logging
from auth import get_power_bi_token

def get_employee_inn(employee_name: str) -> str | None:
    """
    Отримує INN співробітника з таблиці Employees по імені.
    Якщо знайдено кілька — бере перший.
    Якщо нічого не знайдено або помилка — повертає None.
    """
    # Отримуємо токен для доступу до Power BI
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати токен для Power BI.")
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    dataset_id = "8b80be15-7b31-49e4-bc85-8b37a0d98f1c"
    power_bi_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"

    # Запит на отримання INN співробітника по імені
    dax_query = {
        "queries": [
            {
                "query": f"""
                    EVALUATE
                    SELECTCOLUMNS(
                        FILTER(
                            Employees,
                            LEFT(Employees[Employee], LEN("{employee_name}")) = "{employee_name}"
                        ),
                        "INN", Employees[INN]
                    )
                """
            }
        ],
        "serializerSettings": {"includeNulls": True},
    }

    logging.info(f"📤 Шукаємо INN для співробітника {employee_name} в таблиці Employees")
    response = requests.post(power_bi_url, headers=headers, json=dax_query)

    logging.info(f"📥 Статус відповіді Power BI (INN): {response.status_code}")
    
    # Тільки для дебагінга показуємо повну відповідь
    if logging.getLogger().getEffectiveLevel() <= logging.DEBUG:
        logging.debug(f"📄 Вміст відповіді (INN): {response.text}")

    if response.status_code != 200:
        logging.warning("⚠️ Не вдалося отримати INN, спробуємо інший метод.")
        return None

    try:
        data = response.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
    except Exception as e:
        logging.error(f"❌ Помилка при розборі JSON (INN): {e}")
        return None

    if not rows:
        logging.warning(f"⚠️ INN для {employee_name} не знайдено.")
        return None

    # Перевіряємо обидва можливі формати ключів
    first_row = rows[0]
    
    # Спробуємо отримати INN у різних форматах
    inn = None
    possible_keys = ["[INN]", "INN", "inn", "Inn"]
    
    for key in possible_keys:
        if key in first_row:
            inn = first_row[key]
            break
    
    if inn:
        logging.info(f"✅ Знайдено INN для {employee_name}: {inn}")
        return str(inn)  # Конвертуємо в строку на всяк випадок
    else:
        # Для дебагінга покажемо, які ключі є насправді
        logging.warning(f"⚠️ INN для {employee_name} не знайдено. Доступні ключі: {list(first_row.keys())}")
        return None

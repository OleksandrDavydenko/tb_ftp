import requests
import logging
from auth import get_power_bi_token

def get_employee_inn(employee_name: str) -> str | None:
    """
    Отримує INN співробітника з таблиці Employees по імені.
    """
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

    logging.info(f"📤 Шукаємо INN для {employee_name}")
    response = requests.post(power_bi_url, headers=headers, json=dax_query, timeout=60)

    logging.info(f"📥 Статус: {response.status_code}")
    logging.info(f"📄 Відповідь: {response.text}")

    if response.status_code != 200:
        return None

    try:
        data = response.json()
        rows = data["results"][0]["tables"][0].get("rows", [])
        
        if not rows:
            return None
        
        # Головна зміна: використовуємо "[INN]" замість "INN"
        inn = rows[0].get("[INN]")
        
        if inn:
            inn_str = str(inn).strip()
            logging.info(f"✅ Знайдено INN: {inn_str}")
            return inn_str
        else:
            # Дебаг: що насправді в рядку?
            logging.warning(f"⚠️ Ключ [INN] не знайдено. Рядок містить: {rows[0]}")
            return None
            
    except Exception as e:
        logging.error(f"❌ Помилка: {e}")
        return None
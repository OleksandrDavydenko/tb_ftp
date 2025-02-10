import re
import requests
import os
from db import add_telegram_user, get_user_status, get_employee_name  # Імпортуємо функцію перевірки статусу
import logging


# Функція для нормалізації номера телефону (залишає лише останні 9 цифр)
def normalize_phone_number(phone_number):
    digits = re.sub(r'\D', '', phone_number)
    return digits[-9:]

# Отримання токену Power BI
def get_power_bi_token():
    client_id = '706d72b2-a9a2-4d90-b0d8-b08f58459ef6'
    username = 'od@ftpua.com'
    password = os.getenv('PASSWORD')
    url = 'https://login.microsoftonline.com/common/oauth2/token'
    
    body = {
        'grant_type': 'password',
        'resource': 'https://analysis.windows.net/powerbi/api',
        'client_id': client_id,
        'username': username,
        'password': password
    }
    
    response = requests.post(url, data=body, headers={'Content-Type': 'application/x-www-form-urlencoded'})
    
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        print(f"Error getting token: {response.status_code}, {response.text}")
        return None

# Перевірка номера телефону в Power BI
def is_phone_number_in_power_bi(phone_number):
    token = get_power_bi_token()
    if not token:
        logging.error("❌ Не вдалося отримати токен Power BI.")
        return False, None, None

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Запит до Power BI
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        Employees,
                        "Employee", Employees[Employee],
                        "PhoneNumber", Employees[PhoneNumberTelegram],
                        "Status", Employees[Status]
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        logging.info(f"📊 Дані з Power BI: {rows}")

        phone_map = {normalize_phone_number(row.get('[PhoneNumberTelegram]', '')): (row.get('[Employee]', ''), row.get('[Status]', '')) for row in rows}

        normalized_phone_number = normalize_phone_number(phone_number)
        logging.info(f"📞 Нормалізований номер телефону: {normalized_phone_number}")

        if normalized_phone_number in phone_map:
            employee_name, status = phone_map[normalized_phone_number]
            logging.info(f"✅ Знайдено в Power BI: {employee_name}, Статус: {status}")
            return status == "Активний", employee_name, status
        else:
            logging.warning(f"🚫 Номер телефону {normalized_phone_number} не знайдено в Power BI.")
            return False, None, None
    else:
        logging.error(f"❌ Помилка запиту до Power BI: {response.status_code}, {response.text}")
        return False, None, None



# Функція для перевірки користувача і запису в базу
def verify_and_add_user(phone_number, telegram_id, telegram_name):
    """
    1. Перевіряє користувача в Power BI.
    2. Якщо знайдено → оновлює статус у БД відповідно до Power BI.
    3. Якщо не знайдено → статус `deleted`.
    """
    is_active, employee_name, status_from_power_bi = is_phone_number_in_power_bi(phone_number)

    # Отримуємо ім'я з БД, якщо не знайдено в Power BI
    if not employee_name:
        employee_name = get_employee_name(phone_number)

    # Новий статус: "active" або "deleted"
    new_status = "active" if status_from_power_bi == "Активний" else "deleted"

    # Поточний статус у базі
    current_status = get_user_status(phone_number)

    # Оновлюємо статус, якщо він змінився
    if current_status != new_status:
        add_telegram_user(phone_number, telegram_id, telegram_name, employee_name, new_status)
        logging.info(f"🔄 Статус користувача {phone_number} оновлено: {new_status}")
    else:
        logging.info(f"✅ Статус користувача {phone_number} без змін: {current_status}")



# Функція для отримання даних про дебіторку для менеджера
def get_user_debt_data(manager_name):
    token = get_power_bi_token()
    if not token:
        return None
    
    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        FILTER(
                            Deb,
                            Deb[Manager] = "{manager_name}" && Deb[Inform] <> 1
                        ),
                        "Client", Deb[Client],
                        "Sum_$", Deb[Sum_$],
                        "Manager", Deb[Manager],
                        "PlanDatePay", Deb[PlanDatePay],
                        "Account", Deb[Account],
                        "Deal", Deb[Deal],
                        "AccountDate",  Deb[AccountDate]
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }
    
    response = requests.post(power_bi_url, headers=headers, json=query_data)
    
    if response.status_code == 200:
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
        return rows
    else:
        print(f"Error executing query: {response.status_code}, {response.text}")
        return None

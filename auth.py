import re
import requests
import os
from db import add_telegram_user, get_user_status, get_employee_name  # Імпортуємо функцію перевірки статусу


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
        return False, None, None  # Додаємо статус у повернення

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Оновлений запит: повертає всіх користувачів, а не лише активних
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SELECTCOLUMNS(
                        Employees,
                        "Employee", Employees[Employee],
                        "PhoneNumber", Employees[PhoneNumberTelegram],
                        "Status", Employees[Status]  -- Додаємо статус!
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
        if 'results' in data and len(data['results']) > 0 and 'tables' in data['results'][0] and len(data['results'][0]['tables']) > 0:
            rows = data['results'][0]['tables'][0].get('rows', [])
            if rows:
                phone_map = {normalize_phone_number(row.get('[PhoneNumberTelegram]', '')): (row.get('[Employee]', ''), row.get('[Status]', '')) for row in rows}

                normalized_phone_number = normalize_phone_number(phone_number)

                if normalized_phone_number in phone_map:
                    employee_name, status = phone_map[normalized_phone_number]
                    return status == "Активний", employee_name, status  # Додаємо статус!
                else:
                    return False, None, None
        return False, None, None
    else:
        print(f"Error executing query: {response.status_code}, {response.text}")
        return False, None, None


# Функція для перевірки користувача і запису в базу
def verify_and_add_user(phone_number, telegram_id, telegram_name):
    """
    1. Перевіряє статус користувача у Power BI.
    2. Якщо запис знайдено → оновлює статус в БД.
    3. Якщо запис не знайдено → записує `deleted`.
    4. Якщо статус у БД відрізняється від Power BI → оновлює його.
    """

    is_active, employee_name, status_from_power_bi = is_phone_number_in_power_bi(phone_number)

    # Якщо ім'я не знайдено в Power BI, пробуємо отримати його з БД
    if not employee_name:
        employee_name = get_employee_name(phone_number)

    # Визначаємо новий статус
    new_status = "active" if status_from_power_bi == "Активний" else "deleted"

    # Отримуємо поточний статус у БД
    current_status = get_user_status(phone_number)

    # Якщо статус у БД не відповідає Power BI, оновлюємо його
    if current_status != new_status:
        add_telegram_user(phone_number, telegram_id, telegram_name, employee_name, new_status)
        print(f"🔄 Статус оновлено: {phone_number} → {new_status}")
    else:
        print(f"✅ Статус без змін: {phone_number} → {current_status}")


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

import re
import requests
import os
from db import add_telegram_user  # Імпортуємо функцію додавання користувачів

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
        return False, None
    
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
                            Employees,
                            NOT(ISBLANK([PhoneNumber])) && [Status] = "Активний"
                        ),
                        "Employee", Employees[Employee],
                        "PhoneNumber", Employees[PhoneNumber]
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
                # Нормалізуємо номери телефонів з Power BI
                phone_numbers = {normalize_phone_number(row.get('[PhoneNumber]', '')): row.get('[Employee]', '') for row in rows}
                
                normalized_phone_number = normalize_phone_number(phone_number)
                
                if normalized_phone_number in phone_numbers:
                    return True, phone_numbers[normalized_phone_number]
                else:
                    return False, None
            else:
                return False, None
        else:
            return False, None
    else:
        print(f"Error executing query: {response.status_code}, {response.text}")
        return False, None

# Функція для перевірки користувача і запису в базу
def verify_and_add_user(phone_number, telegram_id, telegram_name):
    """
    1. Перевіряє, чи користувач активний у таблиці Employees Power BI.
    2. Якщо активний – записує в users зі статусом 'active'.
    3. Якщо не активний – записує в users зі статусом 'deleted'.
    """
    is_active, employee_name = is_phone_number_in_power_bi(phone_number)

    status = 'active' if is_active else 'deleted'

    add_telegram_user(phone_number, telegram_id, telegram_name, employee_name, status)

    print(f"Користувач {phone_number} доданий/оновлений зі статусом {status}.")

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

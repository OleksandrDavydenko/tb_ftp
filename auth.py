import re
import requests
import os
from db import add_telegram_user, get_user_status, get_employee_name  # Імпортуємо функцію перевірки статусу
import logging


# Функція для нормалізації номера телефону (залишає лише останні 9 цифр)
def normalize_phone_number(phone_number):
    """
    Нормалізує телефонний номер:
    - Видаляє всі нецифрові символи.
    - Додає код країни, якщо його немає.
    """
    digits = re.sub(r'\D', '', phone_number)
    if len(digits) == 9:  # Якщо номер без коду країни
        return f"380{digits}"
    elif len(digits) == 12 and digits.startswith("380"):  # Якщо номер із кодом країни
        return digits
    else:
        return digits  # Повертає те, що залишилось



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

        # Нормалізуємо всі номери з Power BI
        phone_map = {
            normalize_phone_number(row.get('[PhoneNumber]', '')): (row.get('[Employee]', ''), row.get('[Status]', ''))
            for row in rows
        }

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
    is_active, employee_name, status_from_power_bi = is_phone_number_in_power_bi(phone_number)
    logging.info(f"📊 Дані Power BI для {phone_number}: Активний={is_active}, Ім'я={employee_name}, Статус={status_from_power_bi}")

    if not employee_name:
        employee_name = get_employee_name(phone_number)
        logging.info(f"ℹ️ Ім'я з бази: {employee_name}")

    new_status = "active" if status_from_power_bi == "Активний" else "deleted"
    current_status = get_user_status(phone_number)
    logging.info(f"🛠️ Поточний статус у БД: {current_status}, Новий статус: {new_status}")

    if current_status != new_status:
        add_telegram_user(phone_number, telegram_id, telegram_name, employee_name, new_status)
        logging.info(f"🔄 Статус оновлено: {phone_number} → {new_status}")
    else:
        logging.info(f"✅ Статус без змін: {phone_number} → {current_status}")




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

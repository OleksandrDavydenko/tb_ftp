""" Отримання токену """

import requests

# Замініть на ваші дані
client_id = '706d72b2-a9a2-4d90-b0d8-b08f58459ef6'
username = 'od@ftpua.com'
password = 'Hq@ssw0rd352'

url = 'https://login.microsoftonline.com/common/oauth2/token'

# Формуємо тіло запиту
body = {
    'grant_type': 'password',
    'resource': 'https://analysis.windows.net/powerbi/api',
    'client_id': client_id,
    'username': username,
    'password': password
}

# Виконання POST-запиту
response = requests.post(url, data=body, headers={
    'Content-Type': 'application/x-www-form-urlencoded'
})

# Виведення коду статусу для перевірки успішності
print(response.status_code)  # Має бути 200, якщо запит успішний
print(response.text)         # Перевірка відповіді на наявність помилок або даних

# Отримання токена з відповіді
token = response.json().get('access_token')
print("Access Token:", token)





""" Запит з токеном до Датасету """


dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'

# Заголовки з авторизацією
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Тіло запиту
query_data = {
    "queries": [
        {
            "query": "EVALUATE SELECTCOLUMNS(Deb, \"Client\", Deb[Client], \"Sum_$\", Deb[Sum_$])"
        }
    ],
    "serializerSettings": {
        "includeNulls": True
    }
}

# Виконання запиту
response = requests.post(power_bi_url, headers=headers, json=query_data)

# Виведення результату
if response.status_code == 200:
    print("Дані отримані успішно:")
    print(response.json())
else:
    print(f"Помилка {response.status_code}: {response.text}")

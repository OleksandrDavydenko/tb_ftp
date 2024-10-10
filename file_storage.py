import json
import os

# Шлях до папки для збереження даних
DATA_DIR = 'data'
DATA_FILE = os.path.join(DATA_DIR, 'user_data.json')

# Переконуємося, що папка 'data' існує
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# Функція для завантаження даних з файлу
def load_user_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            return json.load(f)
    return {}

# Функція для збереження даних у файл
def save_user_data(data):
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=4)

# Функція для додавання нового користувача
def add_user(phone_number, telegram_id, first_name, last_name):
    data = load_user_data()
    data[phone_number] = {
        'telegram_id': telegram_id,
        'first_name': first_name,
        'last_name': last_name
    }
    save_user_data(data)

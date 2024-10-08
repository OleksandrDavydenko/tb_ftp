import pandas as pd
import sqlite3

def setup_database():
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()

    # Створення таблиці для зберігання інформації про користувачів
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone_number TEXT NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        code TEXT NOT NULL
    )
    ''')
    
    # Додавання тестових користувачів
    cursor.execute('INSERT INTO users (phone_number, first_name, last_name, code) VALUES (?, ?, ?, ?)', 
                   ('+380633493939', 'Ступа', 'Олександр', '1234'))
    cursor.execute('INSERT INTO users (phone_number, first_name, last_name, code) VALUES (?, ?, ?, ?)', 
                   ('+380931193670', 'Іван', 'Іванов', '5678'))
    cursor.execute('INSERT INTO users (phone_number, first_name, last_name, code) VALUES (?, ?, ?, ?)', 
                   ('+380632773227', 'Петро', 'Петренко', '9101'))

    conn.commit()
    conn.close()

def check_phone_number_in_db(phone_number: str) -> tuple:
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()

    # Очищення номера перед перевіркою
    cleaned_number = phone_number.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    
    # Перевірка, чи починається номер з коду країни
    if not cleaned_number.startswith('+'):
        cleaned_number = '+' + cleaned_number  # Додати код країни, якщо відсутній

    # Перевірка наявності номера телефону в базі даних
    cursor.execute('SELECT * FROM users WHERE phone_number = ?', (cleaned_number,))
    result = cursor.fetchone()
    
    conn.close()
    return result

def process_debt_data(file_path: str):
    # Зчитування CSV файлу
    debtors_df = pd.read_csv(file_path)

    # Заміна менеджерів
    debtors_df['Manager'] = debtors_df['Manager'].replace({'Бабич Максим': 'Олександр Ступа', 
                                                             'Постоєнко Аліна': 'Олександр Урфе'})

    return debtors_df

def get_debt_summary(debtors_df, user_name):
    # Розрахунок загальної суми дебіторської заборгованості в доларах
    total_debt = debtors_df[(debtors_df['Manager'] == user_name) & (debtors_df['Inform'] != 1)]['Sum_$'].sum()
    return total_debt


def get_debt_details(debtors_df, user_name):
    # Отримання сум по кожному контрагенту для користувача
    user_debts = debtors_df[(debtors_df['Manager'] == user_name) & (debtors_df['Inform'] != 1)][['Client', 'Sum_$']]


    return user_debts

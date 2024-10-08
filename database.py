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
                   ('+380633493939', 'Олександр', 'Ступа', '1234'))
    cursor.execute('INSERT INTO users (phone_number, first_name, last_name, code) VALUES (?, ?, ?, ?)', 
                   ('+380931193670', 'Дінара', 'Дінарівна', '5678'))
    cursor.execute('INSERT INTO users (phone_number, first_name, last_name, code) VALUES (?, ?, ?, ?)', 
                   ('+380632773227', 'Олександр', 'Урфе', '9101'))
    
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
    
    print(f"Checking phone number: {cleaned_number}")  # Дебаг
    # Перевірка наявності номера телефону в базі даних
    cursor.execute('SELECT * FROM users WHERE phone_number = ?', (cleaned_number,))
    result = cursor.fetchone()
    
    conn.close()
    return result

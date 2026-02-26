import openai
import os
import logging
from db import save_gpt_query, get_last_gpt_queries
from datetime import datetime
 

# OpenAI API Key (змінна середовища)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Отримуємо абсолютний шлях до файлу ACCOUNTING_POLICY.txt
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POLICY_PATH = os.path.join(BASE_DIR, "ACCOUNTING_POLICY.txt")

# Список команд, які бот вже обробляє окремо
KNOWN_COMMANDS = [
    "/menu", "/debt", "/salary", "/analytics", "/info", "/hr", 
    "📊 Аналітика", "💼 Розрахунковий лист", "📉 Дебіторка (AR)",
    "💱 Курс валют", "Таблиця", "Гістограма", "Діаграма", "Назад", "Головне меню",
    "ℹ️ Інформація", "Перевірка девальвації", "Аналітика за місяць", "Аналітика за рік",
    "2024", "2025","2026","2027", "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
    "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень",
    "Дохід", "Валовий прибуток", "Маржинальність", "Кількість угод",
    "Протермінована дебіторська заборгованість", "🧾 Кадровий облік", "🗓 Залишки відпусток", 
    "🕓 Відпрацьовані дні", "📘 Довідка","💼 Зарплата", "💼 Оклад", "🎁 Відомість Бонуси", 
    "💰 Бонуси", "👑 Премії керівників", "👔 Інформація про стаж", "📜 Відомість керівника",
    "🧾 Опис змін"
]

# Завантаження облікової політики з файлу
def load_policy():
    try:
        with open(POLICY_PATH, "r", encoding="utf-8") as file:
            return file.read()
    except FileNotFoundError:
        logging.error("❌ Файл облікової політики не знайдено.")
        return "Облікова політика недоступна."

ACCOUNTING_POLICY = load_policy()

# Перевірка, чи є повідомлення стандартною командою
def is_known_command(text):
    return text in KNOWN_COMMANDS

# Генерація відповіді від GPT-3.5 Turbo
def get_gpt_response(user_input, user_id, employee_name, message_id):
    if not OPENAI_API_KEY:
        logging.error("❌ API-ключ OpenAI не знайдено.")
        return "Помилка: API-ключ OpenAI не знайдено."

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    # Отримуємо останні 3 повідомлення з БД
    chat_history = get_last_gpt_queries(user_id, limit=2)
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Формуємо початкове повідомлення
    messages = [
        {"role": "system", "content": "Ти - корпоративний фінансовий помічник у Telegram-боті."},
        {"role": "system", "content": f"""
        Сьогоднішня дата: {current_date}.
        Ти повинен відповідати у форматі HTML для Telegram. 
            🔹 Використовуй **тільки** HTML-форматування.
            🔹 Не використовуй Markdown.
            🔹 Посилання повинні бути у форматі: 
            <a href="https://example.com">Текст посилання</a>.
            🔹 Ось приклади форматування, які слід використовувати:
            - <b>Жирний текст</b>
            - <i>Курсив</i>
            - <a href="https://example.com">Клікабельне посилання</a>
            Спочатку перевіряй чи є відповіді на запитання в усьому промті, якщо є то відповідай пріорітетно як в базі знань, а потім вже від себе.
            Якщо питання пов'язано з функціями в боті рекомендуй скористатись функціями бота, якщо в запитаннях прослідковується зв'язок з ними:
            Наприклад: Якщо питання про курс валют, нагадай що є функція отримання готівкового курсу, якщо про ЗП тоді нагадай про розрахунковий лист.

            🔹 **Основна інформація про функції бота**:
            - 📊 <b>Аналітика</b>: Персональна статистика користувача за визначений період (дохід, валовий прибуток, маржинальність, кількість угод). <i>Але немає даних про чистий прибуток.</i>
            - 💼 <b>Розрахунковий лист</b>: Інформація по нарахуванню та виплаті ЗП, премій, бонусів за обраний місяць. <i>Проте інформацію про терміни виплати знайдеш в обліковій політиці.</i>
            - 📉 <b>Дебіторська заборгованість</b>: Поточна дебіторська заборгованість працівника.
            - 📌 <b>Протермінована дебіторська заборгованість</b>: Протермінована дебіторська заборгованість працівника.
            - ℹ️ <b>Інформація</b>:
                - 💱 <b>Курс валют</b>: Актуальний курс валют (готівковий). <i>(Інший курс валют не рекомендуй використовувати. На сьогодні актуальний готівковий курс можна дізнатись в боті.)</i>
                - ✅ <b>Перевірка девальвації</b>: Порівнює курс НБУ на дату виставлення рахунку з поточним курсом.
            - ℹ️ <b>Кадровий облік</b>:
                - 📊 <b>👔 Інформація про стаж</b>: Персональна інформація про стаж роботи</i>


            🔹 **База знань:**:
            {ACCOUNTING_POLICY}

            🔹 **Запит користувача**:
            "{user_input}"
        """}
    ]


    # Додаємо історію діалогу (останні 3 повідомлення)
    for msg in chat_history:
        messages.append(msg)

    # Додаємо поточне питання користувача
    messages.append({"role": "user", "content": user_input})

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=messages,
            temperature=0.3
        )

        gpt_response = response.choices[0].message.content

        # Логування використаних токенів
        logging.info(f"🔹 Використано токенів: {response.usage.total_tokens}")

        # Збереження запиту та відповіді у БД з message_id
        save_gpt_query(user_id, employee_name, user_input, gpt_response, message_id)

        # Перевіряємо, чи варто рекомендувати команду бота
        recommended_command = recommend_bot_function(user_input, gpt_response)
        if recommended_command:
            gpt_response += f'\n\nℹ️ <b>Для цього у боті є вбудована функція!</b>\nВикористайте команду: {recommended_command}'

        return gpt_response

    except Exception as e:
        logging.error(f"❌ Помилка виклику OpenAI API: {e}")
        return "Помилка під час отримання відповіді. Спробуйте пізніше."

# Функція рекомендації команди бота
def recommend_bot_function(user_input, gpt_response):
    lower_input = user_input.lower()

    # Визначаємо, чи є запит відповідним до функцій бота
    if "зарплата" in lower_input or "виплата" in lower_input or "зп" in lower_input:
        return "/salary"
    elif "дебітор" in lower_input:
        return "/debt"
    elif "аналітик" in lower_input:
        return "/analytics"
    elif "курс валют" in lower_input or "валюта" in lower_input:
        return "/info"
    elif "девальвація" in lower_input:
        return "/info"

    return None

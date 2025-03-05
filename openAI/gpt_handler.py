import openai
import os
import logging
from db import save_gpt_query

# OpenAI API Key (змінна середовища)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Отримуємо абсолютний шлях до файлу ACCOUNTING_POLICY.txt
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POLICY_PATH = os.path.join(BASE_DIR, "ACCOUNTING_POLICY.txt")

# Список команд, які бот вже обробляє окремо
KNOWN_COMMANDS = [
    "/menu", "/debt", "/salary", "/analytics", "/info",  # Додані команди
    "📊 Аналітика", "💼 Розрахунковий лист", "📉 Дебіторська заборгованість",
    "💱 Курс валют", "Таблиця", "Гістограма", "Діаграма", "Назад", "Головне меню",
    "ℹ️ Інформація", "Перевірка девальвації", "Аналітика за місяць", "Аналітика за рік",
    "2024", "2025", "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
    "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень",
    "Дохід", "Валовий прибуток", "Маржинальність", "Кількість угод",
    "Протермінована дебіторська заборгованість"
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
def get_gpt_response(user_input, user_id, employee_name):
    if not OPENAI_API_KEY:
        logging.error("❌ API-ключ OpenAI не знайдено.")
        return "Помилка: API-ключ OpenAI не знайдено."

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    # Формуємо інструкцію для GPT
    prompt = f"""
        Ти - корпоративний фінансовий помічник у Telegram боті. Відповідай на запитання користувачів, використовуючи облікову політику компанії та рекомендуючи відповідні функції бота, де це доречно.

        🔹 **Облікова політика компанії**:
        {ACCOUNTING_POLICY}

        🔹 **Основна інформація про функції бота**:
        - 📊 Аналітика: Персональна статистика користувача за визначений період (дохід, валовий прибуток, маржинальність, кількість угод) Але немає даних про Чистий прибуток.
        - 💼 Розрахунковий лист: Інформація по нарахуванню та виплаті ЗП, премій, бонусів за обраний місяць. Проте інформацію про терміни виплати знайдеш в обліковій політиці.
        - 📉 Дебіторська заборгованість: Поточна дебіторська заборгованість працівника.
        - 📌 Протермінована дебіторська заборгованість: Протермінована дебіторська заборгованість працівника.
        - ℹ️ Інформація:
            - 💱 Курс валют: Актуальний курс валют (готівковий).
            - ✅ Перевірка девальвації: Порівнює курс НБУ на дату виставлення рахунку з поточним курсом.

        🔹 **Запит користувача**:
        "{user_input}"
        ⚠️ **Не запитуй користувача про додаткову інформацію або уточнення. Відповідай лише конкретними фактами на основі облікової політики.**
        """

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": prompt}],
            temperature=0.2
        )

        gpt_response = response.choices[0].message.content

        # Отримуємо використані токени
        total_tokens = response.usage.total_tokens
        prompt_tokens = response.usage.prompt_tokens
        completion_tokens = response.usage.completion_tokens

        # 📌 Логування використаних токенів
        logging.info(f"🔹 Використано токенів: {total_tokens} (запит: {prompt_tokens}, відповідь: {completion_tokens})")

        # 📌 Збереження запиту та відповіді у базі
        save_gpt_query(user_id, employee_name, user_input, gpt_response)

        # Перевіряємо, чи запит відповідає вбудованій функції
        recommended_command = recommend_bot_function(user_input)
      
        if recommended_command:
          gpt_response += f'\n\nℹ️ <b>Для цього у боті є вбудована функція!</b>\nВикористайте команду: <code>{recommended_command}</code>.'


        return gpt_response

    except Exception as e:
        logging.error(f"❌ Помилка виклику OpenAI API: {e}")
        return "Помилка під час отримання відповіді. Спробуйте пізніше."

# Функція рекомендації команди бота на основі запиту користувача
def recommend_bot_function(user_input):
    lower_input = user_input.lower()

    # Визначаємо, чи запит стосується однієї з вбудованих функцій
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

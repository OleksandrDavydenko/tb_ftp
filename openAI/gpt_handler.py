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
def get_gpt_response(user_input, user_id, username):
    if not OPENAI_API_KEY:
        logging.error("❌ API-ключ OpenAI не знайдено.")
        return "Помилка: API-ключ OpenAI не знайдено."

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": f"Ти - корпоративний фінансовий помічник. Відповідай лише на основі облікової політики:\n{ACCOUNTING_POLICY}"},
                {"role": "user", "content": user_input}
            ],
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
        save_gpt_query(user_id, username, user_input, gpt_response)

        return gpt_response

    except Exception as e:
        logging.error(f"❌ Помилка виклику OpenAI API: {e}")
        return "Помилка під час отримання відповіді. Спробуйте пізніше."

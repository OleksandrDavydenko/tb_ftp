import openai
import os

# OpenAI API Key (змінна середовища)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Отримуємо абсолютний шлях до файлу ACCOUNTING_POLICY.txt
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POLICY_PATH = os.path.join(BASE_DIR, "ACCOUNTING_POLICY.txt")

# Список команд, які бот вже обробляє окремо
KNOWN_COMMANDS = [
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
        return "Облікова політика недоступна."

ACCOUNTING_POLICY = load_policy()

# Перевірка, чи є повідомлення стандартною командою
def is_known_command(text):
    return text in KNOWN_COMMANDS

# Генерація відповіді від GPT-3.5 Turbo
def get_gpt_response(user_input):
    if not OPENAI_API_KEY:
        return "Помилка: API-ключ OpenAI не знайдено."

    openai.api_key = OPENAI_API_KEY

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": f"Ти - корпоративний фінансовий помічник. Відповідай лише на основі облікової політики:\n{ACCOUNTING_POLICY}"},
            {"role": "user", "content": user_input}
        ],
        temperature=0.2
    )

    return response["choices"][0]["message"]["content"]

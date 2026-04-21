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

OUT_OF_SCOPE_RESPONSE = (
    "Вибач, я допомагаю лише з фінансовими та довідковими питаннями в межах цього бота. "
    "Скористайтесь /menu або /info."
)

IN_SCOPE_KEYWORDS = [
    "зарп", "зп", "виплат", "бонус", "премі", "оклад", "kpi",
    "дебітор", "заборг", "кредитор", "валют", "курс", "девальвац",
    "фінанс", "бухгалтер", "звіт", "прибут", "дохід", "собіварт",
    "power bi", "powerbi", "профіт", "облік", "амортизац",
    "контакт", "пошта", "email", "директор", "юридич", "it", "інструкц",
    "кадров", "стаж", "відпуст", "відпрацьован", "довідка", "бот",
    "переговор", "канцеляр", "crm", "етик", "обшук", "броню"
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


def is_in_scope_query(text):
    lower_text = text.lower()
    return any(keyword in lower_text for keyword in IN_SCOPE_KEYWORDS)

# Генерація відповіді від GPT-3.5 Turbo
def get_gpt_response(user_input, user_id, employee_name, message_id):
    if not OPENAI_API_KEY:
        logging.error("❌ API-ключ OpenAI не знайдено.")
        return "Помилка: API-ключ OpenAI не знайдено."

    normalized_input = (user_input or "").strip()
    if not normalized_input:
        return "Будь ласка, сформулюйте запит."

    if not is_in_scope_query(normalized_input):
        return OUT_OF_SCOPE_RESPONSE

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    # Отримуємо останні повідомлення з БД для короткого контексту
    chat_history = get_last_gpt_queries(user_id, limit=2)
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Формуємо початкове повідомлення
    messages = [
        {"role": "system", "content": "Ти - корпоративний фінансовий помічник у Telegram-боті."},
        {"role": "system", "content": f"""
        Сьогоднішня дата: {current_date}.
        Відповідай тільки у форматі HTML для Telegram.
        Використовуй лише теги <b>, <i>, <a href="..."></a>, <br> та списки через рядки.
        Не використовуй Markdown.

        Межі тем:
        - фінансова звітність, зарплата, премії, бонуси, дебіторська/кредиторська заборгованість;
        - курс валют, девальвація;
        - довідкові контакти, інструкції, організаційні правила з бази знань;
        - функції цього Telegram-бота.

        Якщо запит поза межами тем, відповідай тільки так:
        {OUT_OF_SCOPE_RESPONSE}

        Джерело істини:
        - відповідай фактами лише з бази знань нижче та опису функцій бота;
        - нічого не вигадуй;
        - якщо точних даних немає, пиши: "У базі знань немає підтвердженої інформації по цьому запиту.".

        Стиль відповіді:
        - максимум 5 коротких речень;
        - без зайвих деталей і без загальних міркувань;
        - якщо релевантно, рекомендуй одну команду бота.

        Основна інформація про функції бота:
        - 📊 <b>Аналітика</b>: Персональна статистика користувача за період (дохід, валовий прибуток, маржинальність, кількість угод).
        - 💼 <b>Розрахунковий лист</b>: Нарахування та виплата зарплати, премій, бонусів за місяць.
        - 📉 <b>Дебіторська заборгованість</b>: Поточна дебіторська заборгованість працівника.
        - 📌 <b>Протермінована дебіторська заборгованість</b>: Прострочена дебіторська заборгованість працівника.
        - ℹ️ <b>Інформація</b>: готівковий курс валют і перевірка девальвації.
        - 🧾 <b>Кадровий облік</b>: зокрема 👔 інформація про стаж.

        База знань:
            {ACCOUNTING_POLICY}
        """}
    ]


    # Додаємо історію діалогу (останні 3 повідомлення)
    for msg in chat_history:
        messages.append(msg)

    # Додаємо поточне питання користувача
    messages.append({"role": "user", "content": normalized_input})

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
        recommended_command = recommend_bot_function(normalized_input)
        if recommended_command:
            gpt_response += f'\n\nℹ️ <b>Для цього у боті є вбудована функція!</b>\nВикористайте команду: {recommended_command}'

        return gpt_response

    except Exception as e:
        logging.error(f"❌ Помилка виклику OpenAI API: {e}")
        return "Помилка під час отримання відповіді. Спробуйте пізніше."

# Функція рекомендації команди бота
def recommend_bot_function(user_input):
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

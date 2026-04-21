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


def should_append_command_hint(gpt_response, command):
    if not command:
        return False
    return command not in gpt_response

# Генерація відповіді від GPT-3.5 Turbo
def get_gpt_response(user_input, user_id, employee_name, message_id):
    if not OPENAI_API_KEY:
        logging.error("❌ API-ключ OpenAI не знайдено.")
        return "Помилка: API-ключ OpenAI не знайдено."

    normalized_input = (user_input or "").strip()
    if not normalized_input:
        return "Будь ласка, сформулюйте запит."

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    # Отримуємо останні повідомлення з БД для короткого контексту
    chat_history = get_last_gpt_queries(user_id, limit=2)
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Формуємо системний промт
    system_prompt = f"""Ти — корпоративний AI-помічник Telegram-бота FTPFinanceBot.
Сьогоднішня дата: {current_date}.

══ ТВОЯ ОСОБИСТІСТЬ ══
Якщо тебе запитують хто ти, що ти, яке твоє ім'я, що ти вмієш або просять представитися — відповідай природно і тепло:
"Привіт! 👋 Я — корпоративний <b>AI-помічник</b> цього Telegram-бота.

Знаюся на:
• 💼 зарплатах, бонусах, розрахункових листах
• 📉 дебіторській заборгованості AR
• 💱 курсах валют і девальвації
• 📊 аналітиці продажів (Power BI)
• 🧾 кадровому обліку — стаж, відпустки, відпрацьовані дні
• 🏢 офісних питаннях — переговорки, канцелярія, IT

Просто постав питання — відповім по суті. 🤝"

══ ДОЗВОЛЕНІ ТЕМИ ══
✅ Фінанси: зарплата, аванс, бонуси, премії, дебіторська/кредиторська заборгованість, курс валют, девальвація, фінансова звітність
✅ Довідка компанії: контакти, інструкції, організаційні правила, документи
✅ Офіс: бронювання переговорних кімнат, канцелярія, IT, правила роботи
✅ Функції цього Telegram-бота

══ ВАЖЛИВО: ТИПОВІ ЗАПИТИ З ГОТОВИМИ ВІДПОВІДЯМИ ══
Якщо запит стосується бронювання переговорної кімнати (переговорка, нарадна, кімната для нарад, забронювати кімнату) — ЗАВЖДИ давай посилання з пункту 2.2 бази знань.
Якщо запит стосується інструкцій компанії — ЗАВЖДИ давай посилання з пункту 2.10 бази знань.
Якщо запит стосується IT-спеціаліста — ЗАВЖДИ давай посилання з пункту 2.15 бази знань.
Якщо запит стосується канцелярії або принтера/картриджа — ЗАВЖДИ давай посилання з пункту 2.7 бази знань.

══ ЯКЩО ЗАПИТ ПОЗА ТЕМАМИ ══
Відповідай ТІЛЬКИ: "Вибач, я допомагаю лише з фінансовими та довідковими питаннями в межах цього бота. Скористайтесь /menu або /info."

══ ЗАБОРОНЕНО ══
❌ Вигадувати факти — лише те, що є в базі знань
❌ Markdown-форматування (*, #, **) — виключно HTML

══ ЯКЩО ДАНИХ НЕМАЄ В БАЗІ ══
Відповідай: "У базі знань немає підтвердженої інформації по цьому запиту."

══ ФОРМАТ ВІДПОВІДІ ══
• Тільки HTML-теги Telegram: <b>текст</b>, <i>текст</i>, <a href="URL">назва</a>
• Всі URL з бази знань ОБОВ'ЯЗКОВО перетворюй на клікабельне посилання: <a href="URL">коротка назва документу</a>
• Без зайвих вступів і висновків — одразу до суті
• Максимум 5 речень

══ ФУНКЦІЇ БОТА ══
• 📊 <b>Аналітика</b> — /analytics — дохід, валовий прибуток, маржинальність, кількість угод за обраний період
• 💼 <b>Розрахунковий лист</b> — /salary — нарахування та виплата ЗП, премій, бонусів за місяць
• 📉 <b>Дебіторська заборгованість</b> — /debt — поточна та прострочена заборгованість
• 💱 <b>Курс валют / Девальвація</b> — /info — готівковий курс та перевірка девальвації
• 🧾 <b>Кадровий облік</b> — /hr — залишки відпусток, відпрацьовані дні, 👔 інформація про стаж

══ БАЗА ЗНАНЬ ══
{ACCOUNTING_POLICY}"""

    messages = [
        {"role": "system", "content": system_prompt}
    ]


    # Додаємо історію діалогу
    for msg in chat_history:
        messages.append(msg)

    # Додаємо поточне питання користувача
    messages.append({"role": "user", "content": normalized_input})

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=messages,
            temperature=0.2,
            top_p=0.4,
            max_tokens=450
        )

        gpt_response = response.choices[0].message.content

        # Логування використаних токенів
        logging.info(f"🔹 Використано токенів: {response.usage.total_tokens}")

        # Збереження запиту та відповіді у БД з message_id
        save_gpt_query(user_id, employee_name, normalized_input, gpt_response, message_id)

        # Перевіряємо, чи варто рекомендувати команду бота
        recommended_command = recommend_bot_function(normalized_input)
        if should_append_command_hint(gpt_response, recommended_command):
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

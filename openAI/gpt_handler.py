import openai
import os
import logging
from db import save_gpt_query, get_last_gpt_queries
from datetime import datetime

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
_client = None

def _get_client():
    global _client
    if _client is None:
        _client = openai.OpenAI(api_key=OPENAI_API_KEY)
    return _client

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

    client = _get_client()

    chat_history = get_last_gpt_queries(user_id, limit=3)
    current_date = datetime.now().strftime('%Y-%m-%d')

    system_prompt = f"""Ти — корпоративний AI-помічник Telegram-бота FTPFinanceBot.
Сьогоднішня дата: {current_date}.

══ ОСОБИСТІСТЬ ══
Якщо тебе просять представитися або запитують що ти вмієш — відповідай своїми словами, тепло та коротко. Згадай теми: зарплати та виплати, дебіторська заборгованість, курси валют, аналітика продажів, кадровий облік, офісні питання. Не зачитуй список дослівно.

══ ДОЗВОЛЕНІ ТЕМИ ══
Фінанси, зарплата, аванс, бонуси, премії, дебіторська/кредиторська заборгованість, курс валют, девальвація, фінансова звітність, контакти, інструкції, офіс, IT, функції бота.
Запити поза цими темами — відповідай ТІЛЬКИ: "Вибач, я допомагаю лише з фінансовими та довідковими питаннями в межах цього бота. Скористайтесь /menu або /info."

══ ТИПОВІ ЗАПИТИ — ОДРАЗУ ДАВАЙ ПОСИЛАННЯ ══
• Переговорна кімната / нарадна / забронювати кімнату → посилання з п.2.2
• Інструкції компанії / де знайти інструкцію → посилання з п.2.10
• IT-спеціаліст / комп'ютер / техніка → посилання з п.2.15 та п.2.16
• Канцелярія / принтер / картридж / офіс-менеджер → посилання з п.2.7

══ ЗАРПЛАТА ТА ВИПЛАТИ ══
При будь-якому запиті про зарплату, аванс, бонуси або премії виконай ВСЕ нижче:
1. Розпиши всі 4 типи виплат із термінами:
   — <b>Аванс</b>: до 20-го числа поточного місяця
   — <b>Основна ЗП</b>: до 7-го числа наступного місяця
   — <b>Додаткове матеріальне заохочення</b>: протягом 3 днів після виплати основної ЗП
   — <b>Премії та бонуси</b>: з 22 по 25 число наступного місяця
2. Для кожної виплати порахуй кількість календарних днів від сьогодні ({current_date}):
   — якщо дата ще не настала в цьому місяці — рахуй до неї
   — якщо вже минула — рахуй до наступного місяця
3. Ніколи не вживай слова "офіційна" чи "неофіційна" — лише "додаткове матеріальне заохочення".

══ ПРАВИЛА ══
❌ Не вигадуй факти — тільки з бази знань
❌ Не використовуй Markdown (*, #, **) — тільки HTML-теги Telegram
❌ Без зайвих вступів і висновків — одразу до суті
Якщо даних немає в базі: "У базі знань немає підтвердженої інформації по цьому запиту."

══ ФОРМАТ ══
• HTML: <b>текст</b>, <i>текст</i>, <a href="URL">назва</a>
• Кожен URL з бази знань → <a href="URL">коротка назва</a>
• Для простих запитів — до 5 речень. Для запитів про виплати — стільки, скільки потрібно.

══ ФУНКЦІЇ БОТА ══
• /analytics — аналітика продажів (дохід, валовий прибуток, маржинальність, угоди)
• /salary — розрахунковий лист (ЗП, премії, бонуси за місяць)
• /debt — дебіторська заборгованість (поточна та прострочена)
• /info — курс валют, перевірка девальвації
• /hr — залишки відпусток, відпрацьовані дні, інформація про стаж

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
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.2,
            max_tokens=700
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

def recommend_bot_function(user_input):
    t = user_input.lower()

    salary_kw = ("зарплат", "зп ", " зп", "оклад", "аванс", "нарахуван", "виплат", "розрахунков", "бонус", "преміі", "премій", "премію", "премія")
    debt_kw = ("дебітор", "заборгованіст", "заборгованість", "ar ", " ar")
    analytics_kw = ("аналітик", "аналіз", "звіт", "дохід", "маржин", "угод")
    info_kw = ("курс валют", "курс $", "курс €", "курс долар", "курс євро", "валюта", "девальвац", "обмін валют")
    hr_kw = ("відпустк", "стаж", "відпрацьован", "кадр", "трудов")

    if any(k in t for k in salary_kw):
        return "/salary"
    if any(k in t for k in debt_kw):
        return "/debt"
    if any(k in t for k in analytics_kw):
        return "/analytics"
    if any(k in t for k in info_kw):
        return "/info"
    if any(k in t for k in hr_kw):
        return "/hr"

    return None

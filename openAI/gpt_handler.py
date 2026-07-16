import openai
import os
import re
import logging
import numpy as np
from datetime import datetime
from db import save_gpt_query, get_db_connection

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
_client = None

# Моделі
CHAT_MODEL = "gpt-4.1"
EMBEDDING_MODEL = "text-embedding-3-small"


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
    "2024", "2025", "2026", "2027", "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
    "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень",
    "Дохід", "Валовий прибуток", "Маржинальність", "Кількість угод",
    "Протермінована дебіторська заборгованість", "🧾 Кадровий облік", "🗓 Залишки",
    "🕓 Відпрацьовано", "📘 Довідка", "💼 Зарплата", "💼 Оклад", "🎁 Відомість Бонуси",
    "💰 Бонуси", "👑 Премії керівників", "👔 Стаж", "📜 Відомість керівника",
    "🧾 Опис змін", "📊 Звіт В/Л", "🐞 Bug Bounty", "💡 Нові ідеї",
]


# ──────────────────────────────────────────────────────────────────────────
# БАЗА ЗНАНЬ + СЕМАНТИЧНИЙ RAG
# ──────────────────────────────────────────────────────────────────────────

# Завантаження облікової політики з файлу
def load_policy():
    try:
        with open(POLICY_PATH, "r", encoding="utf-8") as file:
            return file.read()
    except FileNotFoundError:
        logging.error("❌ Файл облікової політики не знайдено.")
        return "Облікова політика недоступна."


# Кеш індексу в пам'яті
_CHUNKS = None          # list[str]
_CHUNK_VECS = None       # np.ndarray (n, d), нормалізовані
_POLICY_MTIME = None     # для перебудови при зміні файлу

# Ключові слова критичних чанків, які додаємо завжди (незалежно від скору),
# щоб бот завжди міг розписати виплати та пояснити методики розрахунків.
_ALWAYS_INCLUDE_HINTS = ("терміни виплат", "методики розрахунк", "маржинальн", "девальвац")


def _split_policy_into_chunks(text):
    """Ділить політику на чанки за нумерованими секціями (1., 2.1, 7.2 тощо).

    Кожен пункт довідки (контакт/посилання) стає окремим чанком — це дає
    точніший семантичний підбір.
    """
    lines = text.split("\n")
    chunks = []
    current = []
    # Рядок, що починається з номера секції/підсекції: "2", "2.1", "7.2 ..."
    section_re = re.compile(r"^\s*\d+(\.\d+)*[\.\s)]")

    for line in lines:
        if section_re.match(line) and current:
            chunk = "\n".join(current).strip()
            if chunk:
                chunks.append(chunk)
            current = [line]
        else:
            current.append(line)

    if current:
        chunk = "\n".join(current).strip()
        if chunk:
            chunks.append(chunk)

    # Прибираємо порожні та зливаємо надто короткі «хвости» (заголовки без тіла)
    cleaned = [c for c in chunks if len(c.strip()) > 0]
    return cleaned


def _embed_texts(texts):
    """Батч-ембединг. Повертає нормалізований np.ndarray (n, d)."""
    client = _get_client()
    resp = client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    vecs = np.array([item.embedding for item in resp.data], dtype=np.float32)
    # L2-нормалізація → косинус = dot
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return vecs / norms


def _ensure_index():
    """Лінива одноразова побудова RAG-індексу. Перебудова при зміні файлу.

    Повертає True, якщо індекс готовий, інакше False (буде fallback на повний текст).
    """
    global _CHUNKS, _CHUNK_VECS, _POLICY_MTIME

    try:
        mtime = os.path.getmtime(POLICY_PATH)
    except OSError:
        mtime = None

    if _CHUNKS is not None and _CHUNK_VECS is not None and _POLICY_MTIME == mtime:
        return True

    policy = load_policy()
    if not policy or policy == "Облікова політика недоступна.":
        return False

    chunks = _split_policy_into_chunks(policy)
    if not chunks:
        return False

    try:
        vecs = _embed_texts(chunks)
    except Exception as e:
        logging.error(f"❌ Не вдалося побудувати ембединги бази знань: {e}")
        return False

    _CHUNKS = chunks
    _CHUNK_VECS = vecs
    _POLICY_MTIME = mtime
    logging.info(f"✅ RAG-індекс побудовано: {len(chunks)} чанків.")
    return True


def _retrieve(query, k=4, min_score=0.20):
    """Підбирає релевантні чанки бази знань під запит.

    Завжди додає критичні чанки (терміни виплат, методики розрахунків).
    Якщо індекс недоступний — повертає весь текст політики (fallback).
    """
    if not _ensure_index():
        return load_policy()

    try:
        q_vec = _embed_texts([query])[0]
    except Exception as e:
        logging.error(f"❌ Помилка ембедингу запиту: {e}")
        return load_policy()

    scores = _CHUNK_VECS @ q_vec  # косинусна подібність
    order = np.argsort(scores)[::-1]

    selected_idx = []
    for i in order[:k]:
        if scores[i] >= min_score:
            selected_idx.append(int(i))

    # Завжди додаємо критичні чанки
    for i, chunk in enumerate(_CHUNKS):
        low = chunk.lower()
        if any(h in low for h in _ALWAYS_INCLUDE_HINTS) and i not in selected_idx:
            selected_idx.append(i)

    # Якщо нічого не пройшло поріг і немає критичних — беремо top-2 як підстраховку
    if not selected_idx:
        selected_idx = [int(i) for i in order[:2]]

    # Зберігаємо порядок за релевантністю
    selected_idx = sorted(set(selected_idx), key=lambda i: -scores[i])
    return "\n\n".join(_CHUNKS[i] for i in selected_idx)


ACCOUNTING_POLICY = load_policy()


# ──────────────────────────────────────────────────────────────────────────
# ІСТОРІЯ ДІАЛОГУ (виправлено: зберігаємо і питання, і відповідь)
# ──────────────────────────────────────────────────────────────────────────

def _get_chat_history(user_id, limit=5):
    """Повертає останні N пар (питання → відповідь) як коректне чергування
    user/assistant повідомлень у хронологічному порядку.

    Раніше історія містила лише повідомлення користувача, тож модель не бачила
    власних відповідей і втрачала контекст. Тут будуємо обидві ролі.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT query, response
            FROM gpt_queries_logs
            WHERE user_id = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (user_id, limit),
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        messages = []
        for query, response in rows[::-1]:  # хронологічно
            if query:
                messages.append({"role": "user", "content": query})
            if response:
                messages.append({"role": "assistant", "content": response})
        return messages
    except Exception as e:
        logging.error(f"❌ Помилка при отриманні історії GPT-запитів: {e}")
        return []


# ──────────────────────────────────────────────────────────────────────────
# СОЦІАЛЬНІ ІНТЕНТИ (подяки, вітання, підтвердження)
# ──────────────────────────────────────────────────────────────────────────

_THANKS_KW = (
    "дякую", "дякс", "дяк", "спасибі", "спасиб", "вдячний", "вдячна",
    "thanks", "thank you", "thx", "ти найкращий", "клас бот",
)
_GREETING_KW = ("привіт", "вітаю", "добрий день", "доброго дня", "добрий ранок", "hello", "hi ")
_ACK_KW = ("ок", "окей", "зрозуміло", "зрозумів", "зрозуміла", "супер", "чудово", "класно", "добре")


def _detect_social_intent(text):
    """Розпізнає короткі соціальні репліки. Повертає 'thanks'|'greeting'|'ack'|None."""
    t = (text or "").strip().lower()
    if not t:
        return None
    # Лише для коротких повідомлень, щоб не плутати з реальними питаннями
    if len(t) > 40:
        return None
    if any(k in t for k in _THANKS_KW):
        return "thanks"
    if any(t.startswith(k) or t == k.strip() for k in _GREETING_KW):
        return "greeting"
    if t in _ACK_KW or any(t == k for k in _ACK_KW):
        return "ack"
    return None


# Перевірка, чи є повідомлення стандартною командою
def is_known_command(text):
    return text in KNOWN_COMMANDS


def should_append_command_hint(gpt_response, command):
    if not command:
        return False
    return command not in gpt_response


# ──────────────────────────────────────────────────────────────────────────
# ОСНОВНА ГЕНЕРАЦІЯ ВІДПОВІДІ
# ──────────────────────────────────────────────────────────────────────────

def _build_system_prompt(current_date, knowledge):
    return f"""Ти — корпоративний AI-помічник Telegram-бота @FreightTransportPartnerBot (https://t.me/FreightTransportPartnerBot). Спілкуйся тепло, природно та по-людськи, як уважний колега. Відповідай українською.
Сьогоднішня дата: {current_date}.

══ ОСОБИСТІСТЬ ══
Якщо просять представитися — відповідай своїми словами, тепло й коротко. Можеш допомогти з темами: зарплати та виплати, дебіторська заборгованість, курси валют, аналітика продажів, кадровий облік, офісні та довідкові питання. Не зачитуй список дослівно.

══ ПОДЯКИ ТА СОЦІАЛЬНІ РЕПЛІКИ ══
Якщо користувач дякує — ЗАВЖДИ відповідай тепло: «Будь ласка! Завжди радий допомогти, звертайтесь 🙌» (варіюй формулювання). Ніколи не відмовляй на подяку і не кажи, що це поза темою. На вітання — привітайся у відповідь. На «ок/зрозуміло» — коротко підтримай.

══ ВТРАЧЕНИЙ КОНТЕКСТ ══
Користувач може реагувати на повідомлення, які бот надсилав НЕ через тебе: сповіщення про нову виплату, меню, графіки дебіторки, курси валют, нагадування. Ти цих повідомлень не бачиш. Тому якщо запит схожий на уточнення без контексту («а чому така сума?», «що це означає?», «коли прийде?») — НЕ відмовляй. Натомість:
1. Тепло припусти, про яку функцію бота, ймовірно, йдеться.
2. Постав одне коротке уточнювальне питання.
3. Запропонуй пояснити методику відповідного розрахунку.

══ ЗАРПЛАТА ТА ВИПЛАТИ ══
Якщо у поточному діалозі терміни виплат ще не були названі:
1. Розпиши всі 4 типи виплат із термінами:
   — <b>Аванс</b>: до 20-го числа поточного місяця
   — <b>Основна ЗП</b>: до 7-го числа наступного місяця
   — <b>Додаткове матеріальне заохочення</b>: протягом 3 днів після виплати основної ЗП
   — <b>Премії та бонуси</b>: з 22 по 25 число наступного місяця
2. Для кожної виплати порахуй кількість календарних днів від сьогодні ({current_date}): якщо дата ще не настала цього місяця — рахуй до неї; якщо минула — до наступного місяця.
Якщо терміни вже були наведені у попередній відповіді — не повторюй їх, а відповідай лише на конкретне уточнення користувача.
3. Ніколи не вживай слів «офіційна»/«неофіційна» — лише «додаткове матеріальне заохочення».

══ ПОЯСНЕННЯ МЕТОДИК ══
Якщо запитують ЯК щось рахується (маржинальність, валовий прибуток, девальвація, відпустки, виплати) — поясни методику простими словами на основі бази знань.

══ ПРАВИЛА ══
❌ Не вигадуй факти — лише з бази знань нижче (для конкретних персональних цифр направляй до відповідної функції бота).
❌ Не використовуй Markdown (*, #, **) — лише HTML-теги Telegram.
❌ Без зайвих вступів — одразу до суті, але дружньо.
Якщо даних справді немає в базі — чесно скажи й запропонуй, до кого звернутись або яку функцію бота використати.

══ ФОРМАТ ══
• HTML: <b>текст</b>, <i>текст</i>, <a href="URL">назва</a>
• Кожен URL з бази знань → <a href="URL">коротка назва</a>
• Для простих запитів — стисло (до 5 речень). Для виплат і методик — стільки, скільки потрібно.

══ ФУНКЦІЇ БОТА ══
• /analytics — аналітика продажів (дохід, валовий прибуток, маржинальність, угоди)
• /salary — розрахунковий лист (ЗП, премії, бонуси за місяць)
• /debt — дебіторська заборгованість (поточна та прострочена)
• /info — курс валют, перевірка девальвації
• /hr — залишки відпусток, відпрацьовані дні, інформація про стаж

══ РЕЛЕВАНТНА БАЗА ЗНАНЬ ══
{knowledge}"""


def get_gpt_response(user_input, user_id, employee_name, message_id):
    if not OPENAI_API_KEY:
        logging.error("❌ API-ключ OpenAI не знайдено.")
        return "Помилка: API-ключ OpenAI не знайдено."

    normalized_input = (user_input or "").strip()
    if not normalized_input:
        return "Будь ласка, сформулюйте запит."

    client = _get_client()

    chat_history = _get_chat_history(user_id, limit=5)
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Підбираємо релевантні чанки бази знань під запит (RAG)
    knowledge = _retrieve(normalized_input, k=4)

    system_prompt = _build_system_prompt(current_date, knowledge)

    messages = [{"role": "system", "content": system_prompt}]
    for msg in chat_history:
        messages.append(msg)
    messages.append({"role": "user", "content": normalized_input})

    try:
        response = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=messages,
            temperature=0.4,
            max_tokens=900,
        )

        gpt_response = response.choices[0].message.content

        logging.info(f"🔹 Використано токенів: {response.usage.total_tokens}")

        # Збереження запиту та відповіді у БД з message_id
        save_gpt_query(user_id, employee_name, normalized_input, gpt_response, message_id)

        # Підказку про вбудовану команду НЕ додаємо до соціальних/подячних реплік
        if _detect_social_intent(normalized_input) is None:
            recommended_command = recommend_bot_function(normalized_input)
            if should_append_command_hint(gpt_response, recommended_command):
                gpt_response += (
                    f'\n\nℹ️ <b>Для цього у боті є вбудована функція!</b>\n'
                    f'Використайте команду: {recommended_command}'
                )

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

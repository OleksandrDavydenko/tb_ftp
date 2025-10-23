# information/changelog.py
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext

# ✏️ Редагуй цей список: дата + короткі пункти
CHANGELOG_ENTRIES = [
    ("20.10.2025", [
        "Виконано округлення кількості днів відпусток до цілого числа при відображенні.",
    ]),
    ("21.10.2025", [
        "Додано розділ «Опис змін» у меню Інформація.",
        "Створені короткі запити та відображення лише тих років і місяців, "
        "інформація по яким існує для користувача у розділах «Аналітика» та «Зарплата».",
        "Додано звіт керівника по нарахуванню премій (вивантаження в xlsx).",
    ]),
    ("22.10.2025", [
        "Виправлення помилки головного меню.",
        "Додавання кнопок Назад та Головне меню, після надсилання файлу з Бонусами та Відомість Керівника.",
    ]),
    ("23.10.2025", [
        "Якщо повідомлення Таблиці Дебіторки занадто довге, воно розбивається на частини і надсилається по черзі.",
        
    ]),
]


def _build_changelog_text() -> str:
    """Формує HTML-текст з CHANGELOG_ENTRIES (нові зверху у списку)."""
    if not CHANGELOG_ENTRIES:
        return (
            "🆕 <b>Опис змін</b>\n\n"
            "<b>21.10.2025</b>\n"
            "• Додано розділ «Опис змін» у меню Інформація.\n"
            "• Кнопки «Назад» та «Головне меню» — компактні в один ряд.\n"
        )

    blocks = []
    for date, items in CHANGELOG_ENTRIES:
        lines = "\n".join(f"• {item}" for item in items)
        blocks.append(f"<b>{date}</b>\n{lines}")
    return "🆕 <b>Опис змін</b>\n\n" + "\n\n".join(blocks)


async def _send_long_html(update: Update, html: str, limit: int = 3900) -> None:
    """Відправляє HTML-текст. Якщо довший за ліміт — ріже по найближчому переносу рядка."""
    if len(html) <= limit:
        await update.message.reply_text(html, parse_mode="HTML")
        return

    start = 0
    n = len(html)
    while start < n:
        end = min(start + limit, n)
        if end < n:
            cut = html.rfind("\n", start, end)
            if cut == -1 or cut <= start:
                cut = end
        else:
            cut = end
        await update.message.reply_text(html[start:cut], parse_mode="HTML")
        start = cut + (1 if cut < n and html[cut:cut+1] == "\n" else 0)


async def show_changelog(update: Update, context: CallbackContext) -> None:
    """
    Відображає changelog з цього файлу.
    Кнопки «Назад» і «Головне меню» — компактні та поруч (один ряд).
    """
    text = _build_changelog_text()
    await _send_long_html(update, text)

    reply_markup = ReplyKeyboardMarkup(
        [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

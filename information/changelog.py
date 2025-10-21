# information/changelog.py
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext

# ✏️ Редагуй цей список: дата + короткі пункти
CHANGELOG_ENTRIES = [
    ("21.10.2025", [
        "Додано розділ «Опис змін» у меню Інформація.",
        "Кнопки «Назад» та «Головне меню» стали компактними (в один ряд).",
        "Додано розділ «Перевірка девальвації».",
    ]),
    ("20.10.2025", [
        "Оновлено формат розрахункового листа.",
        "Покращено логування запитів до Power BI.",
    ]),
]

def _build_changelog_text() -> str:
    """Формує HTML-текст з CHANGELOG_ENTRIES (нові зверху)."""
    if not CHANGELOG_ENTRIES:
        # Якщо порожньо — показуємо базову інформацію
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

async def show_changelog(update: Update, context: CallbackContext) -> None:
    """
    Відображає changelog з цього файлу. Кнопки «Назад» і «Головне меню» — маленькі і поруч.
    """
    text = _build_changelog_text()

    # якщо текст дуже довгий — ділимо на частини (ліміт ~4096)
    while text:
        part = text[:3900]
        cut = part.rfind("\n")
        if 0 < cut < len(part):
            part = part[:cut]
        await update.message.reply_text(part, parse_mode="HTML")
        text = text[len(part):].lstrip("\n")

    # компактні кнопки в один ряд
    reply_markup = ReplyKeyboardMarkup(
        [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

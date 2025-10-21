# information/changelog.py
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import os

CHANGELOG_FILE = "data/changelog.txt"

def _ensure_file():
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(CHANGELOG_FILE):
        with open(CHANGELOG_FILE, "w", encoding="utf-8") as f:
            f.write("")

def _send_long_text(update: Update, text: str, chunk=3900):
    # ділимо на частини з запасом до 4096 символів
    while text:
        part = text[:chunk]
        cut = part.rfind("\n")
        if 0 < cut < len(part):
            part = part[:cut]
        update.message.reply_text(part)
        text = text[len(part):].lstrip("\n")

async def show_changelog(update: Update, context: CallbackContext) -> None:
    """
    Показує вміст data/changelog.txt (нові записи — зверху, як ти його напишеш).
    """
    _ensure_file()
    with open(CHANGELOG_FILE, "r", encoding="utf-8") as f:
        content = f.read().strip()

    if not content:
        await update.message.reply_text("ℹ️ Поки що немає записів про оновлення.")
    else:
        # якщо довге — відправляємо частинами
        if len(content) > 3900:
            _send_long_text(update, content)
        else:
            await update.message.reply_text(content)

    back = KeyboardButton("Назад")
    main = KeyboardButton("Головне меню")
    reply_markup = ReplyKeyboardMarkup([[back, main]], one_time_keyboard=True)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

"""
Налаштування логування: одночасно в консоль і в текстовий файл.

Викликати ПЕРШИМ ділом у telegrambot.py, до імпорту решти модулів. Кілька з них
(querryFinanceUa, birthday_greetings, check_devaluation) роблять власний
logging.basicConfig на імпорті, а basicConfig нічого не робить, якщо кореневий
логер уже налаштований. Хто перший — той і визначає формат, тож маємо бути першими.
"""

import logging
import os
import re
from logging.handlers import RotatingFileHandler

LOG_FILE = os.getenv("LOG_FILE", "logs/bot.txt")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Скільки тримаємо: 5 МБ на файл × 3 запасні = 20 МБ максимум
MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 3

FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# httpx пише кожен запит до Telegram разом із токеном у URL
_TOKEN_RE = re.compile(r"bot\d{6,}:[A-Za-z0-9_\-]{20,}")


class RedactSecrets(logging.Filter):
    """Вирізає токен бота з повідомлень — інакше він осідає в кожному рядку."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str) and "bot" in record.msg:
            record.msg = _TOKEN_RE.sub("bot<TOKEN>", record.msg)
        if record.args:
            record.args = tuple(
                _TOKEN_RE.sub("bot<TOKEN>", a) if isinstance(a, str) else a
                for a in record.args
            )
        return True


def setup_logging() -> str | None:
    """Вішає на кореневий логер консоль + файл. Повертає шлях до файлу."""
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    formatter = logging.Formatter(FORMAT)
    redact = RedactSecrets()

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.addFilter(redact)
    root.addHandler(console)

    path = None
    try:
        directory = os.path.dirname(LOG_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        file_handler = RotatingFileHandler(
            LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(redact)
        root.addHandler(file_handler)
        path = os.path.abspath(LOG_FILE)
    except OSError as e:
        # Файлова система лише для читання чи немає місця — консоль має лишитись
        logging.warning(f"[logging] не вдалося відкрити {LOG_FILE}: {e}")

    return path

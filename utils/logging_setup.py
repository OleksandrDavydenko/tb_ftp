"""
Єдине налаштування логування на весь проєкт.

Викликати ПЕРШИМ ділом у telegrambot.py, до імпорту решти модулів. Близько
сорока модулів роблять власний logging.basicConfig прямо на імпорті, а
basicConfig нічого не робить, якщо кореневий логер уже налаштований. Хто
перший — той і визначає формат, тож маємо бути першими.

Пишемо лише в stdout: на Heroku його підбирає платформа, а історію тримає
log drain. Файл на дино сенсу не має — диск обнуляється при кожному рестарті.
"""

import logging
import os
import re

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# httpx друкує рядок на КОЖЕН виклик Telegram, а це getUpdates кожні 10 с —
# 8 640 рядків на добу й ~85% усього обсягу логів. Помилки від цього не
# губляться: на невдалий виклик PTB кидає виняток, який ловлять наші
# обробники (handle_callback_query, on_error) і пишуть своїм рядком.
HTTP_LOG_LEVEL = os.getenv("HTTP_LOG_LEVEL", "WARNING").upper()
FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# httpx логує кожен запит до Telegram разом із токеном прямо в URL
_TOKEN_RE = re.compile(r"bot\d{6,}:[A-Za-z0-9_\-]{20,}")


class RedactSecrets(logging.Filter):
    """
    Вирізає токен бота з повідомлень.

    Без цього він осідає в кожному рядку `HTTP Request`, а отже потрапляє
    і в Heroku-логи, і в будь-який сторонній сервіс, куди їх зливають.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str) and "bot" in record.msg:
            record.msg = _TOKEN_RE.sub("bot<TOKEN>", record.msg)
        if record.args:
            record.args = tuple(
                _TOKEN_RE.sub("bot<TOKEN>", a) if isinstance(a, str) else a
                for a in record.args
            )
        return True


def setup_logging() -> None:
    """Налаштовує кореневий логер: stdout + вирізання секретів."""
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(FORMAT))
    console.addFilter(RedactSecrets())
    root.addHandler(console)

    http_level = getattr(logging, HTTP_LOG_LEVEL, logging.WARNING)
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(http_level)

"""
Єдине налаштування логування на весь проєкт.

Викликати ПЕРШИМ ділом у telegrambot.py, до імпорту решти модулів. Близько
сорока модулів роблять власний logging.basicConfig прямо на імпорті, а
basicConfig нічого не робить, якщо кореневий логер уже налаштований. Хто
перший — той і визначає формат, тож маємо бути першими.

Пишемо лише в stdout: на Heroku його підбирає платформа, а історію тримає
log drain. Файл на дино сенсу не має — диск обнуляється при кожному рестарті.

Звідси ж відкидаємо шум (опитування Telegram) і вирізаємо токен бота.
"""

import logging
import os
import re

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# APScheduler на кожен джоб пише два довгих рядки ("Running job" +
# "executed successfully"), а джоби йдуть кожні 3-15 хвилин — це близько
# 31 МБ логів на місяць. WARNING лишає найцінніше: "Run time of job … was
# missed by …", тобто сигнал, що бот підвисав. INFO — щоб бачити кожен запуск.
SCHEDULER_LOG_LEVEL = os.getenv("SCHEDULER_LOG_LEVEL", "INFO").upper()

# Рядки, які не несуть інформації й лише з'їдають обсяг. Опитування Telegram
# (getUpdates кожні 10 с) — це 8 640 рядків на добу, ~34 МБ на місяць.
# Відкидаємо тільки УСПІШНІ: якщо опитування почне падати, ми це побачимо.
NOISE_PATTERNS = (
    re.compile(r'/getUpdates .*"HTTP/1\.1 2\d\d'),
)
FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# httpx логує кожен запит до Telegram разом із токеном прямо в URL
_TOKEN_RE = re.compile(r"bot\d{6,}:[A-Za-z0-9_\-]{20,}")


class DropNoise(logging.Filter):
    """Відкидає рядки з NOISE_PATTERNS — решта логів проходить як є."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not any(p.search(message) for p in NOISE_PATTERNS)


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
    console.addFilter(DropNoise())
    console.addFilter(RedactSecrets())
    root.addHandler(console)

    scheduler_level = getattr(logging, SCHEDULER_LOG_LEVEL, logging.INFO)
    logging.getLogger("apscheduler").setLevel(scheduler_level)

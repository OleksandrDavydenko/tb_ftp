import asyncio
import logging
import time
from functools import wraps

# Скільки вкладених декорованих викликів зараз працює на чат.
# Хендлери часто викликають один одного (handle_main_menu → show_main_menu,
# handle_callback_query → show_salary_details тощо), і без цього лічильника
# кожен рівень слав би власний sendChatAction — по два-три на одне натискання.
_active_typing: dict[int, int] = {}

# Скільки максимум показуємо «друкує». Якщо хендлер не вклався — щось зависло,
# і краще прибрати індикатор та лишити слід у логах, ніж крутити його вічно.
MAX_TYPING_SECONDS = 120
TYPING_INTERVAL = 4.0

# Скільки хендлер може працювати, поки це вважається нормальним.
# Довше — і callback користувача встигає протухнути в Telegram («Query is too
# old»), а джоби планувальника пропускають розклад. Тому пишемо в лог,
# ЩО саме гальмувало й скільки — інакше видно лише наслідок, а не причину.
SLOW_HANDLER_SECONDS = 10.0


def _warn_if_slow(name: str, chat_id: int, started: float) -> None:
    """Повільний хендлер — першопричина протермінованих callback і зривів джобів."""
    elapsed = time.monotonic() - started
    if elapsed >= SLOW_HANDLER_SECONDS:
        logging.warning(
            f"[slow] {name} | chat_id={chat_id} | виконувався {elapsed:.1f} с — "
            "за цей час callback користувача міг протухнути"
        )


def with_typing_action(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        update = args[0]
        context = args[1]
        chat_id = update.effective_chat.id

        # Індикатор уже показує зовнішній виклик — просто виконуємось усередині
        if _active_typing.get(chat_id, 0) > 0:
            _active_typing[chat_id] += 1
            started = time.monotonic()
            try:
                return await func(*args, **kwargs)
            finally:
                _active_typing[chat_id] = max(0, _active_typing.get(chat_id, 1) - 1)
                _warn_if_slow(func.__name__, chat_id, started)

        async def keep_typing():
            tick = 0
            while tick * TYPING_INTERVAL < MAX_TYPING_SECONDS:
                try:
                    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
                    tick += 1
                except Exception as e:
                    logging.warning(f"[typing] {func.__name__} | send_chat_action failed: {e}")
                await asyncio.sleep(TYPING_INTERVAL)
            logging.warning(
                f"[typing] {func.__name__} | chat_id={chat_id} | "
                f"не завершився за {MAX_TYPING_SECONDS} с — індикатор прибрано"
            )

        _active_typing[chat_id] = 1
        task = asyncio.create_task(keep_typing())
        await asyncio.sleep(0)  # даємо keep_typing стартувати одразу
        started = time.monotonic()
        try:
            return await func(*args, **kwargs)
        finally:
            _warn_if_slow(func.__name__, chat_id, started)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            _active_typing[chat_id] = max(0, _active_typing.get(chat_id, 1) - 1)
            if not _active_typing[chat_id]:
                _active_typing.pop(chat_id, None)

    return wrapper

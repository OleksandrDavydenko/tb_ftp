import asyncio
import logging
from functools import wraps


def with_typing_action(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        update = args[0]
        context = args[1]
        chat_id = update.effective_chat.id

        async def keep_typing():
            tick = 0
            while True:
                try:
                    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
                    logging.info(f"[typing] {func.__name__} | chat_id={chat_id} | tick={tick}")
                    tick += 1
                except Exception as e:
                    logging.warning(f"[typing] {func.__name__} | send_chat_action failed: {e}")
                await asyncio.sleep(4.0)

        logging.info(f"[typing] start | {func.__name__} | chat_id={chat_id}")
        task = asyncio.create_task(keep_typing())
        try:
            return await func(*args, **kwargs)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            logging.info(f"[typing] stop  | {func.__name__} | chat_id={chat_id}")

    return wrapper

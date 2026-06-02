import asyncio
from functools import wraps


def with_typing_action(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        update = args[0]
        context = args[1]
        chat_id = update.effective_chat.id

        async def keep_typing():
            while True:
                try:
                    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
                except Exception:
                    pass
                await asyncio.sleep(4.0)

        task = asyncio.create_task(keep_typing())
        try:
            return await func(*args, **kwargs)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    return wrapper

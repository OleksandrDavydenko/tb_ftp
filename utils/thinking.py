import asyncio
from functools import wraps


def with_typing_action(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        update = args[0]
        context = args[1]
        chat_id = update.effective_chat.id
        stop = asyncio.Event()

        async def keep_typing():
            while not stop.is_set():
                try:
                    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(stop.wait(), timeout=4.0)
                except asyncio.TimeoutError:
                    pass

        task = asyncio.create_task(keep_typing())
        try:
            return await func(*args, **kwargs)
        finally:
            stop.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    return wrapper

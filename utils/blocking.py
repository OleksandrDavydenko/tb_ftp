"""
Виконання блокуючих викликів так, щоб не морозити event loop.

Запити до Power BI синхронні (`requests`) і тривають від секунди до хвилини.
Викликані напряму з async-хендлера, вони зупиняють увесь бот: PTB не читає
оновлення, індикатор «друкує» крутиться, інші користувачі стоять у черзі.

Тому кожен такий виклик загортаємо в run_blocking().
"""

import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor

# Власний пул: типовий executor має лише (CPU + 4) потоків, а на дино це 5 —
# кілька 60-секундних запитів до PBI забили б його повністю.
_EXECUTOR = ThreadPoolExecutor(max_workers=16, thread_name_prefix="blocking")


async def run_blocking(func, *args, **kwargs):
    """Виконує синхронну функцію в окремому потоці й повертає її результат."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_EXECUTOR, functools.partial(func, *args, **kwargs))

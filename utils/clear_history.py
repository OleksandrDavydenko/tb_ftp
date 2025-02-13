import asyncio
from telegram import Update
from telegram.ext import CallbackContext

async def clear_chat_history(update: Update, context: CallbackContext):
    """Видаляє всі повідомлення, які бот може видалити, навіть якщо їх більше 100."""
    chat_id = update.message.chat_id

    # Видаляємо команду користувача
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except:
        pass  # Ігноруємо помилки

    messages_deleted = 0  # Лічильник видалених повідомлень

    while True:
        messages_to_delete = []

        async for message in context.bot.get_chat_history(chat_id, limit=100):
            messages_to_delete.append(message.message_id)

        if not messages_to_delete:
            break  # Якщо немає більше повідомлень, виходимо

        for message_id in messages_to_delete:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                messages_deleted += 1
            except:
                pass  

        await asyncio.sleep(1)  # Уникаємо блокування API

    confirmation_message = await update.message.reply_text(f"🗑 Історію очищено! Видалено {messages_deleted} повідомлень.")

    await asyncio.sleep(3)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=confirmation_message.message_id)
    except:
        pass

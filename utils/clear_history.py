import asyncio
from telegram import Update
from telegram.ext import CallbackContext

async def clear_chat_history(update: Update, context: CallbackContext):
    """Видаляє всі повідомлення, які бот може видалити."""
    chat_id = update.message.chat_id

    # Видаляємо команду користувача "🗑 Очистити всю історію"
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except:
        pass  # Ігноруємо помилки

    messages_deleted = 0  # Лічильник видалених повідомлень

    # Отримуємо список останніх повідомлень через getUpdates()
    updates = await context.bot.get_updates()
    
    for update in updates:
        if update.message and update.message.chat_id == chat_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
                messages_deleted += 1
            except:
                pass  # Ігноруємо помилки, якщо неможливо видалити

    # Повідомлення про успішне очищення
    confirmation_message = await update.message.reply_text(f"🗑 Історію очищено! Видалено {messages_deleted} повідомлень.")

    await asyncio.sleep(3)  # Чекаємо 3 секунди, щоб користувач побачив повідомлення
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=confirmation_message.message_id)
    except:
        pass

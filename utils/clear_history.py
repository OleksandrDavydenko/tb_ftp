import asyncio
from telegram import Update
from telegram.ext import CallbackContext

async def clear_chat_history(update: Update, context: CallbackContext):
    """Видаляє всі повідомлення в чаті, які може видалити бот."""
    chat_id = update.message.chat_id

    # Видаляємо команду користувача "🗑 Очистити всю історію"
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except:
        pass  # Ігноруємо помилки

    messages_deleted = 0  # Лічильник видалених повідомлень

    # Отримуємо останні повідомлення через getUpdates()
    updates = await context.bot.get_updates()

    for update_item in updates:
        if update_item.message and update_item.message.chat_id == chat_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=update_item.message.message_id)
                messages_deleted += 1
                await asyncio.sleep(0.1)  # Запобігаємо API-обмеженню
            except:
                pass  # Ігноруємо помилки, якщо неможливо видалити

    # Повідомлення про успішне очищення
    confirmation_message = await update.message.reply_text(f"🗑 Історію очищено! Видалено {messages_deleted} повідомлень.")

    await asyncio.sleep(3)  # Чекаємо 3 секунди перед видаленням цього повідомлення
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=confirmation_message.message_id)
    except:
        pass

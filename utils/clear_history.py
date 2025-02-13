import asyncio
from telegram import Update
from telegram.ext import CallbackContext

async def clear_chat_history(update: Update, context: CallbackContext):
    """Видаляє всі повідомлення в чаті, які бот може видалити (тільки повідомлення бота)."""
    chat_id = update.message.chat_id

    # Видаляємо команду користувача "🗑 Очистити всю історію"
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except:
        pass  # Ігноруємо помилки

    messages_deleted = 0  # Лічильник видалених повідомлень

    # Отримуємо останні 100 повідомлень у чаті
    async for message in context.bot.get_chat_history(chat_id, limit=100):
        if message.from_user and message.from_user.id == context.bot.id:  # Видаляємо тільки повідомлення бота
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
                messages_deleted += 1
                await asyncio.sleep(0.2)  # Запобігаємо API-обмеженню
            except:
                pass  # Ігноруємо помилки, якщо неможливо видалити

    # Повідомлення про успішне очищення
    confirmation_message = await update.message.reply_text(f"🗑 Історію очищено! Видалено {messages_deleted} повідомлень.")

    await asyncio.sleep(3)  # Чекаємо 3 секунди перед видаленням цього повідомлення
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=confirmation_message.message_id)
    except:
        pass

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

    # Отримуємо всіх адміністраторів чату (щоб дізнатися, чи є бот адміном)
    chat_admins = await context.bot.get_chat_administrators(chat_id)
    bot_id = context.bot.id  # ID бота

    # Перевіряємо, чи бот є адміністратором
    is_admin = any(admin.user.id == bot_id for admin in chat_admins)

    if not is_admin:
        await update.message.reply_text("❌ Бот не має прав адміністратора для очищення історії.")
        return

    # Видаляємо останні 100 повідомлень, які бот може видалити
    async for message in context.bot.get_chat_history(chat_id, limit=100):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
            messages_deleted += 1
            await asyncio.sleep(0.2)  # Уникаємо API-обмежень
        except:
            pass  # Якщо не можна видалити, ігноруємо помилку

    # Повідомлення про успішне очищення
    confirmation_message = await update.message.reply_text(f"🗑 Історію очищено! Видалено {messages_deleted} повідомлень.")

    await asyncio.sleep(3)  # Чекаємо 3 секунди перед видаленням цього повідомлення
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=confirmation_message.message_id)
    except:
        pass

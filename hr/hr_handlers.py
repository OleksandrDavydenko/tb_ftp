# hr_handlers.py
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import CallbackContext
from .vacation_query import show_vacation_balance
from .tenure_info import show_tenure_info   

# Меню кадрового обліку
async def show_hr_menu(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'hr_main'
    keyboard = [
        [KeyboardButton("🗓 Залишки відпусток")],
        [KeyboardButton("🕓 Відпрацьовані дні")],
        [KeyboardButton("👔 Інформація про стаж")],
        [KeyboardButton("📊 Звіт відпусток та лікарняних")],
        [KeyboardButton("Головне меню")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("📋 Кадровий облік – оберіть опцію:", reply_markup=reply_markup)

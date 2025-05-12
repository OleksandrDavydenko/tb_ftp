from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import CallbackContext
from .vacation_query import show_vacation_balance

# Меню кадрового обліку
async def show_hr_menu(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'hr_main'
    keyboard = [
        [KeyboardButton("🗓 Залишки відпусток")],
        [KeyboardButton("🕓 Відпрацьовані дні")],
        [KeyboardButton("Головне меню")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("📋 Кадровий облік – оберіть опцію:", reply_markup=reply_markup)


async def handle_hr_feature_placeholder(update: Update, context: CallbackContext) -> None:
    if update.message.text == "🗓 Залишки відпусток":
        await show_vacation_balance(update, context)
    else:
        await update.message.reply_text("🔧 Функціонал в процесі розробки.")

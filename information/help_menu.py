from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from db import get_latest_currency_rates

async def show_help_menu(update: Update, context: CallbackContext) -> None:
    """
    Відображає меню "Довідкова інформація" з кнопками "Курс Валют" і "Головне меню".
    """
    context.user_data['current_menu'] = 'help_menu'  # Зберігаємо стан

    currency_button = KeyboardButton(text="Курс Валют")
    main_menu_button = KeyboardButton(text="Головне меню")
    reply_markup = ReplyKeyboardMarkup([[currency_button], [main_menu_button]], one_time_keyboard=True)
    await update.message.reply_text("ℹ️ Довідкова інформація:", reply_markup=reply_markup)

async def show_currency_rates(update: Update, context: CallbackContext) -> None:
    """
    Отримує та відображає останні курси валют з бази даних, додає кнопки "Назад" і "Головне меню".
    """
    try:
        # Отримуємо курси валют з БД
        rates = get_latest_currency_rates(["USD", "EUR"])
        
        # Формуємо повідомлення
        message = "Курси валют:\n"
        for rate in rates:
            message += f"{rate['currency']}: {rate['rate']}\n"
        
        # Відправляємо повідомлення з курсами валют
        await update.message.reply_text(message)
        
        # Зберігаємо стан меню
        context.user_data['menu'] = 'help_menu'
        
        # Додаємо кнопки "Назад" і "Головне меню"
        back_button = KeyboardButton(text="Назад")
        main_menu_button = KeyboardButton(text="Головне меню")
        reply_markup = ReplyKeyboardMarkup([[back_button], [main_menu_button]], one_time_keyboard=True)
        await update.message.reply_text("Оберіть подальшу дію:", reply_markup=reply_markup)
    
    except Exception as e:
        await update.message.reply_text("Не вдалося отримати курси валют. Спробуйте пізніше.")
        raise e



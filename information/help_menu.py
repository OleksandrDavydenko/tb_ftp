from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from information.currency_query import get_latest_currency_rates

async def show_help_menu(update: Update, context: CallbackContext) -> None:
    """
    Відображає меню "Довідкова інформація" з кнопками "Курс Валют" і "Головне меню".
    """
    currency_button = KeyboardButton(text="Курс Валют")
    main_menu_button = KeyboardButton(text="Головне меню")
    reply_markup = ReplyKeyboardMarkup([[currency_button], [main_menu_button]], one_time_keyboard=True)
    await update.message.reply_text("ℹ️ Довідкова інформація:", reply_markup=reply_markup)

async def show_currency_rates(update: Update, context: CallbackContext) -> None:
    """
    Відображає останні курси валют USD і EUR з бази даних.
    """
    try:
        rates = get_latest_currency_rates()
        if rates:
            usd_rate, eur_rate = rates
            await update.message.reply_text(
                f"📈 Курси валют:\n\n"
                f"💵 USD: {usd_rate['rate']} (оновлено: {usd_rate['timestamp']})\n"
                f"💶 EUR: {eur_rate['rate']} (оновлено: {eur_rate['timestamp']})"
            )
        else:
            await update.message.reply_text("Не вдалося отримати курси валют. Спробуйте пізніше.")
    except Exception as e:
        await update.message.reply_text(f"Помилка: {e}")

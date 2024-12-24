from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from db import get_latest_currency_rates

async def show_help_menu(update: Update, context: CallbackContext) -> None:
    """
    Відображає меню "Довідкова інформація" з кнопками "Курс Валют" і "Головне меню".
    """
    context.user_data['current_menu'] = 'help_menu'  # Зберігаємо стан


    currency_button = KeyboardButton(text="💱 Курс валют")
    devaluation_button = KeyboardButton(text="Перевірка девальвації")
    main_menu_button = KeyboardButton(text="Головне меню")
    reply_markup = ReplyKeyboardMarkup(
        [[currency_button], [devaluation_button], [main_menu_button]],
        one_time_keyboard=True,
    )
    await update.message.reply_text("ℹ️ Довідкова інформація:", reply_markup=reply_markup)

async def show_currency_rates(update: Update, context: CallbackContext) -> None:
    """
    Отримує та відображає останні курси валют з бази даних.
    """
    try:
        rates = get_latest_currency_rates(["USD", "EUR"])  # Отримати з БД
        message = "💱 Курси валют:\n"
        for rate in rates:
            message += f"{rate['currency']}: {rate['rate']}\n"  # Виводимо тільки валюту та курс
        await update.message.reply_text(message)

        # Додаємо кнопки "Назад" та "Головне меню"
        back_button = KeyboardButton(text="Назад")
        main_menu_button = KeyboardButton(text="Головне меню")
        reply_markup = ReplyKeyboardMarkup([[back_button, main_menu_button]], one_time_keyboard=True)
        await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)
    except Exception as e:
        await update.message.reply_text("Не вдалося отримати курси валют. Спробуйте пізніше.")



from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from db import get_latest_currency_rates
from information.devaluation_query import fetch_devaluation_data

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
        custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
        reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)

        # Відправляємо повідомлення з кнопками
        await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)
    except Exception as e:
        await update.message.reply_text("Не вдалося отримати курси валют. Спробуйте пізніше.")







async def show_devaluation_data(update, context):

    context.user_data['menu'] = 'devaluation_data'  # Зберігаємо стан меню
    """
    Відображає дані девальвації для конкретного менеджера.
    """
    employee_name = context.user_data.get('employee_name')  # Отримуємо ім'я менеджера
    if not employee_name:
        await update.message.reply_text("Помилка: Не знайдено ім'я менеджера.")
        return

    # Виконуємо запит
    devaluation_data = fetch_devaluation_data(employee_name)

    # Формуємо повідомлення для даних з девальвацією, наближеною до +5%
    if devaluation_data:
        near_5_percent = [
            record for record in devaluation_data
            if abs(float(record.get('[Devalvation%]', 0))) >= 4.5 
        ]

        if near_5_percent:
            highlight_message = "❗ *Зверніть увагу на рахунки з девальвацією, наближеною до +5%:*\n\n"
            for record in near_5_percent:
                highlight_message += (
                    f"👤 *Клієнт:* {record.get('[Client]', 'Невідомо')}\n"
                    f"📄 *Номер рахунку:* {record.get('[AccNumber]', 'Невідомо')}\n"
                    f"⚖️ *Відсоток девальвації:* {record.get('[Devalvation%]', 'Невідомо')}%\n\n"
                )
            await update.message.reply_text(highlight_message, parse_mode="Markdown")

        # Формуємо повний список даних
        response = "📉 Всі дані девальвації рахунків:\n\n"
        for record in devaluation_data:
            response += (
                f"👤 *Клієнт:* {record.get('[Client]', 'Невідомо')}\n"
                f"📄 *Номер рахунку:* {record.get('[AccNumber]', 'Невідомо')}\n"
                f"📅 *Дата рахунку:* {record.get('[DateFromAcc]', 'Невідомо')}\n"
                f"📜 *Номер угоди:* {record.get('[ContractNumber]', 'Невідомо')}\n"
                f"💱 *Валюта:* {record.get('[CurrencyFromInformAcc]', 'Невідомо')}\n"
                f"📈 *Курс НБУ на дату рахунку:* {record.get('[NBURateOnAccountDate]', 'Невідомо')}\n"
                f"📉 *Курс НБУ на сьогодні:* {record.get('[NBURateToday]', 'Невідомо')}\n"
                f"⚖️ *Відсоток девальвації:* {record.get('[Devalvation%]', 'Невідомо')}%\n"
                f"👔 *Менеджер:* {record.get('[Manager]', 'Невідомо')}\n\n"
            )
        await update.message.reply_text(response, parse_mode="Markdown")
    else:
        await update.message.reply_text("ℹ️ Немає даних про девальвацію для Вас.")

     # Додаємо кнопки "Назад" і "Головне меню"
    back_button = KeyboardButton(text="Назад")
    main_menu_button = KeyboardButton(text="Головне меню")
    reply_markup = ReplyKeyboardMarkup([[back_button, main_menu_button]], one_time_keyboard=True)
    await update.message.reply_text("Виберіть наступну дію:", reply_markup=reply_markup)

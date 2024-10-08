from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InputFile
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
from database import setup_database, check_phone_number_in_db
import logging
from key import KEY
from generate_salary_graph import generate_salary_graph  # Імпортуємо функцію для графіка зарплати
from generate_debt_graph import generate_debt_graph  # Імпортуємо функцію для графіка дебіторки
from debt_logic import process_debt_data, get_debt_summary, get_debt_details  # Імпортуємо логіку з debt_logic
from generate_debt_graph import generate_debt_graph  # Імпортуємо функцію для графіка дебіторки


# Налаштування бази даних
setup_database()

# Логування
logging.basicConfig(level=logging.INFO)

# Функція для привітання та запиту номера телефону
async def start(update: Update, context: CallbackContext) -> None:
    # Скидаємо реєстрацію користувача
    context.user_data['registered'] = False
    context.user_data['phone_number'] = None

    # Привітання і запит номера телефону
    await prompt_for_phone_number(update)

# Функція для запиту номера телефону
async def prompt_for_phone_number(update: Update) -> None:
    contact_button = KeyboardButton(text="Поділитися номером телефону", request_contact=True)
    custom_keyboard = [[contact_button]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)

    await update.message.reply_text("Будь ласка, поділіться своїм номером телефону, натиснувши кнопку:", reply_markup=reply_markup)

# Функція для обробки контактів
async def handle_contact(update: Update, context: CallbackContext) -> None:
    # Якщо користувач не авторизований, просимо ввести номер телефону
    if not context.user_data.get('registered', False):
        if update.message.contact:
            phone_number = update.message.contact.phone_number

            # Перевірка наявності номера в базі даних
            user_data = check_phone_number_in_db(phone_number)
            if user_data:
                context.user_data['registered'] = True
                context.user_data['phone_number'] = phone_number
                context.user_data['first_name'] = user_data[2]  # Ім'я
                context.user_data['last_name'] = user_data[3]   # Прізвище
                await update.message.reply_text(f"Вітаємо, {context.user_data['first_name']} {context.user_data['last_name']}!")
                await show_main_menu(update)
            else:
                await update.message.reply_text("Номер не знайдено. Доступ відхилено.")
                await prompt_for_phone_number(update)
    else:
        # Якщо вже зареєстрований, просто показуємо меню
        await show_main_menu(update)

# Функція для показу головного меню з кнопками "Дебіторка" і "Нараховано ЗП"
async def show_main_menu(update: Update) -> None:
    # Створюємо клавіатуру з кнопками
    accrual_button = KeyboardButton(text="Нараховано ЗП")
    debt_button = KeyboardButton(text="Дебіторка")
    custom_keyboard = [[accrual_button, debt_button]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)
    
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

# Функція для обробки кнопок "Дебіторка" і "Нараховано ЗП"
async def handle_main_menu(update: Update, context: CallbackContext) -> None:
    # Перевіряємо, чи користувач зареєстрований
    if not context.user_data.get('registered', False):
        await prompt_for_phone_number(update)
        return

    # Обробка кнопок меню
    if update.message.text == "Нараховано ЗП":
        # Генерація графіка для нарахування ЗП
        generate_salary_graph()
        # Відправлення графіка
        with open('salary_accruals.png', 'rb') as graph_file:
            await update.message.reply_photo(photo=InputFile(graph_file), caption="Графік нарахування ЗП.")

        # Додаємо кнопку "Назад"
        back_button = KeyboardButton(text="Назад")
        reply_markup = ReplyKeyboardMarkup([[back_button]], one_time_keyboard=True)
        await update.message.reply_text("Натисніть 'Назад', щоб повернутися.", reply_markup=reply_markup)

    elif update.message.text == "Дебіторка":
        # Обробка даних дебіторки
        debtors_df = process_debt_data('Дебіторка.csv')  # Передайте правильний шлях до файлу
        user_name = context.user_data['first_name'] + ' ' + context.user_data['last_name']
        total_debt = get_debt_summary(debtors_df, user_name)

        if total_debt > 0:
            debt_details = get_debt_details(debtors_df, user_name)
            response = f"Загальна дебіторська заборгованість для {user_name}: {total_debt} USD\n\n"
            response += "Деталі:\n"
            for index, row in debt_details.iterrows():
                response += f"{row['Client']}: {row['Sum_$']} USD\n"
            await update.message.reply_text(response)

            # Генерація графіка дебіторки
            generate_debt_graph(debtors_df, user_name)

            # Відправлення графіка
            with open('debt_graph.png', 'rb') as graph_file:
                await update.message.reply_photo(photo=InputFile(graph_file), caption="Графік дебіторської заборгованості.")

        else:
            await update.message.reply_text(f"Загальна дебіторська заборгованість для {user_name}: {total_debt} USD. Дані поки відсутні.")

        # Додаємо кнопку "Назад"
        back_button = KeyboardButton(text="Назад")
        reply_markup = ReplyKeyboardMarkup([[back_button]], one_time_keyboard=True)
        await update.message.reply_text("Натисніть 'Назад', щоб повернутися.", reply_markup=reply_markup)

    elif update.message.text == "Назад":
        await show_main_menu(update)  # Повертаємо до головного меню




# Основна функція для запуску бота
def main():
    token = KEY
    app = ApplicationBuilder().token(token).build()

    # Додаємо команду /start
    app.add_handler(CommandHandler("start", start))

    # Обробка контактів для авторизації
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))

    # Обробка кнопок меню
    app.add_handler(MessageHandler(filters.Regex("^(Нараховано ЗП|Дебіторка|Назад)$"), handle_main_menu))

    # Запуск бота
    app.run_polling()

if __name__ == '__main__':
    main()

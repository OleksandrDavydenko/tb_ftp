from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
from key import KEY
from auth import is_phone_number_in_power_bi
from db import add_telegram_user  # Імпортуємо функцію для збереження користувача в БД
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from deb.debt_handlers import show_debt_options, show_debt_details, show_debt_histogram, show_debt_pie_chart, show_main_menu
from salary.salary_handlers import show_salary_years, show_salary_months, show_salary_details  # Додано show_salary_details

async def start(update: Update, context: CallbackContext) -> None:
    context.user_data['registered'] = False
    context.user_data['phone_number'] = None
    await prompt_for_phone_number(update, context)

async def prompt_for_phone_number(update: Update, context: CallbackContext) -> None:
    contact_button = KeyboardButton(text="Поділитися номером телефоном", request_contact=True)
    custom_keyboard = [[contact_button]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)
    await update.message.reply_text("Будь ласка, поділіться своїм номером телефону:", reply_markup=reply_markup)

async def handle_contact(update: Update, context: CallbackContext) -> None:
    if update.message.contact:
        phone_number = update.message.contact.phone_number
        found, employee_name = is_phone_number_in_power_bi(phone_number)
        
        if found:

            """ Додавання користувача в бд """

            add_telegram_user(
                phone_number=phone_number,
                telegram_id=update.message.from_user.id,
                first_name=update.message.from_user.first_name,
                last_name=employee_name
            )

            context.user_data['registered'] = True
            context.user_data['phone_number'] = phone_number
            context.user_data['first_name'] = employee_name
            context.user_data['last_name'] = update.message.from_user.last_name
            await update.message.reply_text(f"Вітаємо, {context.user_data['first_name']}! Доступ надано.")
            await show_main_menu(update, context)
        else:
            await update.message.reply_text("Ваш номер не знайдено. Доступ заборонено.")
            await prompt_for_phone_number(update, context)

async def show_main_menu(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'main_menu'
    debt_button = KeyboardButton(text="Дебіторка")
    salary_button = KeyboardButton(text="Розрахунковий лист")
    custom_keyboard = [[debt_button, salary_button]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

async def handle_main_menu(update: Update, context: CallbackContext) -> None:
    if not context.user_data.get('registered', False):
        await prompt_for_phone_number(update, context)
        return

    if update.message.text == "Дебіторка":
        await show_debt_options(update, context)

    elif update.message.text == "Таблиця":
        await show_debt_details(update, context)

    elif update.message.text == "Гістограма":
        await show_debt_histogram(update, context)

    elif update.message.text == "Діаграма":
        await show_debt_pie_chart(update, context)

    elif update.message.text == "Назад":
        current_menu = context.user_data.get('menu')
        if current_menu == 'salary_months':  # Повернення з вибору місяців до вибору років
            await show_salary_years(update, context)
        elif current_menu == 'salary_years':  # Повернення з вибору років до головного меню
            await show_main_menu(update, context)
        elif current_menu == 'debt_options':
            await show_main_menu(update, context)
        elif current_menu in ['debt_details', 'debt_histogram', 'debt_pie_chart']:
            await show_debt_options(update, context)
        else:
            await show_main_menu(update, context)

    elif update.message.text == "Розрахунковий лист":
        await show_salary_years(update, context)  # Показуємо меню вибору року

    elif update.message.text == "Головне меню":
        await show_main_menu(update, context)  # Повернення до головного меню

    # Після вибору року користувачеві пропонуються місяці
    elif update.message.text in ["2024", "2025"]:  # Додати логіку для обробки років
        context.user_data['selected_year'] = update.message.text  # Зберігаємо вибраний рік
        await show_salary_months(update, context)  # Показуємо меню вибору місяця

    # Після вибору місяця показуємо розрахункову таблицю
    elif update.message.text in ["Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень", "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"]:
        context.user_data['selected_month'] = update.message.text  # Зберігаємо вибраний місяць
        await show_salary_details(update, context)  # Показуємо розрахункову таблицю

def main():
    token = KEY
    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.Regex("^(Дебіторка|Назад|Таблиця|Гістограма|Діаграма|Розрахунковий лист|Головне меню|2024|2025|Січень|Лютий|Березень|Квітень|Травень|Червень|Липень|Серпень|Вересень|Жовтень|Листопад|Грудень)$"), handle_main_menu))

    app.run_polling()

if __name__ == '__main__':
    main()

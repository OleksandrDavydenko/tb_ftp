from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
import asyncio
import logging
from messages.check_payments import run_periodic_check
from key import KEY
from auth import is_phone_number_in_power_bi
from db import add_telegram_user, get_user_joined_at
from sync_payments import sync_payments
from deb.debt_handlers import show_debt_options, show_debt_details, show_debt_histogram, show_debt_pie_chart
from salary.salary_handlers import show_salary_years, show_salary_months, show_salary_details

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        logging.info(f"Отримано номер телефону: {phone_number}")
        found, employee_name = is_phone_number_in_power_bi(phone_number)

        if found:
            logging.info(f"Користувач знайдений: {employee_name}")

            add_telegram_user(
                phone_number=phone_number,
                telegram_id=update.message.from_user.id,
                first_name=update.message.from_user.first_name,
                last_name=update.message.from_user.last_name
            )

            joined_at = get_user_joined_at(phone_number)
            logging.info(f"Дата приєднання користувача: {joined_at}")

            if joined_at:
                sync_payments(employee_name, phone_number, joined_at)

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
    custom_keyboard = [[KeyboardButton("Дебіторка"), KeyboardButton("Розрахунковий лист")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

async def handle_main_menu(update: Update, context: CallbackContext) -> None:
    if not context.user_data.get('registered', False):
        await prompt_for_phone_number(update, context)
        return

    menu_option = update.message.text
    if menu_option == "Дебіторка":
        await show_debt_options(update, context)
    elif menu_option in ["Таблиця", "Гістограма", "Діаграма"]:
        await {"Таблиця": show_debt_details, "Гістограма": show_debt_histogram, "Діаграма": show_debt_pie_chart}[menu_option](update, context)
    elif menu_option == "Назад":
        await show_main_menu(update, context)
    elif menu_option == "Розрахунковий лист":
        await show_salary_years(update, context)
    elif menu_option in ["2024", "2025"]:
        context.user_data['selected_year'] = menu_option
        await show_salary_months(update, context)
    elif menu_option in ["Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень", "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"]:
        context.user_data['selected_month'] = menu_option
        await show_salary_details(update, context)

def run_bot():
    app = ApplicationBuilder().token(KEY).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.Regex("^(Дебіторка|Назад|Таблиця|Гістограма|Діаграма|Розрахунковий лист|2024|2025|Січень|Лютий|Березень|Квітень|Травень|Червень|Липень|Серпень|Вересень|Жовтень|Листопад|Грудень)$"), handle_main_menu))

    return app

async def run_async_tasks():
    asyncio.create_task(run_periodic_check())

async def main():
    app = run_bot()

    await run_async_tasks()

    # Запускаємо бота в режимі polling
    await app.run_polling()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Головна помилка: {e}")

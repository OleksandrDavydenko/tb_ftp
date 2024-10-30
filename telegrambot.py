import asyncio
import logging
import os
import threading
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
from messages.check_payments import run_periodic_check
from auth import is_phone_number_in_power_bi
from db import add_telegram_user, get_user_joined_at
from messages.sync_payments import sync_payments, run_periodic_sync
from messages.reminder import schedule_monthly_reminder
from deb.debt_handlers import show_debt_options, show_debt_details, show_debt_histogram, show_debt_pie_chart, show_main_menu
from salary.salary_handlers import show_salary_years, show_salary_months, show_salary_details

KEY = os.getenv('TELEGRAM_BOT_TOKEN')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def start(update: Update, context: CallbackContext) -> None:
    context.user_data['registered'] = False
    context.user_data['phone_number'] = None
    await prompt_for_phone_number(update, context)

async def prompt_for_phone_number(update: Update, context: CallbackContext) -> None:
    contact_button = KeyboardButton(text="Поділитися номером телефоном", request_contact=True)
    reply_markup = ReplyKeyboardMarkup([[contact_button]], one_time_keyboard=True)
    await update.message.reply_text("Будь ласка, поділіться своїм номером телефону:", reply_markup=reply_markup)

def normalize_phone_number(phone_number):
    return phone_number.lstrip('+')

async def handle_contact(update: Update, context: CallbackContext) -> None:
    if update.message.contact:
        phone_number = normalize_phone_number(update.message.contact.phone_number)
        logging.info(f"Отримано номер телефону: {phone_number}")
        found, employee_name = is_phone_number_in_power_bi(phone_number)

        if found:
            logging.info(f"Користувач знайдений: {employee_name}")
            add_telegram_user(
                phone_number=phone_number,
                telegram_id=update.message.from_user.id,
                telegram_name=update.message.from_user.first_name,
                employee_name=employee_name
            )

            joined_at = get_user_joined_at(phone_number)
            logging.info(f"Дата приєднання користувача: {joined_at}")

            if joined_at:
                try:
                    await sync_payments()
                except Exception as e:
                    logging.error(f"Помилка при синхронізації платежів: {e}")

            context.user_data['registered'] = True
            context.user_data['phone_number'] = phone_number
            context.user_data['telegram_name'] = update.message.from_user.first_name
            context.user_data['employee_name'] = employee_name
            await update.message.reply_text(f"Вітаємо, {context.user_data['employee_name']}! Доступ надано.")
            await show_main_menu(update, context)
        else:
            await update.message.reply_text("Ваш номер не знайдено. Доступ заборонено.")
            await prompt_for_phone_number(update, context)

async def show_main_menu(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'main_menu'
    debt_button = KeyboardButton(text="Дебіторка")
    salary_button = KeyboardButton(text="Розрахунковий лист")
    reply_markup = ReplyKeyboardMarkup([[debt_button, salary_button]], one_time_keyboard=True)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

async def handle_main_menu(update: Update, context: CallbackContext) -> None:
    if not context.user_data.get('registered', False):
        await prompt_for_phone_number(update, context)
        return

    menu_text = update.message.text
    if menu_text == "Дебіторка":
        await show_debt_options(update, context)
    elif menu_text == "Таблиця":
        await show_debt_details(update, context)
    elif menu_text == "Гістограма":
        await show_debt_histogram(update, context)
    elif menu_text == "Діаграма":
        await show_debt_pie_chart(update, context)
    elif menu_text == "Назад":
        current_menu = context.user_data.get('menu')
        if current_menu == 'salary_months':
            await show_salary_years(update, context)
        elif current_menu == 'salary_years':
            await show_main_menu(update, context)
        elif current_menu == 'debt_options':
            await show_main_menu(update, context)
        elif current_menu in ['debt_details', 'debt_histogram', 'debt_pie_chart']:
            await show_debt_options(update, context)
        else:
            await show_main_menu(update, context)
    elif menu_text == "Розрахунковий лист":
        await show_salary_years(update, context)
    elif menu_text == "Головне меню":
        await show_main_menu(update, context)
    elif menu_text in ["2024", "2025"]:
        context.user_data['selected_year'] = menu_text
        await show_salary_months(update, context)
    elif menu_text in ["Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень", "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"]:
        context.user_data['selected_month'] = menu_text
        await show_salary_details(update, context)

async def main():
    app = ApplicationBuilder().token(KEY).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.Regex("^(Дебіторка|Назад|Таблиця|Гістограма|Діаграма|Розрахунковий лист|Головне меню|2024|2025|Січень|Лютий|Березень|Квітень|Травень|Червень|Липень|Серпень|Вересень|Жовтень|Листопад|Грудень)$"), handle_main_menu))

    # Запускаємо планувальник для щомісячного нагадування
    schedule_monthly_reminder()

    # Створюємо асинхронні завдання для періодичної перевірки та синхронізації виплат
    asyncio.create_task(run_periodic_check())
    asyncio.create_task(run_periodic_sync())

    await app.run_polling()

if __name__ == '__main__':
    asyncio.run(main())

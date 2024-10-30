import asyncio  # Додаємо імпорт asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
import threading
from messages.check_payments import run_periodic_check
from auth import is_phone_number_in_power_bi
from db import add_telegram_user, get_user_joined_at
from messages.sync_payments import sync_payments
from messages.sync_payments import run_periodic_sync  # Імпорт періодичної синхронізації
from messages.reminder import schedule_monthly_reminder
import sys
import os
import logging


KEY = os.getenv('TELEGRAM_BOT_TOKEN')

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from deb.debt_handlers import show_debt_options, show_debt_details, show_debt_histogram, show_debt_pie_chart, show_main_menu
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


def normalize_phone_number(phone_number):
    # Видаляємо '+' на початку номера
    if phone_number.startswith('+'):
        phone_number = phone_number[1:]
    return phone_number

async def handle_contact(update: Update, context: CallbackContext) -> None:
    if update.message.contact:
        phone_number = normalize_phone_number(update.message.contact.phone_number)

        logging.info(f"Отримано номер телефону: {phone_number}")
        found, employee_name = is_phone_number_in_power_bi(phone_number)
        

        if found:
            logging.info(f"Користувач знайдений: {employee_name}")

            # Додаємо користувача в базу даних з новими назвами стовпців
            add_telegram_user(
                phone_number=phone_number,
                telegram_id=update.message.from_user.id,
                telegram_name=update.message.from_user.first_name,
                employee_name=employee_name
                #employee_name=update.message.from_user.last_name
            )

            # Отримуємо дату приєднання користувача
            joined_at = get_user_joined_at(phone_number)
            logging.info(f"Дата приєднання користувача: {joined_at}")

            if joined_at:
                try:
                    await sync_payments()  # Додаємо await
                except Exception as e:
                    logging.error(f"Помилка при синхронізації платежів: {e}")

            # Оновлюємо дані користувача в контексті
            context.user_data['registered'] = True
            context.user_data['phone_number'] = phone_number
            context.user_data['telegram_name'] = update.message.from_user.first_name
            context.user_data['employee_name'] = employee_name

            # Вітання та відображення головного меню
            await update.message.reply_text(f"Вітаємо, {context.user_data['employee_name']}! Доступ надано.")
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

    elif update.message.text == "Розрахунковий лист":
        await show_salary_years(update, context)

    elif update.message.text == "Головне меню":
        await show_main_menu(update, context)

    elif update.message.text in ["2024", "2025"]:
        context.user_data['selected_year'] = update.message.text
        await show_salary_months(update, context)

    elif update.message.text in ["Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень", "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"]:
        context.user_data['selected_month'] = update.message.text
        await show_salary_details(update, context)

async def main():
    token = KEY
    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.Regex("^(Дебіторка|Назад|Таблиця|Гістограма|Діаграма|Розрахунковий лист|Головне меню|2024|2025|Січень|Лютий|Березень|Квітень|Травень|Червень|Липень|Серпень|Вересень|Жовтень|Листопад|Грудень)$"), handle_main_menu))

    # Запускаємо періодичні завдання
    check_thread = threading.Thread(target=lambda: asyncio.run(run_periodic_check()))
    sync_thread = threading.Thread(target=lambda: asyncio.run(run_periodic_sync()))
    check_thread.daemon = True
    sync_thread.daemon = True
    check_thread.start()
    sync_thread.start()

    # Запускаємо нагадування
    schedule_monthly_reminder()

    await app.run_polling()

if __name__ == '__main__':
    asyncio.run(main())


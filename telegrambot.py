import asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging
import os
import sys

from messages.check_payments import check_new_payments
from messages.sync_payments import sync_payments
from messages.reminder import schedule_monthly_reminder
from auth import is_phone_number_in_power_bi
from db import add_telegram_user, get_user_joined_at

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from deb.debt_handlers import show_debt_options, show_debt_details, show_debt_histogram, show_debt_pie_chart
from salary.salary_handlers import show_salary_years, show_salary_months, show_salary_details

KEY = os.getenv('TELEGRAM_BOT_TOKEN')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

scheduler = AsyncIOScheduler()

async def start(update: Update, context: CallbackContext) -> None:
    context.user_data['registered'] = False
    await prompt_for_phone_number(update, context)

async def prompt_for_phone_number(update: Update, context: CallbackContext) -> None:
    contact_button = KeyboardButton(text="Поділитися номером телефоном", request_contact=True)
    reply_markup = ReplyKeyboardMarkup([[contact_button]], one_time_keyboard=True)
    await update.message.reply_text("Будь ласка, поділіться своїм номером телефону:", reply_markup=reply_markup)

async def handle_contact(update: Update, context: CallbackContext) -> None:
    if update.message.contact:
        phone_number = update.message.contact.phone_number.lstrip('+')
        found, employee_name = is_phone_number_in_power_bi(phone_number)

        if found:
            add_telegram_user(phone_number=phone_number, telegram_id=update.message.from_user.id,
                              employee_name=employee_name)
            context.user_data.update({'registered': True, 'phone_number': phone_number, 'employee_name': employee_name})
            await update.message.reply_text(f"Вітаємо, {employee_name}! Доступ надано.")
            await show_main_menu(update, context)
        else:
            await update.message.reply_text("Ваш номер не знайдено. Доступ заборонено.")
            await prompt_for_phone_number(update, context)

async def show_main_menu(update: Update, context: CallbackContext) -> None:
    debt_button = KeyboardButton(text="Дебіторка")
    salary_button = KeyboardButton(text="Розрахунковий лист")
    reply_markup = ReplyKeyboardMarkup([[debt_button, salary_button]], one_time_keyboard=True)
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
    elif update.message.text == "Розрахунковий лист":
        await show_salary_years(update, context)
    elif update.message.text == "Головне меню":
        await show_main_menu(update, context)
    elif update.message.text in ["2024", "2025"]:
        context.user_data['selected_year'] = update.message.text
        await show_salary_months(update, context)
    elif update.message.text in ["Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
                                 "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"]:
        context.user_data['selected_month'] = update.message.text
        await show_salary_details(update, context)

async def shutdown(app, scheduler):
    await app.shutdown()
    logging.info("Зупиняємо планувальник...")
    scheduler.shutdown(wait=True)
    logging.info("Планувальник зупинено.")

def main():
    app = ApplicationBuilder().token(KEY).build()
    scheduler = AsyncIOScheduler()

    # Налаштування запланованих завдань
    scheduler.add_job(check_new_payments, 'interval', seconds=60, max_instances=1, misfire_grace_time=30)
    scheduler.add_job(sync_payments, 'interval', seconds=60, max_instances=1, misfire_grace_time=30)
    schedule_monthly_reminder(scheduler)

    scheduler.start()
    logging.info("Планувальник запущено.")

    # Обробники команд та повідомлень
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.Regex(
        "^(Дебіторка|Назад|Таблиця|Гістограма|Діаграма|Розрахунковий лист|Головне меню|2024|2025|"
        "Січень|Лютий|Березень|Квітень|Травень|Червень|Липень|Серпень|Вересень|Жовтень|Листопад|Грудень)$"), 
        handle_main_menu))

    try:
        app.run_polling()
    finally:
        asyncio.run(shutdown(app, scheduler))

if __name__ == '__main__':
    main()

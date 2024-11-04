import asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging
import os
import sys

from messages.check_payments import check_new_payments
from messages.sync_payments import sync_payments
from auth import is_phone_number_in_power_bi
from db import add_telegram_user, get_user_joined_at
from messages.reminder import schedule_monthly_reminder

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from deb.debt_handlers import show_debt_options, show_debt_details, show_debt_histogram, show_debt_pie_chart
from salary.salary_handlers import show_salary_years, show_salary_months, show_salary_details
from employee_analytics.analytics_handler import (
    show_analytics_options, show_analytics_years, show_analytics_months, 
    show_monthly_analytics, show_yearly_chart_for_parameter
)

KEY = os.getenv('TELEGRAM_BOT_TOKEN')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
scheduler = AsyncIOScheduler()

async def start(update: Update, context: CallbackContext) -> None:
    context.user_data['registered'] = False
    await prompt_for_phone_number(update, context)

async def prompt_for_phone_number(update: Update, context: CallbackContext) -> None:
    contact_button = KeyboardButton(text="Поділитися номером телефоном", request_contact=True)
    reply_markup = ReplyKeyboardMarkup([[contact_button]], one_time_keyboard=True)
    await update.message.reply_text(
        "Будь ласка, поділіться своїм номером телефону, натиснувши кнопку 'Поділитися номером телефоном' нижче.",
        reply_markup=reply_markup
    )

def normalize_phone_number(phone_number):
    return phone_number[1:] if phone_number.startswith('+') else phone_number

async def handle_contact(update: Update, context: CallbackContext) -> None:
    if update.message.contact:
        phone_number = normalize_phone_number(update.message.contact.phone_number)
        logging.info(f"Отримано номер телефону: {phone_number}")
        found, employee_name = is_phone_number_in_power_bi(phone_number)
        
        if found:
            logging.info(f"Користувач знайдений: {employee_name}")
            add_telegram_user(phone_number=phone_number, telegram_id=update.message.from_user.id,
                              telegram_name=update.message.from_user.first_name, employee_name=employee_name)
            joined_at = get_user_joined_at(phone_number)
            logging.info(f"Дата приєднання користувача: {joined_at}")

            if joined_at:
                try:
                    await sync_payments()
                except Exception as e:
                    logging.error(f"Помилка при синхронізації платежів: {e}")

            context.user_data.update({
                'registered': True,
                'phone_number': phone_number,
                'telegram_name': update.message.from_user.first_name,
                'employee_name': employee_name
            })
            await update.message.reply_text(f"Вітаємо, {context.user_data['employee_name']}! Доступ надано.")
            await show_main_menu(update, context)
        else:
            await update.message.reply_text("Ваш номер не знайдено. Доступ заборонено.")
            await prompt_for_phone_number(update, context)

async def show_main_menu(update: Update, context: CallbackContext) -> None:
    analytics_button = KeyboardButton(text="Аналітика")
    salary_button = KeyboardButton(text="Розрахунковий лист")
    debt_button = KeyboardButton(text="Дебіторська заборгованість")
    reply_markup = ReplyKeyboardMarkup([[analytics_button, salary_button], [debt_button]], one_time_keyboard=True)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

async def handle_main_menu(update: Update, context: CallbackContext) -> None:
    if not context.user_data.get('registered', False):
        await prompt_for_phone_number(update, context)
        return

    text = update.message.text
    if text == "Дебіторська заборгованість":
        await show_debt_options(update, context)
    elif text == "Таблиця":
        await show_debt_details(update, context)
    elif text == "Гістограма":
        await show_debt_histogram(update, context)
    elif text == "Діаграма":
        await show_debt_pie_chart(update, context)
    elif text == "Розрахунковий лист":
        context.user_data['menu'] = 'salary_years'
        await show_salary_years(update, context)
    elif text == "Аналітика":
        await show_analytics_options(update, context)
    elif text == "Назад":
        await handle_back_navigation(update, context)
    elif text == "Головне меню":
        await show_main_menu(update, context)
    elif text in ["Аналітика за місяць", "Аналітика за рік"]:
        await handle_analytics_selection(update, context, text)
    elif text in ["2024", "2025"]:
        await handle_year_choice(update, context)
    elif text in ["Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень", "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"]:
        await handle_month_choice(update, context)
    elif text in ["Дохід", "Валовий прибуток", "Маржинальність", "Кількість угод"]:
        await handle_parameter_choice(update, context)

async def handle_back_navigation(update: Update, context: CallbackContext) -> None:
    menu = context.user_data.get('menu')

    if menu == 'salary_months':
        await show_salary_years(update, context)
    elif menu == 'salary_years':
        await show_main_menu(update, context)
    elif menu == 'analytics_years':
        await show_analytics_options(update, context)
    elif menu == 'parameter_selection':
        await show_analytics_years(update, context)
    elif menu == 'analytics_months':
        await show_analytics_years(update, context)
    elif menu in ['debt_details', 'debt_histogram', 'debt_pie_chart']:
        await show_debt_options(update, context)
    else:
        await show_main_menu(update, context)

async def handle_analytics_selection(update: Update, context: CallbackContext, selection: str) -> None:
    context.user_data['analytics_type'] = 'monthly' if selection == "Аналітика за місяць" else 'yearly'
    await show_analytics_years(update, context)

async def handle_year_choice(update: Update, context: CallbackContext) -> None:
    selected_year = update.message.text
    context.user_data['selected_year'] = selected_year
    current_menu = context.user_data.get('menu')

    if current_menu == 'salary_years':
        await show_salary_months(update, context)
    elif context.user_data.get('analytics_type') == 'monthly':
        await show_analytics_months(update, context)
    elif context.user_data.get('analytics_type') == 'yearly':
        await show_parameter_selection(update, context)

async def handle_month_choice(update: Update, context: CallbackContext) -> None:
    selected_month = update.message.text
    context.user_data['selected_month'] = selected_month
    current_menu = context.user_data.get('menu')

    if current_menu == 'salary_months':
        await show_salary_details(update, context)
    else:
        await show_monthly_analytics(update, context)

async def show_parameter_selection(update: Update, context: CallbackContext) -> None:
    parameter_buttons = [
        [KeyboardButton("Дохід")],
        [KeyboardButton("Валовий прибуток")],
        [KeyboardButton("Маржинальність")],
        [KeyboardButton("Кількість угод")],
        [KeyboardButton("Назад")]
    ]
    reply_markup = ReplyKeyboardMarkup(parameter_buttons, one_time_keyboard=True)
    await update.message.reply_text("Оберіть параметр для відображення графіка:", reply_markup=reply_markup)
    context.user_data['menu'] = 'parameter_selection'

async def handle_parameter_choice(update: Update, context: CallbackContext) -> None:
    selected_parameter = update.message.text
    context.user_data['selected_parameter'] = selected_parameter
    employee_name = context.user_data['employee_name']
    selected_year = context.user_data['selected_year']

    await show_yearly_chart_for_parameter(update, context, employee_name, selected_year, selected_parameter)

async def shutdown(app, scheduler):
    await app.shutdown()
    scheduler.shutdown(wait=True)
    logging.info("Планувальник зупинено.")

def main():
    app = ApplicationBuilder().token(KEY).build()
    scheduler.add_job(check_new_payments, 'interval', seconds=300)
    scheduler.add_job(sync_payments, 'interval', seconds=270)
    schedule_monthly_reminder(scheduler)
    scheduler.start()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.Regex("^(Дебіторська заборгованість|Назад|Таблиця|Гістограма|Діаграма|Розрахунковий лист|Головне меню|Аналітика|Аналітика за місяць|Аналітика за рік|2024|2025|Січень|Лютий|Березень|Квітень|Травень|Червень|Липень|Серпень|Вересень|Жовтень|Листопад|Грудень|Дохід|Валовий прибуток|Маржинальність|Кількість угод)$"), handle_main_menu))

    try:
        app.run_polling()
    finally:
        asyncio.run(shutdown(app, scheduler))

if __name__ == '__main__':
    main()

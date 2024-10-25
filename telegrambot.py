import os
import logging
import asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
from sync_payments import sync_payments
from auth import is_phone_number_in_power_bi
from db import add_telegram_user, get_user_joined_at
import psycopg2
from key import KEY

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

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
                await sync_payments(employee_name, phone_number, joined_at)

            context.user_data['registered'] = True
            context.user_data['phone_number'] = phone_number
            context.user_data['first_name'] = employee_name

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

async def periodic_sync():
    while True:
        logging.info("Періодична синхронізація платежів розпочата.")
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Отримуємо всіх користувачів для синхронізації
            cursor.execute("SELECT phone_number, first_name, joined_at FROM users")
            users = cursor.fetchall()

            for user in users:
                phone_number, employee_name, joined_at = user
                logging.info(f"Синхронізація для {employee_name} ({phone_number})")
                await sync_payments(employee_name, phone_number, joined_at)

            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Помилка при періодичній синхронізації: {e}")
        await asyncio.sleep(3600)  # Перевірка щогодини

def main():
    token = KEY
    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.Regex("^(Дебіторка|Розрахунковий лист|Головне меню)$"), show_main_menu))

    # Запускаємо асинхронну перевірку платежів
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_sync())

    # Запускаємо бота
    app.run_polling()

if __name__ == '__main__':
    main()

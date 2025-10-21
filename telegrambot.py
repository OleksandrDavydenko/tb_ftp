import asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, BotCommandScopeDefault, BotCommand, MenuButtonCommands
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from information.querryFinanceUa import store_exchange_rates
import logging
import os
import sys
from datetime import datetime



from messages.check_payments import check_new_payments
#from messages.sync_payments import sync_payments
from messages.sync_payments import sync_payments

from auth import is_phone_number_in_power_bi
from db import add_telegram_user, get_user_joined_at, get_user_status, get_employee_name, log_user_action, get_user_by_telegram_id
from auth import verify_and_add_user 
from messages.reminder import daily_first_workday_check
from messages.check_devaluation import check_new_devaluation_records
from messages.sync_devaluation import sync_devaluation_data
from messages.birthday_greetings import send_birthday_greetings 
 



#from messages.oneTimeMessages.update1 import send_message_to_users # Імпорт функції з нового файлу
#from messages.oneTimeMessages.update2 import send_message_to_users
#from messages.oneTimeMessages.update3 import send_message_to_users
#from messages.oneTimeMessages.update4 import send_message_to_users
#from messages.oneTimeMessages.update5 import send_message_to_users
#from messages.oneTimeMessages.update6 import send_message_to_users
#from messages.oneTimeMessages.update7 import send_message_to_users
#from messages.oneTimeMessages.update11 import send_message_to_users


sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from deb.debt_handlers import show_debt_options, show_debt_details, show_debt_histogram, show_debt_pie_chart, handle_overdue_debt

from salary.salary_handlers import (
    show_salary_years, show_salary_months, show_salary_details,
    show_salary_menu,
    show_bonuses_years, show_bonuses_months, send_bonuses_excel,
    show_bonusmsg_years, show_bonusmsg_months, send_bonuses_message,
    show_leadprize_years, show_leadprize_months, send_leadprizes_message,
    show_leadreport_years, show_leadreport_months, send_leadreport_excel
)


from employee_analytics.analytics_handler import (
    show_analytics_options, show_analytics_years, show_analytics_months, 
    show_monthly_analytics, show_yearly_chart_for_parameter
)
from hr.hr_handlers import show_hr_menu

from hr.vacation_query import show_vacation_balance
from hr.workdays_query import show_workdays_years, show_workdays_months, show_workdays_details
from hr.tenure_info import show_tenure_info

from information.help_menu import show_help_menu, show_currency_rates, show_devaluation_data
from information.user_guide import show_user_guide
from messages.weekly_overdue_debts import check_overdue_debts
from sync_status import sync_user_statuses
from messages.sync_bonus_docs import sync_bonus_docs
from messages.check_bonus_docs import check_bonus_docs

from utils.name_aliases import display_name


sys.path.append(os.path.join(os.path.dirname(__file__), "openAI"))
from openAI.gpt_handler import is_known_command, get_gpt_response


KEY = os.getenv('TELEGRAM_BOT_TOKEN')

def set_bot_menu_sync(app):
    """Синхронне додавання команд у меню."""
    commands = [
        BotCommand("menu", "🏠 Головне меню"),
        BotCommand("debt", "📉 Дебіторська заборгованість"),
        BotCommand("salary", "💼 Зарплата"),
        BotCommand("analytics", "📊 Аналітика"),
        BotCommand("hr", "🧾 Кадровий облік"),
        BotCommand("info", "ℹ️ Інформація")
    ]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.bot.set_my_commands(commands))
    loop.run_until_complete(app.bot.set_chat_menu_button(menu_button=MenuButtonCommands()))




logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
scheduler = AsyncIOScheduler()



async def start(update: Update, context: CallbackContext) -> None:
    telegram_id = update.message.from_user.id
    user = get_user_by_telegram_id(telegram_id)

    logging.info(f"[START] Telegram ID: {telegram_id}, user from DB: {user}")

    if user:
        phone_number, employee_name = user

        context.user_data.update({
            'registered': True,
            'phone_number': phone_number,
            'telegram_name': update.message.from_user.first_name,
            'employee_name': employee_name
        })
        nice_name = display_name(context.user_data['employee_name'])

        await update.message.reply_text(f"👋 Вітаємо, {nice_name}! Доступ надано.")
        await show_main_menu(update, context)

    else:
        # Лише якщо не знайдено в базі — просимо номер
        await prompt_for_phone_number(update, context)


async def prompt_for_phone_number(update: Update, context: CallbackContext) -> None:
    logging.info(f"[AUTH] Користувача не знайдено. Просимо поділитися номером. Telegram ID: {update.message.from_user.id}")
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
        logging.info(f"📞 Отримано номер телефону: {phone_number}")

        user_id = update.message.from_user.id
        log_user_action(user_id, f"Надано номер телефону: {phone_number}", update.message.message_id)

        # Перевіряємо користувача в Power BI
        verify_and_add_user(phone_number, update.message.from_user.id, update.message.from_user.first_name)

        # Отримуємо статус із бази
        status = get_user_status(phone_number)
        logging.info(f"📊 Статус у БД: {status}")

        if status == "active":
            employee_name = get_employee_name(phone_number)  # Отримуємо ім'я користувача
            logging.info(f"✅ Користувач активний: {employee_name} ({phone_number})")

            joined_at = get_user_joined_at(phone_number)
            logging.info(f"📅 Дата приєднання користувача: {joined_at}")

            # Синхронізація платежів (Немає потреби всіх синхронізувати при верифікації)
            # if joined_at:
            #    try:
            #        await sync_payments()
            #    except Exception as e:
            #        logging.error(f"❌ Помилка при синхронізації платежів: {e}")

            # Оновлення даних користувача в контексті бота
            context.user_data.update({
                'registered': True,
                'phone_number': phone_number,
                'telegram_name': update.message.from_user.first_name,
                'employee_name': employee_name
            })

            nice_name = display_name(context.user_data['employee_name'])

            await update.message.reply_text(
                f"✅ Вітаємо, {nice_name}! Доступ надано.\n\n"
                "Ви можете скористатися меню для перегляду фінансових даних або отримати відповідь на своє запитання.\n\n"
                "💡 Щоб скористатися меню, просто оберіть потрібний розділ.\n"
                "💬 Якщо у вас є запитання щодо фінансів – просто наберіть його у повідомленні, і я допоможу вам знайти відповідь."
            )
            await show_main_menu(update, context)

        else:
            logging.warning(f"🚫 Доступ заборонено для {phone_number} (Статус: {status})")
            await update.message.reply_text("🚫 Ваш номер не знайдено або ви не активний користувач. Доступ заборонено.")
            await prompt_for_phone_number(update, context)



async def show_main_menu(update: Update, context: CallbackContext) -> None:
    """Функція показує головне меню та працює з будь-якого місця бота."""
    
    # Логування для діагностики
    logging.info("🔄 Виклик головного меню")

    # Перевіряємо, чи користувач зареєстрований
    if not context.user_data.get('registered', False):
        logging.warning("❌ Користувач не зареєстрований. Запит номера телефону.")
        await prompt_for_phone_number(update, context)
        return

    # Створюємо клавіатуру головного меню
    reply_markup = get_main_menu_keyboard()

    # Визначаємо, чи це повідомлення або inline-кнопка
    if update.message:
        await update.message.reply_text("🏠 Головне меню:", reply_markup=reply_markup)
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.edit_text("🏠 Головне меню:", reply_markup=reply_markup)


# Окрема функція для генерації клавіатури головного меню
def get_main_menu_keyboard():
    """Генерує клавіатуру головного меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Аналітика"), KeyboardButton(text="💼 Зарплата")],
            [KeyboardButton(text="📉 Дебіторська заборгованість"), KeyboardButton(text="🧾 Кадровий облік")],
            [KeyboardButton(text="ℹ️ Інформація")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def is_registered(telegram_id):
    user = get_user_by_telegram_id(telegram_id)
    return user is not None
def populate_user_context(context: CallbackContext, telegram_id: int):
    user = get_user_by_telegram_id(telegram_id)
    if user:
        phone_number, employee_name = user
        context.user_data.update({
            'registered': True,
            'phone_number': phone_number,
            'employee_name': employee_name
        })


async def handle_main_menu(update: Update, context: CallbackContext) -> None:
    telegram_id = update.effective_user.id
    populate_user_context(context, telegram_id)

    if not is_registered(telegram_id):
        logging.warning(f"❌ Користувача {telegram_id} не знайдено в БД. Просимо номер.")
        await prompt_for_phone_number(update, context)
        return

    query = update.callback_query
    if query:
        text = query.data
        user_id = query.from_user.id
        message_id = query.message.message_id if query.message else -1
        await query.answer()
    else:
        text = update.message.text if update.message else None
        user_id = update.message.from_user.id if update.message else None
        message_id = update.message.message_id if update.message else -1

    if not text or not user_id:
        logging.warning("⚠️ Не вдалося отримати текст кнопки або ID користувача")
        return

    logging.info(f"📩 Отримано повідомлення: {text} від користувача {user_id} (message_id: {message_id})")

    if is_known_command(text):
        try:
            log_user_action(user_id, text, message_id)
            logging.info(f"✅ Користувач {user_id} виконав команду: {text}")
        except Exception as e:
            logging.error(f"❌ Помилка логування для {user_id}: {e}")

        # 🔹 Головні розділи
        if text == "📉 Дебіторська заборгованість":
            await show_debt_options(update, context)
        elif text == "💼 Зарплата":
            await show_salary_menu(update, context)
        elif text == "💼 Оклад":
            context.user_data['menu'] = 'salary_years'
            await show_salary_years(update, context)
        elif text == "💰 Бонуси":
            context.user_data['menu'] = 'bonusmsg_years'
            await show_bonusmsg_years(update, context)
        elif text == "👑 Премії керівників":
            await show_leadprize_years(update, context)
        elif text == "📜 Відомість керівника":
            await show_leadreport_years(update, context)







        elif text in ("🎁 Відомість Бонуси", "Відомість Бонуси"):
            context.user_data['menu'] = 'bonuses_years'
            await show_bonuses_years(update, context)
        elif text == "📊 Аналітика":
            await show_analytics_options(update, context)
        elif text == "🧾 Кадровий облік":
            await show_hr_menu(update, context)
        elif text == "ℹ️ Інформація":
            await show_help_menu(update, context)
        elif text == "💱 Курс валют":
            await show_currency_rates(update, context)
        elif text == "Перевірка девальвації":
            await show_devaluation_data(update, context)
        elif text == "📘 Довідка":
            await show_user_guide(update, context)

        # 🔹 Підменю дебіторки
        elif text == "Таблиця":
            await show_debt_details(update, context)
        elif text == "Гістограма":
            await show_debt_histogram(update, context)
        elif text == "Діаграма":
            await show_debt_pie_chart(update, context)
        elif text == "Протермінована дебіторська заборгованість":
            await handle_overdue_debt(update, context)

        # 🔹 Кадровий облік
        elif text == "🗓 Залишки відпусток":
            await show_vacation_balance(update, context)
        elif text == "🕓 Відпрацьовані дні":
            await show_workdays_years(update, context)
        elif text == "👔 Інформація про стаж":
            context.user_data['menu'] = 'tenure_info'
            await show_tenure_info(update, context)

        # 🔹 Навігація
        elif text == "Назад":
            await handle_back_navigation(update, context)
        elif text == "Головне меню":
            await show_main_menu(update, context)

        # 🔹 Аналітика
        elif text in ["Аналітика за місяць", "Аналітика за рік"]:
            await handle_analytics_selection(update, context, text)
        elif text in ["2024", "2025"]:
            menu = context.user_data.get("menu")
            if menu == "workdays_years":
                await show_workdays_months(update, context)
            else:
                await handle_year_choice(update, context)
        elif text in [
            "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
            "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
        ]:
            menu = context.user_data.get("menu")
            if menu == "workdays_months":
                await show_workdays_details(update, context)
            else:
                await handle_month_choice(update, context)
        elif text in ["Дохід", "Валовий прибуток", "Маржинальність", "Кількість угод"]:
            await handle_parameter_choice(update, context)

        # 🔹 Slash-команди
        elif text.startswith("/debt"):
            await show_debt_options(update, context)
        elif text.startswith("/info"):
            await show_help_menu(update, context)
        elif text.startswith("/analytics"):
            await show_analytics_options(update, context)
        elif text.startswith("/salary"):
            await show_salary_menu(update, context)
        elif text.startswith("/menu"):
            await show_main_menu(update, context)
        elif text.startswith("/hr"):
            await show_hr_menu(update, context)

        return  # Якщо команда відома — завершити

    # 🔸 Якщо команда невідома — викликаємо GPT
    log_user_action(user_id, "GPT-request", message_id)
    logging.info(f"🤖 GPT-request від користувача {user_id}: {text}")

    gpt_response = get_gpt_response(
        text,
        user_id,
        context.user_data.get("employee_name", "Користувач"),
        message_id
    )
    await update.message.reply_text(f"🤖 {gpt_response}", parse_mode="HTML")



    



async def handle_back_navigation(update: Update, context: CallbackContext) -> None:
    menu = context.user_data.get('menu')

    # Зарплата
    if menu == 'salary_months':
        await show_salary_years(update, context)
    elif menu == 'salary_years':
        await show_salary_menu(update, context)
    elif menu == 'salary_menu':
        await show_main_menu(update, context)
    elif menu == 'bonuses_months':
        await show_bonuses_years(update, context)
    elif menu == 'bonuses_years':
        await show_salary_menu(update, context)
    elif menu == 'bonusmsg_months':
        await show_bonusmsg_years(update, context)
    elif menu == 'bonusmsg_years':
        await show_salary_menu(update, context)
    elif menu == 'leadprize_months':
        await show_leadprize_years(update, context)
    elif menu == 'leadprize_years':
        await show_salary_menu(update, context)
    elif menu == 'leadreport_months':
        await show_leadreport_years(update, context)
    elif menu == 'leadreport_years':
     await show_salary_menu(update, context)





    # Аналітика
    elif menu == 'analytics_years':
        await show_analytics_options(update, context)
    elif menu == 'parameter_selection':
        await show_analytics_years(update, context)
    elif menu == 'analytics_months':
        await show_analytics_years(update, context)

    # Дебіторка
    elif menu in ['debt_details', 'debt_histogram', 'debt_pie_chart', 'overdue_debt']:
        await show_debt_options(update, context)

    # Довідкова інформація
    elif menu in ['help_menu', 'devaluation_data']:
        await show_help_menu(update, context)

    # Кадровий облік
    elif menu in ['workdays_years', 'workdays_months', 'workdays_details', 'vacation_balance', 'tenure_info']:
        from hr.hr_handlers import show_hr_menu
        await show_hr_menu(update, context)



    # За замовчуванням — головне меню
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
    elif current_menu == 'bonuses_years':
        await show_bonuses_months(update, context)
    elif current_menu == 'bonusmsg_years':
        await show_bonusmsg_months(update, context)
    elif context.user_data.get('analytics_type') == 'monthly':
        await show_analytics_months(update, context)
    elif context.user_data.get('analytics_type') == 'yearly':
        await show_parameter_selection(update, context)
    elif current_menu == 'leadprize_years':
        await show_leadprize_months(update, context)
    elif current_menu == 'leadreport_years':
        await show_leadreport_months(update, context)


async def handle_month_choice(update: Update, context: CallbackContext) -> None:
    selected_month = update.message.text
    context.user_data['selected_month'] = selected_month
    current_menu = context.user_data.get('menu')

    if current_menu == 'salary_months':
        await show_salary_details(update, context)
    elif current_menu == 'bonuses_months':
        await send_bonuses_excel(update, context)
    elif current_menu == 'bonusmsg_months':
        await send_bonuses_message(update, context)
    elif current_menu == 'leadprize_months':
        await send_leadprizes_message(update, context)
    elif current_menu == 'leadreport_months':
        await send_leadreport_excel(update, context)

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

    set_bot_menu_sync(app)

    scheduler.add_job(check_new_payments, 'interval', seconds=180)
    scheduler.add_job(sync_payments, 'interval', seconds=1200) 
    # scheduler.add_job(check_new_devaluation_records, 'interval', seconds=10800)
    scheduler.add_job(check_new_devaluation_records, 'cron', hour=11, minute=10, timezone='Europe/Kiev') # Перевірка нових записів девальвації щодня о 10:20
    scheduler.add_job(sync_devaluation_data, 'interval', seconds=10600)  # Додаємо нову синхронізацію девальваційних даних
    
    

##################################################################################
# Щомісячне нагадування 

    # ЩОДЕННА перевірка о 09:10 за Києвом:
    scheduler.add_job(
        daily_first_workday_check,
        'cron',
        hour=9,
        minute=10,
        timezone='Europe/Kiev',
        id='monthly_reminder_daily_gate',
        replace_existing=True
    )
    logging.info("Планувальник: щоденна перевірка першого робочого дня налаштована (09:10 Europe/Kiev).")



################################################################################




    kyiv_timezone = timezone('Europe/Kiev')
    scheduler.add_job(
        store_exchange_rates,
        'cron',
        hour=10,
        minute=5,
        timezone=kyiv_timezone,
        id='daily_exchange_rates',
    )

    
    
#    scheduler.add_job(
#       send_message_to_users,
#       'cron',
#       hour=17,
#       minute=50,
#       timezone=kyiv_timezone
#    )



    scheduler.add_job(
        check_overdue_debts,  # Функція, яку потрібно виконувати
        'cron',  # Тип триггера
        day_of_week='tue',  # Запуск щовівторка
        hour=11,  # О 11:00
        timezone='Europe/Kiev'  # Часовий пояс
    )



    scheduler.add_job(send_birthday_greetings, 'cron', hour=9, minute=18, timezone='Europe/Kiev')

    # Синхронізація бонусних документів і перевірка нових документів
    scheduler.add_job(sync_bonus_docs, 'interval', minutes=6)
    scheduler.add_job(check_bonus_docs, 'interval', minutes=1)
    ################################################################################


    scheduler.add_job(sync_user_statuses, 'interval', minutes=30)  # Синхронізація статусів кожні 30 хвилин

    scheduler.start()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu))
    app.add_handler(MessageHandler(filters.COMMAND, handle_main_menu))
  

    app.add_handler(MessageHandler(filters.Regex(r"^\s*Головне меню\s*$"), show_main_menu))
    app.add_handler(CommandHandler("menu", show_main_menu))

    # ✅ Додаємо обробники для всіх команд
    
    app.add_handler(CommandHandler("debt", show_debt_options))
    app.add_handler(CommandHandler("salary", show_salary_menu))
    app.add_handler(CommandHandler("analytics", show_analytics_options))
    app.add_handler(CommandHandler("info", show_help_menu))




    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    
    app.add_handler(MessageHandler(filters.Regex("^(📉 Дебіторська заборгованість|Назад|Таблиця|Гістограма|Діаграма|💼 Зарплата|💼 Оклад|🎁 Відомість Бонуси|ℹ️ Інформація|💱 Курс валют|Перевірка девальвації|Головне меню|📊 Аналітика|Аналітика за місяць|Аналітика за рік|2024|2025|Січень|Лютий|Березень|Квітень|Травень|Червень|Липень|Серпень|Вересень|Жовтень|Листопад|Грудень|Дохід|Валовий прибуток|Маржинальність|Кількість угод|Протермінована дебіторська заборгованість|📘 Довідка|💰 Бонуси|👑 Премії керівників|🧾 Кадровий облік|🗓 Залишки відпусток|👔 Інформація про стаж|🕓 Відпрацьовані дні|📜 Відомість керівника)$"), handle_main_menu))

    #app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu))



    try:
        app.run_polling()
    finally:
        asyncio.run(shutdown(app, scheduler))

if __name__ == '__main__':
    main()




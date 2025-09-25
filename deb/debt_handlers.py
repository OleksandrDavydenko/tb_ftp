import os
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InputFile
from telegram.ext import CallbackContext
from auth import is_phone_number_in_power_bi, get_user_debt_data
from .generate_debt_graph import generate_debt_graph
from .generate_pie_chart import generate_pie_chart
from messages.weekly_overdue_debts import send_overdue_debts_by_request  # Імпорт функції для конкретного користувача

TEMP_DIR = 'temp'
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)


def _has_debt(debt_data) -> bool:
    if not debt_data:
        return False
    try:
        total = sum(float(row.get('[Sum_$]', 0) or 0) for row in debt_data)
        return total > 0
    except Exception:
        return False

# Функція для вибору між таблицею, гістограмою, діаграмою та кнопкою "Назад"
async def show_debt_options(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'debt_options'
    phone_number = context.user_data.get('phone_number')
    found, employee_name, _ = is_phone_number_in_power_bi(phone_number)

    if not found:
        # якщо користувача не знайдено — одразу головне меню
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Доступ заборонено. Поверніться в головне меню.",
                                        reply_markup=reply_markup)
        return

    debt_data = get_user_debt_data(employee_name)
    if _has_debt(debt_data):
        total_debt = sum(float(row.get('[Sum_$]', 0) or 0) for row in debt_data)
        await update.message.reply_text(
            f"Загальна сума дебіторки для {employee_name}: {total_debt:.2f} USD"
        )

        table_button = KeyboardButton("Таблиця")
        histogram_button = KeyboardButton("Гістограма")
        pie_chart_button = KeyboardButton("Діаграма")
        overdue_button = KeyboardButton("Протермінована дебіторська заборгованість")
        back_button = KeyboardButton("Назад")

        custom_keyboard = [
            [table_button, histogram_button, pie_chart_button],
            [overdue_button],
            [back_button]
        ]
        reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Оберіть, що хочете зробити:", reply_markup=reply_markup)
    else:
        # НІЯКИХ «Таблиця/Діаграма» — лише повідомлення + Головне меню
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text(f"ℹ️ У {employee_name} немає дебіторської заборгованості.",
                                        reply_markup=reply_markup)
        return


# Обробка натискання кнопки "Протермінована дебіторська заборгованість"
async def handle_overdue_debt(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'overdue_debt'  # Встановлюємо стан меню

    # Виклик функції для формування звіту
    await send_overdue_debts_by_request(update, context)

    # Додаємо кнопки "Назад" і "Головне меню"
    back_button = KeyboardButton("Назад")
    main_menu_button = KeyboardButton("Головне меню")
    

    reply_markup = ReplyKeyboardMarkup(
        [[back_button, main_menu_button]],
        one_time_keyboard=True,
        resize_keyboard=True
    )

    # Відправляємо кнопки
    await update.message.reply_text("Натисніть 'Назад' або 'Головне меню':", reply_markup=reply_markup)







# Функція для показу таблиці
async def show_debt_details(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'debt_details'
    phone_number = context.user_data['phone_number']
    found, employee_name, _ = is_phone_number_in_power_bi(phone_number)
    debt_data = get_user_debt_data(employee_name)

    if not _has_debt(debt_data):
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text(f"ℹ️ Немає даних по дебіторці для {employee_name}.",
                                        reply_markup=reply_markup)
        return

    if debt_data:
        response = f"📋 *Дебіторка для {employee_name}:*\n\n"
        total_debt = 0

        # Групування даних за клієнтами
        grouped_data = {}
        for row in debt_data:
            client = row.get('[Client]', 'Не вказано')
            account = row.get('[Account]', 'Невідомо')
            sum_debt = float(row.get('[Sum_$]', '0'))

            if client not in grouped_data:
                grouped_data[client] = []
            grouped_data[client].append({'Account': account, 'Sum_$': sum_debt})
            total_debt += sum_debt

        # Формування списку
        for client, accounts in grouped_data.items():
            response += f"👤 *Клієнт:* {client}\n"
            client_total = sum([acc['Sum_$'] for acc in accounts])
            response += f"   💵 *Сума по клієнту:* {client_total:.2f} USD\n"
            for account_data in accounts:
                account = account_data['Account']
                sum_debt = account_data['Sum_$']
                response += f"      📄 *Рахунок:* {account}, 💰 {sum_debt:.2f} USD\n"
            response += "\n"

        response += f"💵 *Загальна сума:* {total_debt:.2f} USD\n"

        await update.message.reply_text(response, parse_mode="Markdown")
    else:
        await update.message.reply_text(f"ℹ️ Немає даних для {employee_name}.")


    # Додаємо кнопки "Назад" та "Головне меню"
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)

    # Відправляємо повідомлення з кнопками
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

# Функція для показу гістограми
async def show_debt_histogram(update: Update, context: CallbackContext):
    context.user_data['menu'] = 'debt_histogram'
    phone_number = context.user_data['phone_number']
    found, employee_name, _ = is_phone_number_in_power_bi(phone_number)
    debt_data = get_user_debt_data(employee_name)

    if not _has_debt(debt_data):
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text(f"ℹ️ Немає даних по дебіторці для {employee_name}.",
                                        reply_markup=reply_markup)
        return

    if debt_data:
        file_path = generate_debt_graph(debt_data, employee_name, TEMP_DIR)
        try:
            with open(file_path, 'rb') as graph_file:
                await update.message.reply_photo(photo=InputFile(graph_file), caption="Гістограма дебіторки.")
            os.remove(file_path)
        except FileNotFoundError:
            await update.message.reply_text("Графік не був створений через відсутність даних.")
    else:
        await update.message.reply_text(f"Немає даних для {employee_name}.")

    # Додаємо кнопки "Назад" та "Головне меню"
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)

    # Відправляємо повідомлення з кнопками
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

    

# Функція для показу секторної діаграми
async def show_debt_pie_chart(update: Update, context: CallbackContext):
    context.user_data['menu'] = 'debt_pie_chart'
    phone_number = context.user_data['phone_number']
    found, employee_name, _ = is_phone_number_in_power_bi(phone_number)
    debt_data = get_user_debt_data(employee_name)

    if not _has_debt(debt_data):
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text(f"ℹ️ Немає даних по дебіторці для {employee_name}.",
                                        reply_markup=reply_markup)
        return

    if debt_data:
        file_path = generate_pie_chart(debt_data, employee_name, TEMP_DIR)
        try:
            with open(file_path, 'rb') as graph_file:
                await update.message.reply_photo(photo=InputFile(graph_file), caption="Секторна діаграма дебіторки.")
            os.remove(file_path)
        except FileNotFoundError:
            await update.message.reply_text("Діаграма не була створена через відсутність даних.")
    else:
        await update.message.reply_text(f"Немає даних для {employee_name}.")

    # Додаємо кнопки "Назад" та "Головне меню"
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)

    # Відправляємо повідомлення з кнопками
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)


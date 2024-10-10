import os
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InputFile
from telegram.ext import CallbackContext
from auth import is_phone_number_in_power_bi, get_user_debt_data
from .generate_debt_graph import generate_debt_graph
from .generate_pie_chart import generate_pie_chart

TEMP_DIR = 'temp'
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

# Функція для вибору між таблицею, гістограмою, діаграмою та кнопкою "Назад"
async def show_debt_options(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'debt_options'
    phone_number = context.user_data.get('phone_number')
    found, employee_name = is_phone_number_in_power_bi(phone_number)

    if found:
        debt_data = get_user_debt_data(employee_name)
        if debt_data:
            total_debt = sum(float(row.get('[Sum_$]', 0)) for row in debt_data if row.get('[Sum_$]', 0))
            await update.message.reply_text(f"Загальна сума дебіторки для {employee_name}: {total_debt:.2f} USD")
        else:
            await update.message.reply_text(f"Немає даних для {employee_name}.")
    
    table_button = KeyboardButton(text="Таблиця")
    histogram_button = KeyboardButton(text="Гістограма")
    pie_chart_button = KeyboardButton(text="Діаграма")
    back_button = KeyboardButton(text="Назад")
    custom_keyboard = [[table_button, histogram_button, pie_chart_button], [back_button]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)
    await update.message.reply_text("Оберіть, що хочете зробити:", reply_markup=reply_markup)

# Функція для показу таблиці
async def show_debt_details(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'debt_details'
    phone_number = context.user_data['phone_number']
    found, employee_name = is_phone_number_in_power_bi(phone_number)
    debt_data = get_user_debt_data(employee_name)

    if debt_data:
        response = f"Дебіторка для {employee_name}:\n\n"
        response += f"{'Клієнт':<30}{'Сума (USD)':<12}\n"
        response += "-" * 40 + "\n"
        total_debt = 0
        for row in debt_data:
            client = row.get('[Client]', 'Unknown Client')
            sum_debt = row.get('[Sum_$]', '0')
            response += f"{client:<30}{sum_debt:<12}\n"
            total_debt += float(sum_debt)

        response += "-" * 40 + "\n"
        response += f"{'Загальна сума':<30}{total_debt:<12}\n"

        await update.message.reply_text(f"```\n{response}```", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"Немає даних для {employee_name}.")

    # Додаємо кнопки "Назад" і "Головне меню"
    back_button = KeyboardButton(text="Назад")
    main_menu_button = KeyboardButton(text="Головне меню")
    reply_markup = ReplyKeyboardMarkup([[back_button, main_menu_button]], one_time_keyboard=True)
    await update.message.reply_text("Натисніть 'Назад' або 'Головне меню'.", reply_markup=reply_markup)

# Функція для показу гістограми
async def show_debt_histogram(update: Update, context: CallbackContext):
    context.user_data['menu'] = 'debt_histogram'
    phone_number = context.user_data['phone_number']
    found, employee_name = is_phone_number_in_power_bi(phone_number)
    debt_data = get_user_debt_data(employee_name)

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

    # Додаємо кнопки "Назад" і "Головне меню"
    back_button = KeyboardButton(text="Назад")
    main_menu_button = KeyboardButton(text="Головне меню")
    reply_markup = ReplyKeyboardMarkup([[back_button, main_menu_button]], one_time_keyboard=True)
    await update.message.reply_text("Натисніть 'Назад' або 'Головне меню'.", reply_markup=reply_markup)

# Функція для показу секторної діаграми
async def show_debt_pie_chart(update: Update, context: CallbackContext):
    context.user_data['menu'] = 'debt_pie_chart'
    phone_number = context.user_data['phone_number']
    found, employee_name = is_phone_number_in_power_bi(phone_number)
    debt_data = get_user_debt_data(employee_name)

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

    # Додаємо кнопки "Назад" і "Головне меню"
    back_button = KeyboardButton(text="Назад")
    main_menu_button = KeyboardButton(text="Головне меню")
    reply_markup = ReplyKeyboardMarkup([[back_button, main_menu_button]], one_time_keyboard=True)
    await update.message.reply_text("Натисніть 'Назад' або 'Головне меню'.", reply_markup=reply_markup)

# Функція для повернення до головного меню
async def show_main_menu(update: Update, context: CallbackContext):
    context.user_data['menu'] = 'main_menu'
    debt_button = KeyboardButton(text="Дебіторка")
    custom_keyboard = [[debt_button]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)

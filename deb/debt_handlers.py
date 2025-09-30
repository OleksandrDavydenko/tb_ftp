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

    # Підтримка обох сигнатур: (found, employee_name) і (found, employee_name, status)
    res = is_phone_number_in_power_bi(phone_number)
    if isinstance(res, tuple):
        if len(res) == 3:
            found, employee_name, _ = res
        else:
            found, employee_name = res
    else:
        # на всяк випадок
        found, employee_name = (False, None)

    if not found or not employee_name:
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Доступ заборонено. Поверніться в головне меню.",
                                        reply_markup=reply_markup)
        return

    debt_data = get_user_debt_data(employee_name)

    if not _has_debt(debt_data):
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text(
            f"ℹ️ Немає даних по дебіторці для {employee_name}.",
            reply_markup=reply_markup
        )
        return

    # ── ГРУПУВАННЯ: Client → Deal → [Account rows]
    grouped = {}
    total_debt = 0.0

    for row in debt_data:
        client = row.get('[Client]', 'Не вказано') or 'Не вказано'
        # Fallback: Deal або DealNumber
        deal   = (row.get('[Deal]') or row.get('[DealNumber]') or 'Без № угоди')
        acc    = row.get('[Account]', 'Невідомо') or 'Невідомо'
        amt    = float(row.get('[Sum_$]', 0) or 0)

        grouped.setdefault(client, {}).setdefault(deal, []).append({'Account': acc, 'Sum_$': amt})
        total_debt += amt  # рахуємо лише по рядках рахунків

    # ── ФОРМУВАННЯ ПОВІДОМЛЕННЯ
    response_lines = [f"📋 *Дебіторка для {employee_name}:*", ""]

    for client, deals in grouped.items():
        response_lines.append(f"👤 *Клієнт:* {client}")
        client_total = 0.0

        for deal, acc_rows in deals.items():
            deal_total = sum(r['Sum_$'] for r in acc_rows)
            client_total += deal_total

            response_lines.append(f"   📑 *Угода №:* {deal}")
            for r in acc_rows:
                response_lines.append(f"      📄 *Рахунок:* {r['Account']}, 💰 {r['Sum_$']:.2f} USD")
            response_lines.append(f"      🔹 *Разом по угоді:* {deal_total:.2f} USD\n")

        response_lines.append(f"   💵 *Разом по клієнту:* {client_total:.2f} USD\n")

    response_lines.append(f"💵 *Загальна сума:* {total_debt:.2f} USD")
    response = "\n".join(response_lines)

    await update.message.reply_text(response, parse_mode="Markdown")

    # Кнопки навігації
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)
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


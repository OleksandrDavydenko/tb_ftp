import os
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InputFile
from telegram.ext import CallbackContext
from auth import is_phone_number_in_power_bi, get_user_debt_data
from .generate_debt_graph import generate_debt_graph
from .generate_pie_chart import generate_pie_chart
from messages.weekly_overdue_debts import send_overdue_debts_by_request  # Імпорт функції для конкретного користувача
from utils.name_aliases import display_name


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
    employee_name = context.user_data.get('employee_name')

    if not employee_name:
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Доступ заборонено. Поверніться в головне меню.",
                                        reply_markup=reply_markup)
        return

    nice_name = display_name(employee_name)
    custom_keyboard = [
        [KeyboardButton("Таблиця"), KeyboardButton("Гістограма"), KeyboardButton("Діаграма")],
        [KeyboardButton("Протермінована дебіторська заборгованість")],
        [KeyboardButton("Назад")]
    ]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(f"📉 Дебіторка {nice_name} — оберіть формат:", reply_markup=reply_markup)


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

def split_message(text, max_length=4096):
     """
     Функція для поділу довгого тексту на частини, якщо його довжина перевищує max_length.
     """
     parts = []
     while len(text) > max_length:
         split_point = text.rfind('\n', 0, max_length)
         if split_point == -1:
             split_point = max_length
         parts.append(text[:split_point])
         text = text[split_point:].strip()
     parts.append(text)
     return parts


 # Розбиваємо повідомлення на частини, якщо воно занадто велике
async def send_large_message(update, context, message):
     parts = split_message(message)
     for part in parts:
         await update.message.reply_text(part)






def fmt(n: float) -> str:
    """Форматує число як 252 256.65 (пробіл між тисячами)."""
    return f"{n:,.2f}".replace(",", " ").replace("\xa0", " ")

async def show_debt_details(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'debt_details'
    phone_number = context.user_data['phone_number']

    # Підтримка обох сигнатур is_phone_number_in_power_bi: 2 або 3 значення
    res = is_phone_number_in_power_bi(phone_number)
    if isinstance(res, tuple) and len(res) == 3:
        found, employee_name, _ = res
    else:
        found, employee_name = res if isinstance(res, tuple) else (False, None)

    if not found or not employee_name:
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Доступ заборонено. Поверніться в головне меню.", reply_markup=reply_markup)
        return

    debt_data = get_user_debt_data(employee_name)

    if not _has_debt(debt_data):
        reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Головне меню")]],
                                           one_time_keyboard=True, resize_keyboard=True)
        nice_name  = display_name(employee_name)
        await update.message.reply_text(f"ℹ️ Немає даних по дебіторці для {nice_name}.", reply_markup=reply_markup)
        return

    # ── ГРУПУВАННЯ: Client → Deal → [Account rows]
    grouped = {}
    total_debt = 0.0

    for row in debt_data:
        client = row.get('[Client]', 'Не вказано') or 'Не вказано'
        deal   = (row.get('[Deal]') or row.get('[DealNumber]') or 'Без № угоди')
        acc    = row.get('[Account]', 'Невідомо') or 'Невідомо'
        amt    = float(row.get('[Sum_$]', 0) or 0)

        grouped.setdefault(client, {}).setdefault(deal, []).append({'Account': acc, 'Sum_$': amt})
        total_debt += amt

    nice_name = display_name(employee_name)
    # ── Формування повідомлення у стилі «Компактний список»
    lines = [f"📋 Дебіторка для {nice_name}:", ""]

    for client, deals in grouped.items():
        # попередньо порахуємо суми по угодах
        deal_totals = {d: sum(r['Sum_$'] for r in rows) for d, rows in deals.items()}
        client_total = sum(deal_totals.values())

        lines.append(f"👤 Клієнт: {client}")

        # сортуємо угоди за сумою (DESC)
        for deal in sorted(deals, key=lambda d: deal_totals[d], reverse=True):
            acc_rows = sorted(deals[deal], key=lambda r: r['Sum_$'], reverse=True)
            deal_sum = deal_totals[deal]

            lines.append(f"📑 Угода {deal} — {fmt(deal_sum)} USD")
            for r in acc_rows:
                lines.append(f"   ▪️ Рахунок {r['Account']} — {fmt(r['Sum_$'])}")
            lines.append(f"   └─ Разом по угоді: {fmt(deal_sum)} USD\n")

        lines.append(f"💵 Разом по клієнту: {fmt(client_total)} USD\n")

    lines.append(f"💰 Загальна сума: {fmt(total_debt)} USD")
    message = "\n".join(lines)

    await send_large_message(update, context, message)








    # Кнопки навігації
    reply_markup = ReplyKeyboardMarkup([[KeyboardButton("Назад"), KeyboardButton("Головне меню")]],
                                       one_time_keyboard=True, resize_keyboard=True)
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
        nice_name = display_name(employee_name)
        await update.message.reply_text(f"ℹ️ Немає даних по дебіторці для {nice_name}.",
                                        reply_markup=reply_markup)
        return

    if debt_data:
        nice_name = display_name(employee_name)
        file_path = generate_debt_graph(debt_data, nice_name, TEMP_DIR)
        try:
            with open(file_path, 'rb') as graph_file:
                await update.message.reply_photo(photo=InputFile(graph_file), caption="Гістограма дебіторки.")
            os.remove(file_path)
        except FileNotFoundError:
            await update.message.reply_text("Графік не був створений через відсутність даних.")
    else:
        nice_name = display_name(employee_name)
        await update.message.reply_text(f"Немає даних для {nice_name}.")

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
        nice_name = display_name(employee_name)
        await update.message.reply_text(f"ℹ️ Немає даних по дебіторці для {nice_name}.",
                                        reply_markup=reply_markup)
        return

    if debt_data:
        nice_name = display_name(employee_name)
        file_path = generate_pie_chart(debt_data, nice_name, TEMP_DIR)
        try:
            with open(file_path, 'rb') as graph_file:
                await update.message.reply_photo(photo=InputFile(graph_file), caption="Секторна діаграма дебіторки.")
            os.remove(file_path)
        except FileNotFoundError:
            await update.message.reply_text("Діаграма не була створена через відсутність даних.")
    else:
        nice_name = display_name(employee_name)
        await update.message.reply_text(f"Немає даних для {nice_name}.")

    # Додаємо кнопки "Назад" та "Головне меню"
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)

    # Відправляємо повідомлення з кнопками
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)


from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from auth import get_power_bi_token
import requests

async def show_vacation_balance(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'hr_main'
    employee_name = context.user_data.get('employee_name')

    if not employee_name:
        await update.message.reply_text("❌ Неможливо визначити ім'я співробітника.")
        return

    token = get_power_bi_token()
    if not token:
        await update.message.reply_text("❌ Не вдалося отримати токен для доступу до Power BI.")
        return

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'

    dax_query = {
        "queries": [
            {
                "query": f"""
                    EVALUATE
                    SELECTCOLUMNS(
                        FILTER(
                            VacationBalance,
                            VacationBalance[employee_name] = \"{employee_name}\"
                        ),
                        \"Year\", VacationBalance[year],
                        \"Organization\", VacationBalance[organization],
                        \"Accrued\", VacationBalance[accrued_days],
                        \"Used\", VacationBalance[used_days],
                        \"Remaining\", VacationBalance[remaining_days]
                    )
                """
            }
        ],
        "serializerSettings": {"includeNulls": True}
    }

    response = requests.post(power_bi_url, headers=headers, json=dax_query)

    if response.status_code != 200:
        await update.message.reply_text("❌ Не вдалося отримати дані про відпустки.")
        return

    data = response.json()
    rows = data['results'][0]['tables'][0].get('rows', [])

    if not rows:
        await update.message.reply_text("ℹ️ Немає даних про відпустки.")
        return

    # Форматування таблиці вручну
    message = f"📄 *Залишки відпусток для {employee_name}:*\n\n"
    message += f"{'Організація':<15} {'Рік':<5} {'Нараховано':<12} {'Використано':<12} {'Залишок':<10}\n"
    message += "-" * 60 + "\n"

    for row in rows:
        org = str(row['Organization'])
        year = str(row['Year'])
        accrued = str(row['Accrued'])
        used = str(row['Used'])
        remaining = str(row['Remaining'])
        message += f"{org:<15} {year:<5} {accrued:<12} {used:<12} {remaining:<10}\n"

    await update.message.reply_text(f"```\n{message}\n```", parse_mode="Markdown")

    # Кнопки "Назад" і "Головне меню"
    keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("⬅️ Повернення:", reply_markup=reply_markup)

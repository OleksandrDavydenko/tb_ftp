from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from auth import get_power_bi_token
import requests
import logging

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

    logging.info(f"📤 Відправляємо запит до Power BI для {employee_name}")
    response = requests.post(power_bi_url, headers=headers, json=dax_query)

    # ⚠️ Додаткове логування
    logging.info(f"📥 Статус відповіді Power BI: {response.status_code}")
    try:
        logging.info(f"📄 Вміст відповіді: {response.text}")
    except Exception as e:
        logging.warning(f"⚠️ Неможливо прочитати тіло відповіді: {e}")

    if response.status_code != 200:
        await update.message.reply_text("❌ Не вдалося отримати дані про відпустки.")
        return

    try:
        data = response.json()
        rows = data['results'][0]['tables'][0].get('rows', [])
    except Exception as e:
        logging.error(f"❌ Помилка при розборі JSON: {e}")
        await update.message.reply_text("❌ Виникла помилка при обробці відповіді Power BI.")
        return

    if not rows:
        await update.message.reply_text("ℹ️ Немає даних про відпустки.")
        return

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

    keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("⬅️ Повернення:", reply_markup=reply_markup)

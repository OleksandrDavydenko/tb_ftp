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
                            employee_vacation_summary,
                            STARTSWITH(employee_vacation_summary[employee_name] = \"{employee_name}\"
                        ),
                        "Year", employee_vacation_summary[year],
                        "Organization", employee_vacation_summary[organization],
                        "Accrued", employee_vacation_summary[accrued_days],
                        "Used", employee_vacation_summary[used_days],
                        "Remaining", employee_vacation_summary[remaining_days]
                    )
                """
            }
        ],
        "serializerSettings": {"includeNulls": True}
    }

    logging.info(f"📤 Відправляємо запит до Power BI для {employee_name}")
    response = requests.post(power_bi_url, headers=headers, json=dax_query)

    logging.info(f"📥 Статус відповіді Power BI: {response.status_code}")
    logging.info(f"📄 Вміст відповіді: {response.text}")

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

    # Побудова повідомлення
    message = f"📄 Відпустки для: *{employee_name}*\n\n"

    total_accrued = total_used = total_remaining = 0

    for row in rows:
        year = str(row['[Year]'])
        org = str(row['[Organization]'])
        accrued = float(row['[Accrued]'] or 0)
        used = float(row['[Used]'] or 0)
        remaining = float(row['[Remaining]'] or 0)

        total_accrued += accrued
        total_used += used
        total_remaining += remaining

        message += (
            f"📆 *{year}* | 🏢 {org}\n"
            f"📈 Нараховано: {accrued:.1f} днів\n"
            f"📉 Використано: {used:.1f} днів\n"
            f"📌 Залишок: {remaining:.1f} днів\n\n"
        )

    message += (
        "🧾 *Підсумок:*\n"
        f"🔹 Всього нараховано: {total_accrued:.1f} днів\n"
        f"🔸 Всього використано: {total_used:.1f} днів\n"
        f"✅ Залишок: {total_remaining:.1f} днів"
    )

    await update.message.reply_text(message, parse_mode="Markdown")

    keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("Оберіть дію:", reply_markup=reply_markup)

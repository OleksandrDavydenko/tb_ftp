from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
from auth import get_power_bi_token
from utils.get_inn import get_employee_inn
import requests
import logging
from datetime import datetime, date

from utils.name_aliases import display_name
from utils.thinking import with_typing_action


@with_typing_action
async def show_vacation_balance(update: Update, context: CallbackContext) -> None:
    context.user_data['menu'] = 'vacation_balance'
    employee_name = context.user_data.get('employee_name')

    if not employee_name:
        await update.message.reply_text("❌ Неможливо визначити ім'я співробітника.")
        return

    # Отримуємо токен для доступу до Power BI
    token = get_power_bi_token()
    if not token:
        await update.message.reply_text("❌ Не вдалося отримати токен для доступу до Power BI.")
        return

    # Отримуємо INN співробітника
    tax_code = get_employee_inn(employee_name)

    if not tax_code:
        logging.info(f"⚠️ Не вдалося знайти INN для {employee_name}. Використовуємо фільтрацію по імені.")
    else:
        logging.info(f"✅ INN знайдено для {employee_name}: {tax_code}")

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'

    def _build_dax(filter_expr: str) -> dict:
        return {
            "queries": [{
                "query": f"""
                    EVALUATE
                    SELECTCOLUMNS(
                        FILTER(employee_vacation_summary, {filter_expr}),
                        "Year",         employee_vacation_summary[year],
                        "AccrualStart", employee_vacation_summary[accrual_start_date],
                        "Accrued",      employee_vacation_summary[accrued_days],
                        "Used",         employee_vacation_summary[used_days],
                        "Remaining",    employee_vacation_summary[remaining_days]
                    )
                """
            }],
            "serializerSettings": {"includeNulls": True}
        }

    if tax_code:
        dax_query = _build_dax(f'employee_vacation_summary[tax_code] = "{tax_code}"')
    else:
        dax_query = _build_dax(
            f'LEFT(employee_vacation_summary[employee_name], LEN("{employee_name}")) = "{employee_name}"'
        )

    logging.info(f"📤 Відправляємо запит до Power BI для {employee_name} з INN {tax_code if tax_code else 'не знайдено'}")
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
        await update.message.reply_text("ℹ️ Немає даних про залишки відпустки.")
        return

    today = date.today()
    proportional = 0.0
    full_remaining = 0.0
    period_end_str = None

    # Шукаємо річну норму з останнього закритого року (щоб не екстраполювати темп)
    annual_norm = None
    for row in rows:
        try:
            y = int(row.get('[Year]', 0))
            sr = (row.get('[AccrualStart]') or '')[:10]
            ast = datetime.strptime(sr, '%Y-%m-%d').date()
            pe = date(y + 1, ast.month, ast.day)
            if pe <= today:
                annual_norm = float(row.get('[Accrued]', 0) or 0)
        except ValueError:
            pass

    for row in rows:
        remaining = float(row.get('[Remaining]', 0) or 0)
        accrued   = float(row.get('[Accrued]',   0) or 0)
        year      = int(row.get('[Year]', 0))
        start_raw = (row.get('[AccrualStart]') or '')[:10]

        proportional += remaining

        try:
            accrual_start = datetime.strptime(start_raw, '%Y-%m-%d').date()
            period_start  = date(year,     accrual_start.month, accrual_start.day)
            period_end    = date(year + 1, accrual_start.month, accrual_start.day)
        except ValueError:
            full_remaining += remaining
            continue

        if period_start <= today < period_end:
            if annual_norm is not None and annual_norm > accrued:
                # коректний підхід: донарахується рівно стільки, скільки лишилось до норми
                still_to_accrue = annual_norm - accrued
            elif accrued > 0:
                # fallback: екстраполяція темпу (якщо немає закритого року для норми)
                days_elapsed = (today - period_start).days
                if days_elapsed > 0:
                    still_to_accrue = (period_end - today).days * (accrued / days_elapsed)
                else:
                    still_to_accrue = 0
            else:
                still_to_accrue = 0
            full_remaining += remaining + still_to_accrue
            period_end_str  = period_end.strftime('%d.%m.%y')
        else:
            full_remaining += remaining

    nice_name = display_name(employee_name)
    lines = [f"🧑 {nice_name}"]
    lines.append(f"1. Пропорційно (на сьогодні) — {proportional:.0f} календарних днів")
    if period_end_str:
        lines.append(f"2. Загальний залишок — {full_remaining:.0f} календарних днів до {period_end_str} (включно)")
    else:
        lines.append(f"2. Загальний залишок — {full_remaining:.0f} календарних днів")
    message = '\n'.join(lines)

    await update.message.reply_text(message)

    keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("Оберіть дію:", reply_markup=reply_markup)

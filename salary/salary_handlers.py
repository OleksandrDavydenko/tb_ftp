import asyncio
import datetime
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import logging

from .salary_queries import (
    get_salary_data,
    get_salary_payments,
    get_bonuses,
    format_salary_table,
    get_bonus_payments
)

# ──────────────────────────────────────────────────────────────────────────────
# Константи
# ──────────────────────────────────────────────────────────────────────────────
MONTHS_UA = [
    "Січень",
    "Лютий",
    "Березень",
    "Квітень",
    "Травень",
    "Червень",
    "Липень",
    "Серпень",
    "Вересень",
    "Жовтень",
    "Листопад",
    "Грудень",
]
MONTHS_MAP = {name: idx + 1 for idx, name in enumerate(MONTHS_UA)}

# ──────────────────────────────────────────────────────────────────────────────
# Меню вибору року / місяця
# ──────────────────────────────────────────────────────────────────────────────
async def show_salary_years(update: Update, context: CallbackContext) -> None:
    current_year = datetime.datetime.now().year
    years = [str(y) for y in range(2025, current_year + 1)]

    kb = [[KeyboardButton(y)] for y in years] + [[KeyboardButton("Назад")]]
    context.user_data["menu"] = "salary_years"
    await update.message.reply_text("Оберіть рік:", reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True))


async def show_salary_months(update: Update, context: CallbackContext) -> None:
    kb = [[KeyboardButton(m)] for m in MONTHS_UA]
    kb.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])
    context.user_data["menu"] = "salary_months"
    await update.message.reply_text("Оберіть місяць:", reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True))

# ──────────────────────────────────────────────────────────────────────────────
# Показ розрахункового листа
# ──────────────────────────────────────────────────────────────────────────────
async def show_salary_details(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    year = context.user_data.get("selected_year")
    month_name = context.user_data.get("selected_month")

    if not (employee and year and month_name):
        await update.message.reply_text("Помилка: спочатку оберіть рік та місяць.")
        return

    month_num = MONTHS_MAP.get(month_name)
    if month_num is None:
        await update.message.reply_text("Невідомий місяць.")
        return

    # Дані Power BI
    salary_rows = get_salary_data(employee, year, month_name)
    payments_rows = get_salary_payments(employee, year, month_name)
    bonus_rows = get_bonuses(employee, year, month_name)
    bonus_payments = get_bonus_payments(employee, year, month_name)

    if not (salary_rows or payments_rows or bonus_rows or bonus_payments):
        await update.message.reply_text("Немає даних для вибраного періоду.")
        return



    main_table, bonus_table = format_salary_table(
        salary_rows, employee, int(year), month_num,
        payments_rows or [], bonus_rows or [], bonus_payments or []
    )

    # --- 1️⃣ основна таблиця (завжди)
    main_msg = (
        heading("Розрахунковий лист") +
        f"Співробітник: {employee}\n" +
        f"Період: {month_name} {year}\n\n" +
        code_block(main_table)
    )
    await _send_autodelete(update, context, main_msg)

    # --- 2️⃣ бонуси (якщо є хоча б щось)
    if bonus_rows or bonus_payments:
        if bonus_table:
            logging.info("✅ Бонусна таблиця сформована:")
            logging.info(bonus_table)
            bonus_msg = heading("Бонуси") + code_block(bonus_table)
            await _send_autodelete(update, context, bonus_msg)
        else:
            logging.warning("⚠️ Бонусна таблиця порожня або не сформована.")

    # Навігація
    nav_kb = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    await update.message.reply_text("Виберіть опцію:", reply_markup=ReplyKeyboardMarkup(nav_kb, one_time_keyboard=True, resize_keyboard=True))

# ──────────────────────────────────────────────────────────────────────────────
#   Service helpers
# ──────────────────────────────────────────────────────────────────────────────

def heading(text: str) -> str:
    return f"*{text}*\n"


def code_block(content: str) -> str:
    return f"```\n{content}\n```"


async def _send_autodelete(update: Update, context: CallbackContext, message_text: str, *, delay: int = 60):
    msg = await update.message.reply_text(message_text, parse_mode="Markdown")
    warn = await update.message.reply_text("⚠️ Це повідомлення буде видалено через 60 секунд!")
    asyncio.create_task(_delete_later(context, update.effective_chat.id, [msg.message_id, warn.message_id], delay))


async def _delete_later(context: CallbackContext, chat_id: int, mids: list[int], delay: int):
    await asyncio.sleep(delay)
    for mid in mids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass

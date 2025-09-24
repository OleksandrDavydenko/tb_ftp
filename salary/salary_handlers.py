import asyncio
import datetime
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import logging

import os
import shutil
from .bonuses_report import generate_excel 

from .salary_queries import (
    get_salary_data,
    get_salary_payments,
    get_bonuses,
    format_salary_table,
    get_bonus_payments,
    get_prize_payments 
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
# Підменю "Зарплата"
# ──────────────────────────────────────────────────────────────────────────────
async def show_salary_menu(update: Update, context: CallbackContext) -> None:
    kb = [
        [KeyboardButton("💼 Оклад"), KeyboardButton("💰 Бонуси")],   # ← один рядок, дві кнопки
        [KeyboardButton("🎁 Відомість Бонуси")],                    # ← окремий рядок
        [KeyboardButton("Головне меню")],                           # ← окремий рядок
    ]
    context.user_data["menu"] = "salary_menu"
    await update.message.reply_text(
        "Оберіть розділ:",
        reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True, resize_keyboard=True)
    )






async def show_bonuses_stub(update: Update, context: CallbackContext) -> None:
    context.user_data["menu"] = "bonuses_stub"
    await update.message.reply_text("🎁 Розділ «Бонуси» — функціонал у розробці. Слідкуйте за оновленнями!")
    nav = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    await update.message.reply_text("Виберіть опцію:", reply_markup=ReplyKeyboardMarkup(nav, one_time_keyboard=True, resize_keyboard=True))


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
# Відомість Бонуси: вибір року/місяця та відправка Excel
# ──────────────────────────────────────────────────────────────────────────────
async def show_bonuses_years(update: Update, context: CallbackContext) -> None:
    current_year = datetime.datetime.now().year
    years = [str(y) for y in range(2025, current_year + 1)]
    kb = [[KeyboardButton(y)] for y in years] + [[KeyboardButton("Назад")]]
    context.user_data["menu"] = "bonuses_years"
    await update.message.reply_text(
        "Оберіть рік (Відомість Бонуси):",
        reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True)
    )

async def show_bonuses_months(update: Update, context: CallbackContext) -> None:
    kb = [[KeyboardButton(m)] for m in MONTHS_UA]
    kb.append([KeyboardButton("Назад"), KeyboardButton("Головне меню")])
    context.user_data["menu"] = "bonuses_months"
    await update.message.reply_text(
        "Оберіть місяць (Відомість Бонуси):",
        reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True)
    )

async def send_bonuses_excel(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    year     = context.user_data.get("selected_year")
    month    = context.user_data.get("selected_month")

    # валідація вибору
    if not (employee and year and month):
        await update.message.reply_text("Помилка: спочатку оберіть рік та місяць.")
        return

    month_num = MONTHS_MAP.get(month)
    if month_num is None:
        await update.message.reply_text("Невідомий місяць.")
        return

    period_ym = f"{year}-{month_num:02d}"
    wait_msg = await update.message.reply_text("⏳ Формую відомість бонусів…")

    xlsx_path = None
    try:
        # 1) Генеруємо тимчасовий файл у /tmp (Heroku-friendly)
        xlsx_path = generate_excel(employee, period_ym)

        # якщо генератор повернув None або шлях не існує — немає нарахувань
        if not xlsx_path or not os.path.exists(xlsx_path):
            await update.message.reply_text(
                f"ℹ️ У вас відсутні нарахування бонусів за {month} {year}."
            )
            return

        # 2) Надсилаємо файл
        with open(xlsx_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=os.path.basename(xlsx_path),
                caption=f"Відомість бонусів • {employee} • {period_ym}"
            )

    except ValueError as e:
        # generate_excel може підняти ValueError, якщо даних немає
        await update.message.reply_text(
            f"ℹ️ У вас відсутні нарахування бонусів за {month} {year}."
        )
        return

    except Exception as e:
        logging.exception("Помилка генерації бонусів")
        await update.message.reply_text(f"❌ Не вдалося сформувати файл: {e}")
        return

    finally:
        # 3) Прибираємо тимчасові файли/папки
        try:
            if xlsx_path:
                tmp_dir = os.path.dirname(xlsx_path)
                if os.path.isdir(tmp_dir):
                    shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

        # 4) Прибираємо повідомлення “формую…”
        try:
            if wait_msg:
                await context.bot.delete_message(update.effective_chat.id, wait_msg.message_id)
        except Exception:
            pass



# ──────────────────────────────────────────────────────────────────────────────
# Показ розрахункового листа ОКЛАД
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

    # Отримання даних
    salary_rows = get_salary_data(employee, year, month_name)
    payments_rows = get_salary_payments(employee, year, month_name)
    bonus_rows = get_bonuses(employee, year, month_name)
    bonus_payments = get_bonus_payments(employee, year, month_name)
    prize_payments = get_prize_payments(employee, year, month_name)


    if not (salary_rows or payments_rows or bonus_rows or bonus_payments):
        await update.message.reply_text("Немає даних для вибраного періоду.")
        return

    # Формування таблиць
    main_table, bonus_table, prize_table = format_salary_table(
        salary_rows, employee, int(year), month_num,
        payments_rows or [], bonus_rows or [], bonus_payments or [], prize_payments or []
    )


    # --- 1️⃣ основна таблиця (обов'язково)
    main_msg = (
        heading("Оклад/KPI") +
        f"Співробітник: {employee}\n" +
        f"Період: {month_name} {year}\n\n" +
        code_block(main_table)
    )
    await _send_autodelete(update, context, main_msg)

    # --- 2️⃣ бонуси (якщо є)
    if bonus_rows or bonus_payments:
        if bonus_table and "Нарахування бонусів відсутні" not in bonus_table:
            bonus_msg = (
                heading("Бонуси") +
                f"Співробітник: {employee}\n" +
                f"Період: {month_name} {year}\n\n" +
                code_block(bonus_table)
            )
            await _send_autodelete(update, context, bonus_msg)

    # --- 3️⃣ премії (якщо є нарахування або виплати премій)
    has_prize_accruals = any(
        float(row.get("[Нараховано Премії UAH]", 0)) > 0 or float(row.get("[Нараховано Премії USD]", 0)) > 0
        for row in salary_rows or []
    )
    has_prize_payments = prize_payments and len(prize_payments) > 0

    if has_prize_accruals or has_prize_payments:
        if prize_table:
            prize_msg = (
                heading("Премії") +
                f"Співробітник: {employee}\n" +
                f"Період: {month_name} {year}\n\n" +
                code_block(prize_table)
            )
            await _send_autodelete(update, context, prize_msg)


    # --- Навігація
    nav_kb = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    await update.message.reply_text(
        "Виберіть опцію:", 
        reply_markup=ReplyKeyboardMarkup(nav_kb, one_time_keyboard=True, resize_keyboard=True)
    )


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

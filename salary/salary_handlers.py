import asyncio
import datetime
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackContext
import logging

import os
import shutil
from .bonuses_report import generate_excel
from .bonuses_message import build_bonus_message_for_period
from .lead_prizes_message import build_lead_prizes_message_for_period
from .lead_prizes_report import generate_hod_excel

from .salary_queries import (
    get_salary_data,
    get_salary_payments,
    get_bonuses,
    format_salary_table,
    get_bonus_payments,
    get_prize_payments,
    get_employee_accounts_3330_3320,
    # ↓↓↓ додати
    get_available_years_salary, get_available_months_salary,
    get_available_years_bonuses, get_available_months_bonuses,
    get_available_years_prizes,  get_available_months_prizes,
)


from utils.name_aliases import display_name

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

_NAV_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]],
    resize_keyboard=True,
    one_time_keyboard=True,
)


# ──────────────────────────────────────────────────────────────────────────────
# Підменю "Зарплата"
# ──────────────────────────────────────────────────────────────────────────────

async def show_salary_menu(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    codes = get_employee_accounts_3330_3320(employee) if employee else set()

    # 1-й ряд — Оклад (повна ширина)
    rows = [[KeyboardButton("💼 Оклад")]]

    # 2-й ряд — Бонуси + Відомість Бонуси (разом, якщо є 3330)
    if "3330" in codes:
        rows.append([KeyboardButton("💰 Бонуси"), KeyboardButton("🎁 Відомість Бонуси")])

    # 3-й ряд — Премії керівників + Відомість керівника (разом, якщо є 3320)
    if "3320" in codes:
        rows.append([KeyboardButton("👑 Премії керівників"), KeyboardButton("📜 Відомість керівника")])

    # 4-й ряд — Головне меню
    rows.append([KeyboardButton("Головне меню")])

    context.user_data["menu"] = "salary_menu"
    await update.message.reply_text(
        "Оберіть розділ:",
        reply_markup=ReplyKeyboardMarkup(rows, one_time_keyboard=True, resize_keyboard=True)
    )




# ──────────────────────────────────────────────────────────────────────────────
# Відомість керівника (Excel): рік → місяць → файл
# ──────────────────────────────────────────────────────────────────────────────

async def show_leadreport_years(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    years = get_available_years_prizes(employee) if employee else []
    context.user_data["menu"] = "leadreport_years"
    msg = update.effective_message
    if not years:
        await msg.reply_text("ℹ️ Немає даних по відомості керівника.", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(y, callback_data=f"leadreport_year:{y}")] for y in years])
    await msg.reply_text("Оберіть рік (Відомість керівника):", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def show_leadreport_months(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    year = context.user_data.get("selected_year")
    months = get_available_months_prizes(employee, year) if (employee and year) else []
    context.user_data["menu"] = "leadreport_months"
    msg = update.effective_message
    if not months:
        await msg.reply_text("ℹ️ Немає даних за обраний рік (Відомість керівника).", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(m, callback_data=f"leadreport_month:{m}")] for m in months])
    await msg.reply_text("Оберіть місяць (Відомість керівника):", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def send_leadreport_excel(update: Update, context: CallbackContext) -> None:
    head = context.user_data.get("employee_name")  # Керівник = поточний користувач
    year = context.user_data.get("selected_year")
    month = context.user_data.get("selected_month")

    msg = update.effective_message

    if not (head and year and month):
        await msg.reply_text("Помилка: спочатку оберіть рік та місяць.")
        return

    month_num = MONTHS_MAP.get(month)
    if month_num is None:
        await msg.reply_text("Невідомий місяць.")
        return

    period_ym = f"{year}-{month_num:02d}"
    wait_msg = await msg.reply_text("⏳ Формую відомість керівника…")

    xlsx_path = None
    try:
        xlsx_path = generate_hod_excel(head, period_ym)
        with open(xlsx_path, "rb") as f:
            await msg.reply_document(
                document=f,
                filename=os.path.basename(xlsx_path),
                caption=f"Відомість керівника • {head} • {period_ym}"
            )
    except ValueError:
        await msg.reply_text(f"ℹ️ Відсутні дані за {month} {year}.")
    except Exception as e:
        await msg.reply_text(f"❌ Не вдалося сформувати файл: {e}")
    finally:
        # приберемо тимчасову папку
        try:
            if xlsx_path:
                tmp_dir = os.path.dirname(xlsx_path)
                import shutil
                if os.path.isdir(tmp_dir):
                    shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        try:
            if wait_msg:
                await context.bot.delete_message(update.effective_chat.id, wait_msg.message_id)
        except Exception:
            pass

    await msg.reply_text("Виберіть опцію:", reply_markup=_NAV_KB)




# ──────────────────────────────────────────────────────────────────────────────
# Премії керівників: рік → місяць → повідомлення
# ──────────────────────────────────────────────────────────────────────────────
# перенаправляємо користувача одразу до вибору року.
async def show_lead_prizes_stub(update: Update, context: CallbackContext) -> None:
    await show_leadprize_years(update, context)

async def show_leadprize_years(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    years = get_available_years_prizes(employee) if employee else []
    context.user_data["menu"] = "leadprize_years"
    msg = update.effective_message
    if not years:
        await msg.reply_text("ℹ️ Немає даних по преміях керівників.", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(y, callback_data=f"leadprize_year:{y}")] for y in years])
    await msg.reply_text("Оберіть рік (Премії керівників):", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def show_leadprize_months(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    year = context.user_data.get("selected_year")
    months = get_available_months_prizes(employee, year) if (employee and year) else []
    context.user_data["menu"] = "leadprize_months"
    msg = update.effective_message
    if not months:
        await msg.reply_text("ℹ️ Немає премій за обраний рік.", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(m, callback_data=f"leadprize_month:{m}")] for m in months])
    await msg.reply_text("Оберіть місяць (Премії керівників):", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def send_leadprizes_message(update: Update, context: CallbackContext) -> None:
    employee   = context.user_data.get("employee_name")
    year       = context.user_data.get("selected_year")
    month_name = context.user_data.get("selected_month")

    msg = update.effective_message

    if not (employee and year and month_name):
        await msg.reply_text("Помилка: спочатку оберіть рік та місяць.")
        return

    month_num = MONTHS_MAP.get(month_name)
    if month_num is None:
        await msg.reply_text("Невідомий місяць.")
        return

    try:
        text = build_lead_prizes_message_for_period(employee, int(year), int(month_num))
    except Exception as e:
        text = f"❌ Не вдалося завантажити премії: {e}"
    await msg.reply_text(text)
    await msg.reply_text("Виберіть опцію:", reply_markup=_NAV_KB)


# ──────────────────────────────────────────────────────────────────────────────
# Бонуси (повідомлення): вибір року → місяця → результат
# ──────────────────────────────────────────────────────────────────────────────
async def show_bonusmsg_years(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    years = get_available_years_bonuses(employee) if employee else []
    context.user_data["menu"] = "bonusmsg_years"
    msg = update.effective_message
    if not years:
        await msg.reply_text("ℹ️ Немає даних по бонусах.", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(y, callback_data=f"bonusmsg_year:{y}")] for y in years])
    await msg.reply_text("Оберіть рік (Бонуси):", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def show_bonusmsg_months(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    year = context.user_data.get("selected_year")
    months = get_available_months_bonuses(employee, year) if (employee and year) else []
    context.user_data["menu"] = "bonusmsg_months"
    msg = update.effective_message
    if not months:
        await msg.reply_text("ℹ️ Немає бонусів за обраний рік.", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(m, callback_data=f"bonusmsg_month:{m}")] for m in months])
    await msg.reply_text("Оберіть місяць (Бонуси):", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def send_bonuses_message(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    year = context.user_data.get("selected_year")
    month_name = context.user_data.get("selected_month")

    msg = update.effective_message

    if not (employee and year and month_name):
        await msg.reply_text("Помилка: спочатку оберіть рік та місяць.")
        return

    month_num = MONTHS_MAP.get(month_name)
    if month_num is None:
        await msg.reply_text("Невідомий місяць.")
        return

    try:
        text = build_bonus_message_for_period(employee, int(year), int(month_num))
    except Exception as e:
        text = f"❌ Не вдалося завантажити бонуси: {e}"
    await msg.reply_text(text)
    await msg.reply_text("Виберіть опцію:", reply_markup=_NAV_KB)


# ──────────────────────────────────────────────────────────────────────────────
# Відомість Бонуси (Excel)
# ──────────────────────────────────────────────────────────────────────────────
async def show_bonuses_years(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    years = get_available_years_bonuses(employee) if employee else []
    context.user_data["menu"] = "bonuses_years"
    msg = update.effective_message
    if not years:
        await msg.reply_text("ℹ️ Немає нарахувань бонусів.", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(y, callback_data=f"bonuses_year:{y}")] for y in years])
    await msg.reply_text("Оберіть рік (Відомість Бонуси):", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def show_bonuses_months(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    year = context.user_data.get("selected_year")
    months = get_available_months_bonuses(employee, year) if (employee and year) else []
    context.user_data["menu"] = "bonuses_months"
    msg = update.effective_message
    if not months:
        await msg.reply_text("ℹ️ Немає нарахувань бонусів за цей рік.", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(m, callback_data=f"bonuses_month:{m}")] for m in months])
    await msg.reply_text("Оберіть місяць (Відомість Бонуси):", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def send_bonuses_excel(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    nice = display_name(employee)
    year = context.user_data.get("selected_year")
    month = context.user_data.get("selected_month")

    msg = update.effective_message

    if not (employee and year and month):
        await msg.reply_text("Помилка: спочатку оберіть рік та місяць.")
        return

    month_num = MONTHS_MAP.get(month)
    if month_num is None:
        await msg.reply_text("Невідомий місяць.")
        return

    period_ym = f"{year}-{month_num:02d}"
    wait_msg = await msg.reply_text("⏳ Формую відомість бонусів…")

    xlsx_path = None
    try:
        xlsx_path = generate_excel(employee, period_ym)
        if not xlsx_path or not os.path.exists(xlsx_path):
            await msg.reply_text(f"ℹ️ У вас відсутні нарахування бонусів за {month} {year}.")
            return

        with open(xlsx_path, "rb") as f:
            await msg.reply_document(
                document=f,
                filename=os.path.basename(xlsx_path),
                caption=f"Відомість бонусів • {nice} • {period_ym}"
            )
    except ValueError:
        await msg.reply_text(f"ℹ️ У вас відсутні нарахування бонусів за {month} {year}.")
        return
    except Exception as e:
        logging.exception("Помилка генерації бонусів")
        await msg.reply_text(f"❌ Не вдалося сформувати файл: {e}")
        return
    finally:
        try:
            if xlsx_path:
                tmp_dir = os.path.dirname(xlsx_path)
                if os.path.isdir(tmp_dir):
                    shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        try:
            if wait_msg:
                await context.bot.delete_message(update.effective_chat.id, wait_msg.message_id)
        except Exception:
            pass

    await msg.reply_text("Виберіть опцію:", reply_markup=_NAV_KB)


# ──────────────────────────────────────────────────────────────────────────────
# Показ розрахункового листа ОКЛАД
# ──────────────────────────────────────────────────────────────────────────────
async def show_salary_years(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    years = get_available_years_salary(employee) if employee else []
    context.user_data["menu"] = "salary_years"
    msg = update.effective_message
    if not years:
        await msg.reply_text("ℹ️ Немає даних по окладу.", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(y, callback_data=f"salary_year:{y}")] for y in years])
    await msg.reply_text("Оберіть рік:", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def show_salary_months(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    year = context.user_data.get("selected_year")
    months = get_available_months_salary(employee, year) if (employee and year) else []
    context.user_data["menu"] = "salary_months"
    msg = update.effective_message
    if not months:
        await msg.reply_text("ℹ️ Немає місяців з даними за цей рік.", reply_markup=_NAV_KB)
        return
    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(m, callback_data=f"salary_month:{m}")] for m in months])
    await msg.reply_text("Оберіть місяць:", reply_markup=inline_kb)
    await msg.reply_text("​", reply_markup=_NAV_KB)


async def show_salary_details(update: Update, context: CallbackContext) -> None:
    employee = context.user_data.get("employee_name")
    nice = display_name(employee)
    year = context.user_data.get("selected_year")
    month_name = context.user_data.get("selected_month")

    msg = update.effective_message

    if not (employee and year and month_name):
        await msg.reply_text("Помилка: спочатку оберіть рік та місяць.")
        return

    month_num = MONTHS_MAP.get(month_name)
    if month_num is None:
        await msg.reply_text("Невідомий місяць.")
        return

    salary_rows = get_salary_data(employee, year, month_name)
    payments_rows = get_salary_payments(employee, year, month_name)
    bonus_rows = get_bonuses(employee, year, month_name)
    bonus_payments = get_bonus_payments(employee, year, month_name)
    prize_payments = get_prize_payments(employee, year, month_name)

    if not (salary_rows or payments_rows or bonus_rows or bonus_payments):
        await msg.reply_text("Немає даних для вибраного періоду.")
        return

    main_table, bonus_table, prize_table = format_salary_table(
        salary_rows, employee, int(year), month_num,
        payments_rows or [], bonus_rows or [], bonus_payments or [], prize_payments or []
    )

    main_msg = (
        heading("Оклад/KPI") +
        f"Співробітник: {nice}\n" +
        f"Період: {month_name} {year}\n\n" +
        code_block(main_table)
    )
    await _send_autodelete(update, context, main_msg)

    if bonus_rows or bonus_payments:
        if bonus_table and "Нарахування бонусів відсутні" not in bonus_table:
            bonus_msg = (
                heading("Бонуси") +
                f"Співробітник: {nice}\n" +
                f"Період: {month_name} {year}\n\n" +
                code_block(bonus_table)
            )
            await _send_autodelete(update, context, bonus_msg)

    has_prize_accruals = any(
        float(row.get("[Нараховано Премії UAH]", 0)) > 0 or float(row.get("[Нараховано Премії USD]", 0)) > 0
        for row in salary_rows or []
    )
    has_prize_payments = prize_payments and len(prize_payments) > 0

    if has_prize_accruals or has_prize_payments:
        if prize_table:
            prize_msg = (
                heading("Премії") +
                f"Співробітник: {nice}\n" +
                f"Період: {month_name} {year}\n\n" +
                code_block(prize_table)
            )
            await _send_autodelete(update, context, prize_msg)

    await msg.reply_text("Виберіть опцію:", reply_markup=_NAV_KB)


# ──────────────────────────────────────────────────────────────────────────────
#   Service helpers
# ──────────────────────────────────────────────────────────────────────────────
def heading(text: str) -> str:
    return f"*{text}*\n"


def code_block(content: str) -> str:
    return f"```\n{content}\n```"


async def _send_autodelete(update: Update, context: CallbackContext, message_text: str, *, delay: int = 60):
    msg = await update.effective_message.reply_text(message_text, parse_mode="Markdown")
    warn = await update.effective_message.reply_text("⚠️ Це повідомлення буде видалено через 60 секунд!")
    asyncio.create_task(_delete_later(context, update.effective_chat.id, [msg.message_id, warn.message_id], delay))


async def _delete_later(context: CallbackContext, chat_id: int, mids: list[int], delay: int):
    await asyncio.sleep(delay)
    for mid in mids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass

from __future__ import annotations

from io import BytesIO
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pytz

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackContext

from auth import get_power_bi_token
from utils.name_aliases import display_name
from .workdays_query import (
    _get_employee_periods_cached,
    _extract_year_month,
    _get_headers,
    _execute_dax,
    _to_int,
)

POWER_BI_URL = "https://api.powerbi.com/v1.0/myorg/datasets/8b80be15-7b31-49e4-bc85-8b37a0d98f1c/executeQueries"

MONTHS_UA_SHORT = ["Січ", "Лют", "Бер", "Кві", "Тра", "Чер",
                   "Лип", "Сер", "Вер", "Жов", "Лис", "Гру"]
MONTHS_UA_FULL = ["Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
                  "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"]

COLOR_VAC = "#3A86FF"
COLOR_SICK = "#EF233C"
COLOR_LWP = "#8D99AE"


def _fetch_yearly_data(employee_name: str, year: int) -> dict[int, dict]:
    headers = _get_headers()
    if not headers:
        return {}

    emp = employee_name.replace('"', '""')
    dax = f"""
        EVALUATE
        SELECTCOLUMNS(
            FILTER(
                workdays_by_employee,
                workdays_by_employee[TaxCode] IN
                    SELECTCOLUMNS(
                        FILTER(Employees, Employees[Employee] = "{emp}"),
                        "INN", Employees[INN]
                    ) &&
                YEAR(DATEVALUE(workdays_by_employee[Period])) = {year}
            ),
            "Period",              workdays_by_employee[Period],
            "RegularVacationDays", workdays_by_employee[RegularVacationDays],
            "SickLeaveDays",       workdays_by_employee[SickLeaveDays],
            "LeaveWithoutPay",     workdays_by_employee[LeaveWithoutPay],
            "VacationOnWeekends",  workdays_by_employee[VacationOnWeekends]
        )
    """

    rows = _execute_dax(headers, dax)
    result: dict[int, dict] = {}
    for row in rows:
        _, month = _extract_year_month(str(row.get("[Period]") or ""))
        if month is None:
            continue
        result[month] = {
            "vac":  _to_int(row.get("[RegularVacationDays]")),
            "sick": _to_int(row.get("[SickLeaveDays]")),
            "lwp":  _to_int(row.get("[LeaveWithoutPay]")),
            "vow":  _to_int(row.get("[VacationOnWeekends]")),
        }
    return result


def _generate_chart(months_data: dict[int, dict], employee_name: str, year: int) -> BytesIO:
    plt.rcParams["font.family"] = "DejaVu Sans"

    months_range = list(range(1, 13))
    labels = MONTHS_UA_SHORT
    vac_vals  = [months_data.get(m, {}).get("vac",  0) for m in months_range]
    sick_vals = [months_data.get(m, {}).get("sick", 0) for m in months_range]
    lwp_vals  = [months_data.get(m, {}).get("lwp",  0) for m in months_range]

    fig, ax = plt.subplots(figsize=(13, 7))
    fig.patch.set_facecolor("#F8F9FA")
    ax.set_facecolor("#FFFFFF")

    x = list(range(12))

    bars_vac  = ax.bar(x, vac_vals,  color=COLOR_VAC,  label="Звичайна відпустка", zorder=3)
    bars_sick = ax.bar(x, sick_vals, color=COLOR_SICK,  label="Лікарняні",
                       bottom=vac_vals, zorder=3)
    bars_lwp  = ax.bar(x, lwp_vals,  color=COLOR_LWP,  label="Відпустка за свій рахунок",
                       bottom=[v + s for v, s in zip(vac_vals, sick_vals)], zorder=3)

    # Підписи чисел на сегментах
    for i, bar in enumerate(bars_vac):
        if vac_vals[i] > 0:
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_y() + bar.get_height() / 2,
                    str(vac_vals[i]),
                    ha="center", va="center", fontsize=9, fontweight="bold", color="white", zorder=4)

    for i, bar in enumerate(bars_sick):
        if sick_vals[i] > 0:
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_y() + bar.get_height() / 2,
                    str(sick_vals[i]),
                    ha="center", va="center", fontsize=9, fontweight="bold", color="white", zorder=4)

    for i, bar in enumerate(bars_lwp):
        if lwp_vals[i] > 0:
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_y() + bar.get_height() / 2,
                    str(lwp_vals[i]),
                    ha="center", va="center", fontsize=9, fontweight="bold", color="white", zorder=4)

    # Підпис загальної суми над стовпцем
    for i in range(12):
        total = vac_vals[i] + sick_vals[i] + lwp_vals[i]
        if total > 0:
            ax.text(i, total + 0.15, str(total),
                    ha="center", va="bottom", fontsize=8.5, color="#333333", zorder=4)

    # Середнє по відсутностях (без нульових місяців)
    non_zero = [vac_vals[i] + sick_vals[i] + lwp_vals[i] for i in range(12)
                if vac_vals[i] + sick_vals[i] + lwp_vals[i] > 0]
    if len(non_zero) > 1:
        avg = sum(non_zero) / len(non_zero)
        ax.axhline(avg, color="#FF9F1C", linewidth=1.4, linestyle="--",
                   label=f"Середнє ({avg:.1f} дн.)", zorder=2)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel("Кількість днів", fontsize=10)
    ax.set_xlabel("Місяць", fontsize=10)

    max_val = max((vac_vals[i] + sick_vals[i] + lwp_vals[i] for i in range(12)), default=1)
    ax.set_ylim(0, max_val + max(2, max_val * 0.18))
    ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    nice_name = display_name(employee_name)
    ax.set_title(f"Відпустки та лікарняні  ·  {nice_name}  ·  {year} рік",
                 fontsize=13, fontweight="bold", pad=14)

    ax.legend(loc="upper right", fontsize=9,
              handles=[
                  mpatches.Patch(color=COLOR_VAC,  label="Звичайна відпустка"),
                  mpatches.Patch(color=COLOR_SICK, label="Лікарняні"),
                  mpatches.Patch(color=COLOR_LWP,  label="Відпустка за свій рахунок"),
              ] + ([ax.lines[0]] if non_zero and len(non_zero) > 1 else []))

    kyiv_tz = pytz.timezone("Europe/Kyiv")
    now_str = datetime.now(kyiv_tz).strftime("%Y-%m-%d %H:%M")
    fig.text(0.01, 0.99, f"Згенеровано ботом FTP | Дата формування: {now_str}",
             ha="left", va="top", fontsize=7.5, color="#999999")

    buf = BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight", dpi=130, facecolor=fig.get_facecolor())
    buf.seek(0)
    plt.close(fig)
    return buf


def _format_summary(months_data: dict[int, dict], employee_name: str, year: int) -> str:
    nice_name = display_name(employee_name)

    total_vac  = sum(d["vac"]  for d in months_data.values())
    total_sick = sum(d["sick"] for d in months_data.values())
    total_lwp  = sum(d["lwp"]  for d in months_data.values())

    lines = [
        f"📊 *Звіт за {year} рік*",
        f"👤 {nice_name}",
        "",
    ]

    # Відпустки
    if total_vac > 0 or total_lwp > 0:
        lines.append(f"🏖 *Відпустки — {total_vac + total_lwp} дн.*")
        if total_vac > 0:
            lines.append(f"  ├ Звичайна: {total_vac} дн.")
        if total_lwp > 0:
            lines.append(f"  └ За власний рахунок: {total_lwp} дн.")
        elif total_vac > 0:
            lines[-1] = lines[-1].replace("├", "└")
    else:
        lines.append("🏖 Відпусток не було")

    lines.append("")

    # Лікарняні
    if total_sick > 0:
        lines.append(f"🤒 *Лікарняні — {total_sick} дн.*")
    else:
        lines.append("🤒 Лікарняних не було")

    # Місяці з відпустками
    vac_months = [(m, months_data[m]["vac"] + months_data[m]["lwp"])
                  for m in sorted(months_data)
                  if months_data[m]["vac"] + months_data[m]["lwp"] > 0]
    if vac_months:
        lines.append("")
        lines.append("📅 *Місяці з відпустками:*")
        medals = ["🥇", "🥈", "🥉"]
        vac_months_sorted = sorted(vac_months, key=lambda x: -x[1])
        for idx, (m, days) in enumerate(vac_months_sorted):
            medal = medals[idx] if idx < 3 else "  "
            lines.append(f"  {medal} {MONTHS_UA_FULL[m - 1]} — {days} дн.")

    # Місяці з лікарняними
    sick_months = [(m, months_data[m]["sick"])
                   for m in sorted(months_data)
                   if months_data[m]["sick"] > 0]
    if sick_months:
        lines.append("")
        lines.append("🤒 *Місяці з лікарняними:*")
        for m, days in sick_months:
            lines.append(f"  {MONTHS_UA_FULL[m - 1]} — {days} дн.")

    # Підсумок
    lines.append("")
    if total_vac == 0 and total_sick == 0 and total_lwp == 0:
        lines.append("✅ Весь рік відпрацьовано без відсутностей")
    else:
        clean_months = 12 - len({m for m in months_data
                                  if months_data[m]["vac"] + months_data[m]["sick"] + months_data[m]["lwp"] > 0})
        if clean_months > 0:
            lines.append(f"✅ Решту {clean_months} міс. відпрацьовано без відсутностей")

    return "\n".join(lines)


async def show_vacation_sick_years(update: Update, context: CallbackContext) -> None:
    context.user_data["menu"] = "vsr_years"

    employee_name = context.user_data.get("employee_name")
    if not employee_name:
        await update.effective_message.reply_text("⚠️ Не знайдено працівника в контексті.")
        return

    await update.effective_message.reply_text("⏳ Завантаження даних...")

    periods = _get_employee_periods_cached(context, employee_name)
    years = sorted({y for (y, _) in (_extract_year_month(p) for p in periods) if y is not None})

    nav_kb = ReplyKeyboardMarkup([[KeyboardButton("Назад"), KeyboardButton("Головне меню")]], resize_keyboard=True, one_time_keyboard=True)

    if not years:
        await update.effective_message.reply_text("ℹ️ Дані про відпустки відсутні.", reply_markup=nav_kb)
        return

    inline_kb = InlineKeyboardMarkup([[InlineKeyboardButton(str(y), callback_data=f"vsr_year:{y}")] for y in years])
    await update.effective_message.reply_text("📅 Оберіть рік для звіту:", reply_markup=inline_kb)
    await update.effective_message.reply_text("або поверніться:", reply_markup=nav_kb)


async def show_vacation_sick_report(update: Update, context: CallbackContext) -> None:
    year_str = str(context.user_data.get("selected_year", ""))
    try:
        year = int(year_str)
    except ValueError:
        await update.effective_message.reply_text("⚠️ Невірний рік.")
        return

    context.user_data["vsr_selected_year"] = year
    context.user_data["menu"] = "vsr_report"

    employee_name = context.user_data.get("employee_name")
    if not employee_name:
        await update.effective_message.reply_text("⚠️ Не знайдено працівника в контексті.")
        return

    await update.effective_message.reply_text("⏳ Формую звіт, зачекайте...")

    months_data = _fetch_yearly_data(employee_name, year)

    nav_kb = ReplyKeyboardMarkup([[KeyboardButton("Назад"), KeyboardButton("Головне меню")]], one_time_keyboard=True, resize_keyboard=True)

    if not months_data or all(
        d["vac"] == 0 and d["sick"] == 0 and d["lwp"] == 0
        for d in months_data.values()
    ):
        await update.effective_message.reply_text(
            f"ℹ️ За {year} рік відсутні дані про відпустки та лікарняні.",
            reply_markup=nav_kb,
        )
        return

    chart_buf = _generate_chart(months_data, employee_name, year)
    summary = _format_summary(months_data, employee_name, year)

    await update.effective_message.reply_photo(photo=chart_buf)
    await update.effective_message.reply_text(summary, parse_mode="Markdown", reply_markup=nav_kb)

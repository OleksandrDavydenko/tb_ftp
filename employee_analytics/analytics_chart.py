from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CallbackContext
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from io import BytesIO
from .analytics_table import get_income_data, get_available_months_analytics, get_yearly_breakdown, MONTHS_UA
import logging
from datetime import datetime
import pytz

from utils.name_aliases import display_name

MONTHS_UA_SHORT = ["Січ","Лют","Бер","Кві","Тра","Чер","Лип","Сер","Вер","Жов","Лис","Гру"]

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для побудови річного графіка за обраним параметром
async def show_yearly_chart_for_parameter(update: Update, context: CallbackContext, employee_name: str, year: str, parameter: str):
    # Повідомлення користувачу про очікування
    await update.message.reply_text("Зачекайте, будь ласка. Це може зайняти деякий час...")
    nice_name = display_name(employee_name)

    # Місяці для отримання даних та побудови графіка
    months = get_available_months_analytics(employee_name, year) or []
    monthly_values = []
    if not months:
        custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
        reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text(f"Для {nice_name} немає даних за {year} рік.", reply_markup=reply_markup)
        return


    # Визначення параметра для отримання даних
    parameter_column = {
        "Дохід": "[Sum USD]",
        "Валовий прибуток": "[Gross Profit]",
        "Маржинальність": "[Margin Percentage]",
        "Кількість угод": "[Deal Count]"
    }.get(parameter)

    # Перевірка на випадок, якщо обраний параметр недоступний
    if not parameter_column:
        await update.message.reply_text("Обраний параметр не підтримується.")
        return

    # Отримання даних про обраний параметр за кожен місяць року
    for month in months:
        income_data = get_income_data(employee_name, "Менеджер", year, month) or get_income_data(employee_name, "Сейлс", year, month)
        
        # Розрахунок маржинальності, якщо вибрано цей параметр
        if parameter == "Маржинальність":
            income = income_data.get("[Sum USD]", 0) if income_data else 0
            gross_profit = income_data.get("[Gross Profit]", 0) if income_data else 0
            value = (gross_profit / income * 100) if income else 0
        else:
            value = income_data.get(parameter_column, 0) if income_data else 0
        
        monthly_values.append(value)

    # Додавання кнопок "Назад" та "Головне меню" для навігації
    custom_keyboard = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(custom_keyboard, one_time_keyboard=True, resize_keyboard=True)

    # Перевірка на випадок, якщо всі значення рівні нулю
    if all(value == 0 for value in monthly_values):
        await update.message.reply_text(f"Для {nice_name} немає інформації за {year} рік.", reply_markup=reply_markup)
        logging.info(f"Немає даних для графіка {parameter.lower()} для {employee_name} за {year} рік.")
        return

    # Побудова графіка з більшим розміром
    plt.figure(figsize=(12, 8))
    plt.plot(months, monthly_values, marker='o', label=parameter)

    # Додавання значень біля точок
    for i, value in enumerate(monthly_values):
        plt.annotate(f"{value:.2f}", (months[i], monthly_values[i]), textcoords="offset points", xytext=(0, 10), ha='center')

    plt.title(f"Аналітика {parameter.lower()} {nice_name} за {year} рік")
    plt.xlabel("Місяці")
    plt.ylabel(
        "Маржинальність (%)" if parameter == "Маржинальність"
        else (parameter if parameter == "Кількість угод" else f"{parameter} (USD)")
    )
    plt.xticks(rotation=45)
    plt.grid()
    plt.legend()

    # Додавання підпису з датою та часом формування в лівому верхньому куті (київський час)
    kyiv_timezone = pytz.timezone("Europe/Kyiv")
    current_datetime = datetime.now(kyiv_timezone).strftime("%Y-%m-%d %H:%M")
    plt.figtext(0.01, 0.98, f"Згенеровано ботом FTP | Дата формування: {current_datetime}", ha="left", fontsize=8, color="gray", va="top")

    # Збереження графіка як зображення
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()

    # Відправка графіка як зображення
    await update.message.reply_photo(photo=buffer)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)
    logging.info(f"Графік {parameter.lower()} для {employee_name} за {year} рік відображено.")


async def show_yearly_dashboard(update: Update, context: CallbackContext, employee_name: str, year: str):
    """Composite yearly dashboard: KPI row + bar/margin combo chart + insights."""
    await update.message.reply_text("Зачекайте, будь ласка. Це може зайняти деякий час...")
    nice_name = display_name(employee_name)

    months_data = get_yearly_breakdown(employee_name, year)

    kb = [[KeyboardButton("Назад"), KeyboardButton("Головне меню")]]
    reply_markup = ReplyKeyboardMarkup(kb, one_time_keyboard=True, resize_keyboard=True)

    if not months_data:
        await update.message.reply_text(
            f"Для {nice_name} немає даних за {year} рік.", reply_markup=reply_markup
        )
        return

    all_months = sorted(months_data.keys())
    labels   = [MONTHS_UA_SHORT[m - 1] for m in all_months]
    incomes  = [months_data[m]["income"]       for m in all_months]
    gps      = [months_data[m]["gross_profit"] for m in all_months]
    margins  = [(gp / inc * 100) if inc else 0 for gp, inc in zip(gps, incomes)]

    total_income = sum(incomes)
    total_gp     = sum(gps)
    avg_margin   = (total_gp / total_income * 100) if total_income else 0
    total_deals  = sum(months_data[m].get("deal_count", 0) for m in all_months)

    def fmt_k(v):
        return f"{v / 1000:.1f}K" if v >= 1000 else f"{v:.0f}"

    def fmt_usd(v):
        return f"{int(v):,}".replace(",", " ") + " $"

    fig = plt.figure(figsize=(13, 8), facecolor="#F8F9FA")
    gs  = gridspec.GridSpec(2, 1, figure=fig, height_ratios=[1, 5], hspace=0.12)

    # ── KPI row ──────────────────────────────────────────────────────────────
    ax_kpi = fig.add_subplot(gs[0])
    ax_kpi.set_facecolor("#F8F9FA")
    ax_kpi.axis("off")

    kpis = [
        (fmt_k(total_income) + " $", "Дохід"),
        (fmt_k(total_gp) + " $",     "Прибуток"),
        (f"{avg_margin:.1f}%",        "Маржа"),
        (str(total_deals),            "Угоди"),
    ]
    for i, (val, label) in enumerate(kpis):
        cx = 0.125 + i * 0.25
        ax_kpi.text(cx, 0.72, val,   ha="center", va="center", fontsize=17,
                    fontweight="bold", color="#212529", transform=ax_kpi.transAxes)
        ax_kpi.text(cx, 0.18, label, ha="center", va="center", fontsize=10,
                    color="#6C757D",  transform=ax_kpi.transAxes)
        if i > 0:
            line = plt.Line2D([0.25 * i, 0.25 * i], [0.05, 0.95],
                              transform=ax_kpi.transAxes, color="#DEE2E6", linewidth=1)
            ax_kpi.add_line(line)

    # ── Combo chart ──────────────────────────────────────────────────────────
    ax_bar = fig.add_subplot(gs[1])
    ax_bar.set_facecolor("#FFFFFF")

    x       = np.arange(len(labels))
    max_inc = max(incomes) if incomes else 1
    colors  = ["#06A77D" if inc == max_inc else "#3A86FF" for inc in incomes]
    bars    = ax_bar.bar(x, incomes, color=colors, alpha=0.85, width=0.55)

    for bar, val in zip(bars, incomes):
        ax_bar.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_inc * 0.012,
            fmt_k(val),
            ha="center", va="bottom", fontsize=8, color="#495057"
        )

    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(labels, fontsize=10)
    ax_bar.set_ylabel("Дохід (USD)", fontsize=10, color="#3A86FF")
    ax_bar.tick_params(axis="y", labelcolor="#3A86FF")
    ax_bar.spines["top"].set_visible(False)
    ax_bar.spines["right"].set_visible(False)
    ax_bar.grid(axis="y", alpha=0.3, linestyle="--", color="#CED4DA")

    ax_mg = ax_bar.twinx()
    ax_mg.plot(x, margins, color="#FF9F1C", marker="o", linewidth=2.5, markersize=6, zorder=5)
    for xi, mg in zip(x, margins):
        ax_mg.annotate(f"{mg:.0f}%", (xi, mg),
                       textcoords="offset points", xytext=(0, 7),
                       ha="center", fontsize=7, color="#FF9F1C")
    ax_mg.set_ylabel("Маржа (%)", fontsize=10, color="#FF9F1C")
    ax_mg.tick_params(axis="y", labelcolor="#FF9F1C")
    ax_mg.spines["top"].set_visible(False)
    ax_mg.spines["left"].set_visible(False)

    from matplotlib.lines import Line2D
    ax_bar.legend(
        handles=[
            plt.Rectangle((0, 0), 1, 1, color="#3A86FF", alpha=0.85, label="Дохід"),
            Line2D([0], [0], color="#FF9F1C", linewidth=2, marker="o", label="Маржа"),
        ],
        loc="upper left", fontsize=9, framealpha=0.7
    )

    # ── Insights text ─────────────────────────────────────────────────────────
    if len(incomes) > 1:
        best_i  = incomes.index(max(incomes))
        worst_i = incomes.index(min(incomes))
        avg_inc = total_income / len(incomes)
        best_pct = max(incomes) / total_income * 100 if total_income else 0
        insights = (
            f"Топ: {MONTHS_UA[all_months[best_i]-1]} — {fmt_usd(max(incomes))} ({best_pct:.0f}% за рік)   "
            f"Слабкий: {MONTHS_UA[all_months[worst_i]-1]} — {fmt_usd(min(incomes))}   "
            f"Середній: {fmt_usd(avg_inc)}"
        )
        fig.text(0.5, 0.005, insights, ha="center", fontsize=9, color="#495057")

    # ── Title + timestamp ─────────────────────────────────────────────────────
    kyiv_tz = pytz.timezone("Europe/Kyiv")
    ts = datetime.now(kyiv_tz).strftime("%Y-%m-%d %H:%M")
    fig.suptitle(f"Аналітика · {nice_name} · {year} рік",
                 fontsize=13, fontweight="bold", color="#212529", y=0.995)
    fig.text(0.01, 0.995, f"Згенеровано ботом FTP | {ts}",
             ha="left", fontsize=7, color="gray", va="top")

    buffer = BytesIO()
    plt.savefig(buffer, format="png", dpi=130, bbox_inches="tight", facecolor="#F8F9FA")
    buffer.seek(0)
    plt.close()

    context.user_data["menu"] = "analytics_yearly_dashboard"

    await update.message.reply_photo(photo=buffer)
    await update.message.reply_text("Виберіть опцію:", reply_markup=reply_markup)
    logging.info(f"Yearly dashboard для {employee_name} за {year} рік відображено.")


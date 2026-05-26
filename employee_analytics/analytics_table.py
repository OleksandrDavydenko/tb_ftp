import requests
import logging
from auth import get_power_bi_token
from utils.name_aliases import display_name


# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для отримання даних про дохід, валовий прибуток та кількість угод для конкретного співробітника за обраний місяць та рік
def get_income_data(employee_name, role, year, month):
    logging.info(f"Запит на отримання даних для: {employee_name}, роль: {role}, рік: {year}, місяць: {month}")
    token = get_power_bi_token()
    if not token:
        logging.error("Не вдалося отримати токен Power BI.")
        return None

    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    power_bi_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Визначення колонки для фільтрування за роллю
    role_column = "Manager" if role == "Менеджер" else "Seller"
    formatted_date = f"{month.lower()} {year} р."

    # Запит з фільтрацією за користувачем, для обчислення доходу, валового прибутку, бонусів та кількості угод
    query_data = {
        "queries": [
            {
                "query": f"""
                    EVALUATE 
                    SUMMARIZECOLUMNS(
                        'GrossProfitFromDeals'[{role_column}],
                        FILTER(
                            'GrossProfitFromDeals',
                            'GrossProfitFromDeals'[{role_column}] = "{employee_name}" &&
                            FORMAT('GrossProfitFromDeals'[RegistrDate], "MMMM yyyy р.") = "{formatted_date}"
                        ),
                        "Sum USD", SUM('GrossProfitFromDeals'[Income]),
                        "Gross Profit", SUM('GrossProfitFromDeals'[GrossProfit]),
                        "Bonuses", SUM('GrossProfitFromDeals'[Bonuses]),
                        "Deal Count", COUNTROWS(SUMMARIZE('GrossProfitFromDeals', 'GrossProfitFromDeals'[DealNumber]))
                    )
                """
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    logging.info(f"Виконуємо запит до Power BI для {role} {employee_name} за {formatted_date}.")
    response = requests.post(power_bi_url, headers=headers, json=query_data)

    if response.status_code == 200:
        logging.info(f"Запит до Power BI для {role} {employee_name} успішний.")
        data = response.json()
        logging.info(f"Повна відповідь від Power BI: {data}")
        rows = data['results'][0]['tables'][0].get('rows', [])
        logging.info(f"Отримано {len(rows)} рядків для {role} {employee_name}.")
        return rows[0] if rows else None
    else:
        logging.error(f"Помилка при виконанні запиту: {response.status_code}, {response.text}")
        return None

# Функція для форматування таблиці аналітики для одного співробітника
def format_analytics_table(income_data, employee_name, month, year):
    nice_name = display_name(employee_name)
    # Форматування заголовка таблиці
    formatted_date = f"{month.lower()} {year} р."
    table = f"Аналітика {nice_name} за {formatted_date}:\n"
    table += "-" * 30 + "\n"
    table += f"{'Показник':<20}{'Сума USD':<10}\n"
    table += "-" * 30 + "\n"

    # Отримання значень з правильними ключами
    total_income = income_data.get("[Sum USD]", 0) if income_data else 0
    gross_profit = income_data.get("[Gross Profit]", 0) if income_data else 0
    bonuses = income_data.get("[Bonuses]", 0) if income_data else 0
    deal_count = income_data.get("[Deal Count]", 0) if income_data else 0

    # Розрахунок валового прибутку з урахуванням бонусів
    total_gross_profit = gross_profit #+ bonuses
    # Розрахунок маржинальності
    margin = (total_gross_profit / total_income * 100) if total_income else 0

    table += f"{'Загальний дохід':<20}{total_income:<10}\n"
    table += f"{'Валовий прибуток':<20}{total_gross_profit:<10}\n"
    table += f"{'Кількість угод':<20}{deal_count:<10}\n"
    table += f"{'Маржинальність':<20}{margin:.2f}%\n"
    table += "-" * 30 + "\n"
    
    logging.info("Формування таблиці аналітики завершено.")
    return table






# ──────────────────────────────────────────────────────────────────────────────
# КОРОТКІ ЗАПИТИ ДЛЯ СПИСКІВ РОКІВ/МІСЯЦІВ (АНАЛІТИКА)
# ──────────────────────────────────────────────────────────────────────────────

MONTHS_UA = ["Січень","Лютий","Березень","Квітень","Травень","Червень",
             "Липень","Серпень","Вересень","Жовтень","Листопад","Грудень"]
MIN_YEAR = 2025  # мінімальний рік у виборі

def _pbi_exec_analytics(dax: str):
    token = get_power_bi_token()
    if not token:
        logging.error("PBI token missing")
        return []
    dataset_id = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
    url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries'
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code != 200:
        logging.error(f"PBI {r.status_code}: {r.text}")
        return []
    return r.json()['results'][0]['tables'][0].get('rows', []) or []

def get_available_years_analytics(employee_name: str) -> list[str]:
    emp = employee_name.replace('"', '""')
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(
    FILTER('GrossProfitFromDeals', 'GrossProfitFromDeals'[Manager] = "{emp}" && YEAR('GrossProfitFromDeals'[RegistrDate]) >= {MIN_YEAR}),
    "Y", YEAR('GrossProfitFromDeals'[RegistrDate])
  ),
  SELECTCOLUMNS(
    FILTER('GrossProfitFromDeals', 'GrossProfitFromDeals'[Seller]  = "{emp}" && YEAR('GrossProfitFromDeals'[RegistrDate]) >= {MIN_YEAR}),
    "Y", YEAR('GrossProfitFromDeals'[RegistrDate])
  )
)
"""
    rows = _pbi_exec_analytics(dax)
    years = sorted({int(r.get("[Y]", 0)) for r in rows if r.get("[Y]")})
    return [str(y) for y in years if y >= MIN_YEAR]

def get_available_months_analytics(employee_name: str, year: str) -> list[str]:
    emp = employee_name.replace('"', '""')
    dax = f"""
EVALUATE
UNION(
  SELECTCOLUMNS(
    FILTER('GrossProfitFromDeals',
      'GrossProfitFromDeals'[Manager] = "{emp}" &&
      YEAR('GrossProfitFromDeals'[RegistrDate]) = {int(year)}
    ),
    "M", MONTH('GrossProfitFromDeals'[RegistrDate])
  ),
  SELECTCOLUMNS(
    FILTER('GrossProfitFromDeals',
      'GrossProfitFromDeals'[Seller] = "{emp}" &&
      YEAR('GrossProfitFromDeals'[RegistrDate]) = {int(year)}
    ),
    "M", MONTH('GrossProfitFromDeals'[RegistrDate])
  )
)
"""
    rows = _pbi_exec_analytics(dax)
    mm = sorted({int(r.get("[M]", 0)) for r in rows if r.get("[M]") and 1 <= int(r.get("[M]", 0)) <= 12})
    return [MONTHS_UA[i-1] for i in mm]


def get_yearly_breakdown(employee_name: str, year: str) -> dict:
    """One DAX call returning all months' income/profit/bonuses/deal_count for the year."""
    emp = employee_name.replace('"', '""')
    dax = f"""
EVALUATE
GROUPBY(
    SELECTCOLUMNS(
        FILTER(
            'GrossProfitFromDeals',
            ('GrossProfitFromDeals'[Manager] = "{emp}" || 'GrossProfitFromDeals'[Seller] = "{emp}") &&
            YEAR('GrossProfitFromDeals'[RegistrDate]) = {int(year)}
        ),
        "Month",      MONTH('GrossProfitFromDeals'[RegistrDate]),
        "Income",     'GrossProfitFromDeals'[Income],
        "GrossProfit",'GrossProfitFromDeals'[GrossProfit],
        "Bonuses",    'GrossProfitFromDeals'[Bonuses],
        "DealNo",     'GrossProfitFromDeals'[DealNumber]
    ),
    [Month],
    "Sum USD",     SUMX(CURRENTGROUP(), [Income]),
    "Gross Profit",SUMX(CURRENTGROUP(), [GrossProfit]),
    "Bonuses",     SUMX(CURRENTGROUP(), [Bonuses]),
    "Deal Count",  COUNTX(CURRENTGROUP(), [DealNo])
)
"""
    rows = _pbi_exec_analytics(dax)
    result = {}
    for r in rows:
        m = int(r.get("[Month]", 0) or 0)
        if 1 <= m <= 12:
            result[m] = {
                "income":      float(r.get("[Sum USD]",     0) or 0),
                "gross_profit":float(r.get("[Gross Profit]",0) or 0),
                "bonuses":     float(r.get("[Bonuses]",     0) or 0),
                "deal_count":  int(r.get("[Deal Count]",   0) or 0),
            }
    return result


def format_smart_monthly_card(current: dict, previous, ytd_months: dict,
                               employee_name: str, month: str, year: str) -> str:
    nice_name = display_name(employee_name)

    income  = float(current.get("[Sum USD]",     0) or 0) if current else 0
    gp      = float(current.get("[Gross Profit]",0) or 0) if current else 0
    bonuses = float(current.get("[Bonuses]",     0) or 0) if current else 0
    deals   = int(current.get("[Deal Count]",   0) or 0) if current else 0
    margin  = (gp / income * 100) if income else None
    avg_deal = (income / deals) if deals else 0

    prev_income = float(previous.get("[Sum USD]",     0) or 0) if previous else None
    prev_gp     = float(previous.get("[Gross Profit]",0) or 0) if previous else None
    prev_margin = (prev_gp / prev_income * 100) if (prev_income and prev_gp is not None) else None

    def fmt(v):
        return f"{int(v):,}".replace(",", " ") + " $"

    def pct_delta(curr, prev_val):
        if prev_val is None or prev_val == 0:
            return None
        d = (curr - prev_val) / prev_val * 100
        sign = "↑" if d >= 0 else "↓"
        return f"  {sign} {abs(d):.0f}% від попереднього"

    # YTD aggregates
    ytd_income = sum(v["income"]       for v in ytd_months.values())
    ytd_gp     = sum(v["gross_profit"] for v in ytd_months.values())
    ytd_deals  = sum(v.get("deal_count", 0) for v in ytd_months.values())
    n_months   = len(ytd_months)
    avg_margin_ytd = (ytd_gp / ytd_income * 100) if ytd_income else 0

    # Ranking by income within the year
    month_num = (MONTHS_UA.index(month) + 1) if month in MONTHS_UA else None
    ranked    = sorted(ytd_months.keys(), key=lambda m: ytd_months[m]["income"], reverse=True)
    rank      = (ranked.index(month_num) + 1) if (month_num in ranked) else None

    lines = [f"📊 {month} {year} · {nice_name}", ""]

    lines.append(f"💵 Дохід:           {fmt(income)}")
    d = pct_delta(income, prev_income)
    if d:
        lines.append(d)

    lines += ["", f"🏆 Валовий прибуток: {fmt(gp)}"]
    d = pct_delta(gp, prev_gp)
    if d:
        lines.append(d)

    lines.append("")
    if margin is not None:
        lines.append(f"🎯 Маржинальність:   {margin:.1f}%")
        if prev_margin is not None:
            diff = margin - prev_margin
            sign = "↑" if diff >= 0 else "↓"
            lines.append(f"  {sign} {abs(diff):.1f} п.п. від попереднього")
    else:
        lines.append("🎯 Маржинальність:   —")

    lines.append("")
    avg_str = fmt(avg_deal) if avg_deal else "—"
    lines.append(f"🤝 Угод: {deals}   Середній чек: {avg_str}")

    lines += [
        "",
        "─" * 28,
        f"📈 З початку {year} року:",
        f"   Дохід:    {fmt(ytd_income)}",
        f"   Прибуток: {fmt(ytd_gp)}",
    ]
    if ytd_deals:
        lines.append(f"   Угод:     {ytd_deals}")
    lines.append(f"   Маржа:    {avg_margin_ytd:.1f}%")

    if n_months >= 2 and rank is not None:
        lines.append("")
        if rank == 1:
            lines.append("✨ Найкращий місяць за рік!")
        elif rank == 2:
            lines.append("✨ 2-й найкращий місяць за рік")
        elif rank == 3:
            lines.append("✨ 3-й найкращий місяць за рік")
        elif rank == n_months:
            lines.append(f"📉 Найслабший місяць за {year} р.")

    return "\n".join(lines)

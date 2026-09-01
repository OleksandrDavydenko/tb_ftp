# -*- coding: utf-8 -*-
import pandas as pd
import requests
from datetime import datetime
from auth import get_power_bi_token

from utils.name_aliases import display_name

# ==== Налаштування Power BI ====
DATASET_ID = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
PBI_URL = f'https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries'

# ---- Службові мапінги ----
UA_MONTHS = {
    1:"Січень",2:"Лютий",3:"Березень",4:"Квітень",5:"Травень",6:"Червень",
    7:"Липень",8:"Серпень",9:"Вересень",10:"Жовтень",11:"Листопад",12:"Грудень"
}
def ua_month_name(n: int) -> str: 
    return UA_MONTHS.get(int(n), f"{int(n):02d}")

def fmt_num(x: float) -> str:
    try:
        xv = float(x)
    except Exception:
        return "0"
    return str(int(xv)) if xv.is_integer() else f"{xv:.2f}"

# ---- Виконання DAX ----
def _exec_dax(token: str, dax: str) -> dict:
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    r = requests.post(PBI_URL, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def _to_dataframe(result_json: dict) -> pd.DataFrame:
    results = result_json.get("results", [])
    tables  = results[0].get("tables", []) if results else []
    if not tables:
        return pd.DataFrame()
    table = tables[0]
    cols  = [c.get("name") for c in table.get("columns", [])] if table.get("columns") else []
    rows  = table.get("rows", []) or []
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
        else:
            out.append({cols[i]: row[i] for i in range(len(cols))})

    def clean(k: str) -> str:
        return k.split("[", 1)[-1].rstrip("]") if "[" in k else k

    return pd.DataFrame([{clean(k): v for k, v in r.items()} for r in out])

# ---- Запит таблиці '3330' з фільтром Subconto2Period ----
def fetch_3330(employee: str, period_date: datetime) -> pd.DataFrame:
    token = get_power_bi_token()
    if not token:
        return pd.DataFrame()

    emp_escaped = employee.replace('"', '""')
    y, m, d = period_date.year, period_date.month, period_date.day
    dax = f"""
EVALUATE
FILTER(
    '3330/3320',
    '3330/3320'[Subconto1Emp] = "{emp_escaped}"
    && '3330/3320'[Subconto2Period] = DATE({y},{m},{d})
    && '3330/3320'[AccountCode] = 3330
)
"""
    return _to_dataframe(_exec_dax(token, dax))

# ---- Формування повідомлення ----
def build_bonus_message(df: pd.DataFrame, employee: str, period_date: datetime) -> str:
    if df.empty:
        return f"Для {display_name(employee)} за {period_date:%m.%Y} даних не знайдено."

    # типи
    for col in ["RegistrDate", "Subconto2Period"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df["AmountDt"] = pd.to_numeric(df.get("AmountDt", 0), errors="coerce").fillna(0.0)
    df["AmountCt"] = pd.to_numeric(df.get("AmountCt", 0), errors="coerce").fillna(0.0)
    df["DocumentNumber"] = df.get("DocumentNumber", "").astype(str).str.strip()

    # ===== Нарахування =====
    acc = df[df["AmountCt"] != 0].copy()
    acc_dt = acc["RegistrDate"].fillna(acc["Subconto2Period"])
    acc["Y"] = acc_dt.dt.year
    acc["M"] = acc_dt.dt.month
    acc["Label"] = acc.apply(lambda r: f"{ua_month_name(r['M'])} {int(r['Y'])}", axis=1)
    acc["Doc"] = acc["DocumentNumber"]

    accr_group = (
        acc.groupby(["Y", "M", "Label", "Doc"], dropna=False)["AmountCt"]
           .sum().round(2).reset_index(name="Sum")
           .sort_values(["Y", "M", "Doc"], kind="stable")
    )
    total_accrual = float(accr_group["Sum"].sum()) if not accr_group.empty else 0.0

    # ===== Виплати =====
    pay = df[df["AmountDt"] != 0].copy()
    pay["DateDT"] = pay["RegistrDate"]
    pay["Date"]   = pay["DateDT"].dt.strftime("%d.%m.%Y")
    pay["Doc"]    = pay["DocumentNumber"]

    pay_group = (
        pay.groupby(["DateDT","Date","Doc"], dropna=False)["AmountDt"]
           .sum().round(2).reset_index(name="Sum")
           .sort_values(["DateDT","Doc"], kind="stable")
    )
    total_paid = float(pay_group["Sum"].sum()) if not pay_group.empty else 0.0

    unpaid = round(total_accrual - total_paid, 2)

    # Заголовок
    title_month = ua_month_name(period_date.month)
    title_year  = period_date.year

    year = period_date.year
    month = period_date.month

    lines = []
    nice_name = display_name(employee)
    lines.append(f"📊 Бонуси за {title_month} {title_year} — {nice_name}.")
    lines.append("")

    # --- Нарахування ---
    lines.append("📝 Нарахування:")
    if accr_group.empty:
        lines.append("• (немає даних)")
    else:
        # головний період
        main_rows = accr_group[(accr_group["Y"] == year) & (accr_group["M"] == month)]
        corr_rows = accr_group[~((accr_group["Y"] == year) & (accr_group["M"] == month))]

        for _, r in main_rows.iterrows():
            lines.append(f"• {r['Label']} — Док. {r['Doc']} → {fmt_num(r['Sum'])}")

        if not corr_rows.empty:
            lines.append("")
            lines.append("🔄 Коригування:")
            for _, r in corr_rows.iterrows():
                lines.append(f"• {r['Label']} — Док. {r['Doc']} → {fmt_num(r['Sum'])}")

    lines.append(f"✅ Всього нараховано: {fmt_num(total_accrual)}")
    lines.append("")

    # --- Виплати ---
    lines.append("💵 Виплата бонусів по закритій дебіторці:")
    if pay_group.empty:
        lines.append("• (виплат не було)")
    else:
        for _, r in pay_group.iterrows():
            lines.append(f"• {r['Date']} — Док. {r['Doc']} → {fmt_num(r['Sum'])}")
    lines.append(f"🥇 Всього виплачено: {fmt_num(total_paid)}")
    lines.append("")
    lines.append(f"📌 Невиплачений залишок: {fmt_num(unpaid)}")

    return "\n".join(lines)


# ---- Головна точка виклику з бота ----
def build_bonus_message_for_period(employee: str, year: int, month: int) -> str:
    """Будує текст повідомлення для співробітника за вказаний рік-місяць (Subconto2Period = перше число місяця)."""
    period_date = datetime(year, month, 1)
    df = fetch_3330(employee, period_date)
    return build_bonus_message(df, employee, period_date)

# -*- coding: utf-8 -*-
import os, re, math, tempfile
import pandas as pd
import requests

from auth import get_power_bi_token  # вже є в проєкті

DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")

def _exec_dax(token: str, dax: str) -> dict:
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"queries":[{"query":dax}], "serializerSettings":{"includeNulls": True}}
    )
    r.raise_for_status()
    return r.json()

def _query_bonus_hod_by_period(token: str, hod: str, period_ym: str) -> dict:
    h = hod.replace('"','""'); p = period_ym.replace('"','""')
    dax = f"""
EVALUATE
VAR T =
    ADDCOLUMNS(
        BonusHeadOfDepartment,
        "PeriodYM", FORMAT(BonusHeadOfDepartment[Period], "yyyy-MM")
    )
RETURN
FILTER(T, [HeadOfDepartment] = "{h}" && [PeriodYM] = "{p}")
"""
    return _exec_dax(token, dax)

def _to_dataframe(res: dict) -> pd.DataFrame:
    t = (res.get("results") or [{}])[0].get("tables") or []
    if not t: return pd.DataFrame()
    cols = [c.get("name") for c in t[0].get("columns",[])]
    rows = t[0].get("rows") or []
    out=[]
    for r in rows:
        if isinstance(r, dict): out.append(r)
        else: out.append({cols[i]: r[i] for i in range(len(cols))})
    cleaned=[]
    for r in out:
        nr={}
        for k,v in r.items():
            if k.startswith("BonusHeadOfDepartment[") and k.endswith("]"):
                k = k[len("BonusHeadOfDepartment["):-1]
            nr[k.strip("[]")] = v
        cleaned.append(nr)
    return pd.DataFrame(cleaned)

_COLS_MAP = {
    "Period":"Period","DocNumber":"DocNumber","UgodaNumber":"UgodaNumber","PremiaSum":"PremiaSum",
    "HeadOfDepartment":"HeadOfDepartment","Client":"Client","Profit":"Profit","ShareOfProfits":"ShareOfProfits",
    "toPay":"toPay","PercentToPay":"PercentToPay","PartOfPlan":"PartOfPlan","LosingTrade":"LosingTrade",
    "BaseToCount":"BaseToCount","ForChief":"ForChief","DealPeriod":"DealPeriod","KindOfPeriod":"KindOfPeriod",
}
_NUM_COLS = ["Profit","ShareOfProfits","toPay","PercentToPay","PartOfPlan","LosingTrade","BaseToCount","ForChief","PremiaSum"]

def _to_xlsx_value(v):
    try:
        if pd.isna(v): return None
    except Exception: pass
    if isinstance(v, pd.Timestamp): return v.to_pydatetime()
    return v

def _build_sheet_for_head(wb, writer, df: pd.DataFrame, head: str, period_ym: str, sheet_name="Звіт"):
    import math  # локально, щоб уникнути конфліктів
    ws = wb.add_worksheet(sheet_name[:31])
    writer.sheets[sheet_name[:31]] = ws

    title_fmt   = wb.add_format({"bold": True, "font_size": 12})
    header_fmt  = wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border": 1,
                                 "align": "center", "valign": "vcenter", "text_wrap": True})
    cell_fmt    = wb.add_format({"border": 1})
    bold_fmt    = wb.add_format({"bold": True, "border": 1})
    highlight   = wb.add_format({"bold": True, "border": 1, "bg_color": "#FFF2CC"})

    def xwrite(r, c, v, fmt=None):
        v = _to_xlsx_value(v)
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): v = None
        ws.write(r, c, v, fmt or cell_fmt)

    cur_mask = df["KindOfPeriod"].astype(str).str.lower().eq("current period")
    prev_df  = df[~cur_mask].copy()
    curr_df  = df[cur_mask].copy()

    accrued_current = pd.to_numeric(curr_df.get("ForChief"), errors="coerce").fillna(0).sum()
    prev_to_pay     = pd.to_numeric(prev_df.get("toPay"), errors="coerce").fillna(0).sum()
    curr_to_pay     = pd.to_numeric(curr_df.get("toPay"), errors="coerce").fillna(0).sum()
    total_to_pay    = prev_to_pay + curr_to_pay
    balance         = accrued_current - curr_to_pay

    ws.merge_range(0, 0, 0, 7, f"Керівник: {head}  •  Період звіту (по Period): {period_ym}", title_fmt)

    headers = ["N","Менеджер","Нараховано в поточному періоді",
               "До виплати за минулі","До виплати за поточний",
               "До виплати в поточному","Залишок"]
    ws.write_row(1, 0, headers, header_fmt); ws.set_row(1, 28)

    xwrite(2, 0, 1); xwrite(2, 1, head)
    xwrite(2, 2, round(accrued_current, 2))
    xwrite(2, 3, round(prev_to_pay, 2))
    xwrite(2, 4, round(curr_to_pay, 2))
    xwrite(2, 5, round(total_to_pay, 2), highlight)
    xwrite(2, 6, round(balance, 2))
    ws.set_column(0, 1, 18); ws.set_column(2, 6, 22)

    row = 4
    def section(sec_df: pd.DataFrame, title: str):
        nonlocal row
        ws.write(row, 0, title, title_fmt); row += 1
        cols = ["N","Менеджер","Клієнт","Угода","Період",
                "Прибуток","До виплати","Відсоток оплати",
                "Частка від загального прибутку","Частина плану",
                "Розподіл збиткової угоди","База розрахунку","Фонд премії","Керівнику"]
        ws.write_row(row, 0, cols, header_fmt); ws.set_row(row, 28); row += 1

        table = pd.DataFrame({
            "Менеджер": head,
            "Клієнт": sec_df.get("Client"),
            "Угода": sec_df.get("UgodaNumber"),
            "Період": sec_df.get("DealPeriod"),
            "Прибуток": pd.to_numeric(sec_df.get("Profit"), errors="coerce"),
            "До виплати": pd.to_numeric(sec_df.get("toPay"), errors="coerce"),
            "Відсоток оплати": pd.to_numeric(sec_df.get("PercentToPay"), errors="coerce"),
            "Частка від загального прибутку": pd.to_numeric(sec_df.get("ShareOfProfits"), errors="coerce"),
            "Частина плану": pd.to_numeric(sec_df.get("PartOfPlan"), errors="coerce"),
            "Розподіл збиткової угоди": pd.to_numeric(sec_df.get("LosingTrade"), errors="coerce"),
            "База розрахунку": pd.to_numeric(sec_df.get("BaseToCount"), errors="coerce"),
            "Фонд премії": pd.to_numeric(sec_df.get("ForChief"), errors="coerce"),
            "Керівнику": pd.to_numeric(sec_df.get("ForChief"), errors="coerce"),
        })
        table.insert(0, "N", range(1, len(table) + 1))

        for i in range(len(table)):
            for j, c in enumerate(cols):
                xwrite(row + i, j, table.iloc[i][c])
        row += len(table)

        sums = table[["Прибуток","До виплати","База розрахунку","Фонд премії","Керівнику"]].apply(
            pd.to_numeric, errors="coerce").fillna(0).sum().round(2)
        ws.write_row(row, 0, ["", "Разом:"], bold_fmt)
        xwrite(row, cols.index("Прибуток"),        float(sums.get("Прибуток", 0)),        bold_fmt)
        xwrite(row, cols.index("До виплати"),      float(sums.get("До виплати", 0)),      bold_fmt)
        xwrite(row, cols.index("База розрахунку"), float(sums.get("База розрахунку", 0)), bold_fmt)
        xwrite(row, cols.index("Фонд премії"),     float(sums.get("Фонд премії", 0)),     bold_fmt)
        xwrite(row, cols.index("Керівнику"),       float(sums.get("Керівнику", 0)),       bold_fmt)
        row += 2

    section(prev_df.copy(), "Оплати за минулі періоди")
    section(curr_df.copy(), "Угоди поточного періоду")

    ws.set_column(0, 0, 4)
    ws.set_column(1, 1, 20)
    ws.set_column(2, 2, 26)
    ws.set_column(3, 3, 18)
    ws.set_column(4, 13, 16)

def generate_hod_excel(head_of_dept: str, period_ym: str) -> str:
    """
    Повертає шлях до згенерованого XLSX (у тимчасовій папці).
    Якщо даних немає — кидає ValueError.
    """
    token = get_power_bi_token()
    if not token:
        raise RuntimeError("Не вдалося отримати токен Power BI.")

    df = _to_dataframe(_query_bonus_hod_by_period(token, head_of_dept, period_ym))
    if df.empty:
        raise ValueError("Немає даних за обраними фільтрами.")

    df = df.rename(columns=_COLS_MAP).copy()
    for c in _NUM_COLS:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")

    safe = re.sub(r"[^A-Za-z0-9_\-]+","_", head_of_dept).strip("_")
    tmpdir = tempfile.mkdtemp(prefix="hod_report_")
    path = os.path.join(tmpdir, f"BonusHoD_{safe}_{period_ym}.xlsx")

    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        wb = writer.book
        _build_sheet_for_head(wb, writer, df, head_of_dept, period_ym, "Звіт")

    return path

# salary/bonuses_report.py
# -*- coding: utf-8 -*-
import os
import re
import math
import pandas as pd
import requests
import tempfile
from datetime import datetime
from auth import get_power_bi_token

DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")

def query_bonuses_details(token: str, employee: str, period_ym: str) -> dict:
    emp_escaped = employee.replace('"', '""')
    dax = f"""
EVALUATE
FILTER(
    ADDCOLUMNS(
        BonusesDetails,
        "PeriodYM", FORMAT(BonusesDetails[Period], "yyyy-MM")
    ),
    BonusesDetails[Employee] = "{emp_escaped}" && [PeriodYM] = "{period_ym}"
)
"""
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    r = requests.post(url, headers=headers, json=payload)
    r.raise_for_status()
    return r.json()

def to_dataframe(result_json: dict) -> pd.DataFrame:
    results = result_json.get("results", [])
    tables  = results[0].get("tables", []) if results else []
    if not tables:
        return pd.DataFrame()

    table = tables[0]
    cols  = [c.get("name") for c in table.get("columns", [])] if table.get("columns") else []
    rows  = table.get("rows", [])

    records = []
    for row in rows:
        if isinstance(row, dict):
            records.append(row)
        elif isinstance(row, list):
            if cols:
                records.append({cols[i]: row[i] for i in range(len(row))})
            else:
                records.append({f"c{i}": v for i, v in enumerate(row)})

    def clean(col: str) -> str:
        if col.startswith("BonusesDetails[") and col.endswith("]"):
            return col[len("BonusesDetails["):-1]
        return col.strip("[]")

    records = [{clean(k): v for k, v in rec.items()} for rec in records]
    return pd.DataFrame(records)

def _fmt_date_series(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s, errors="coerce")
    bad = (d.dt.year == 1899) & (d.dt.month == 12) & (d.dt.day == 30)
    d = d.mask(bad)
    return d.dt.strftime("%d.%m.%Y")

def build_excel(df: pd.DataFrame, employee: str, period_ym: str, path_dir: str) -> str:
    import xlsxwriter

    cur_mask  = df["RecordType"].fillna("").str.contains("Поточ", case=False)
    prev_mask = ~cur_mask

    r  = df["ManagerRole"].fillna("").str.lower()
    rm = df["ManagerRoleWithSales"].fillna("").str.lower()

    sales_mask   = r.str.contains("сейл") | rm.str.contains("sales", regex=False)
    ops_mgr_mask = (r.str.contains(r"операт|операц")) & (~r.str.contains("процент"))
    ops_pct_mask = r.str.contains("процент") | rm.str.contains("percent", regex=False)

    def fnum(x):
        try: return float(x)
        except: return 0.0

    def agg_row(descr, role_mask):
        cur  = df[cur_mask  & role_mask]
        prv  = df[prev_mask & role_mask]
        allr = df[role_mask]

        accrual = round(cur["Bonus"].map(fnum).sum(), 2)
        to_cur  = round(cur["ToPay"].map(fnum).sum(), 2)
        to_prev = round(prv["ToPay"].map(fnum).sum(), 2)
        unpaid  = round(accrual - to_cur, 2)
        curr    = allr["Currency"].iloc[0] if "Currency" in allr and allr["Currency"].nunique()==1 \
                  else (df["Currency"].iloc[0] if "Currency" in df else "")
        return [employee, descr, accrual, to_cur, to_prev, unpaid, curr]

    summary_rows = [
        agg_row("Оперативний менеджер", ops_mgr_mask),
        agg_row("Процент оперативний",  ops_pct_mask),
        agg_row("Сейлс",                sales_mask),
    ]
    total_accrual = sum(r[2] for r in summary_rows)
    total_cur     = sum(r[3] for r in summary_rows)
    total_prev    = sum(r[4] for r in summary_rows)
    total_unpaid  = round(total_accrual - total_cur, 2)
    currency_val  = summary_rows[0][6] or (df["Currency"].iloc[0] if "Currency" in df else "")

    def make_section(dfs: pd.DataFrame, title: str, prev: bool):
        base_cols = [
            ("Employee","Менеджер"), ("Client","Клієнт"), ("DealType","Тип угоди"),
            ("DealNumber","Угода"), ("DealCompletionDate","Дата завершення"),
            ("ManagerRole","Роль менеджера"), ("Deprtment","Відділ"),
            ("PercentValue","Процент"), ("Currency","Валюта"),
            ("Income","Дохід"), ("Profit","Прибуток"), ("PercentPaid","Процент оплати"),
            ("Bonus","Бонус"), ("ToPay","До виплати"), ("NotPayYet","Не виплачено"),
            ("PayDate","Дата оплати"),
        ]
        out = pd.DataFrame({dst: dfs.get(src) for src, dst in base_cols})

        if prev:
            if "Period" in dfs.columns:        out["Період"] = dfs["Period"]
            if "ProfitBecome" in dfs.columns:  out["Прибуток новий"] = dfs["ProfitBecome"]
            if "ExchangeRateDifference" in dfs.columns:
                out["Курсова різниця"] = pd.to_numeric(dfs["ExchangeRateDifference"], errors="coerce").round(2)
            elif "ProfitDiference" in dfs.columns:
                out["Курсова різниця"] = pd.to_numeric(dfs["ProfitDiference"], errors="coerce").round(2)
            if "NewBonus" in dfs.columns:
                out["Новий бонус"] = pd.to_numeric(dfs["NewBonus"].astype(str).str.replace(",", ".", regex=False),
                                                   errors="coerce")

        for dcol in ["Дата завершення", "Дата оплати", "Період"]:
            if dcol in out.columns:
                out[dcol] = _fmt_date_series(out[dcol])

        if out.dropna(how="all").empty:
            return title, pd.DataFrame([{"(порожньо)": ""}])

        sum_cols = [c for c in ["Дохід","Прибуток","Бонус","До виплати","Не виплачено",
                                "Прибуток новий","Курсова різниця","Новий бонус"] if c in out.columns]
        totals = {c: round(pd.to_numeric(out[c], errors="coerce").fillna(0).sum(), 2) for c in sum_cols}
        total_row = {k: "" for k in out.columns}
        total_row.update({"Менеджер": "Разом:", **totals})
        out = pd.concat([out, pd.DataFrame([total_row], columns=out.columns)], ignore_index=True)
        return title, out

    def build_prev_sales_section(dfs: pd.DataFrame):
        cols_order = [
            "Менеджер","Клієнт","Тип угоди","Угода","Дата завершення","Роль менеджера",
            "Процент","Прибуток","Бонус база","Процент оплати","Період","Прибуток новий",
            "Курсова різниця","Новий бонус","Було не виплачено","До виплати","Остаток",
            "Тип процента","Продавець"
        ]
        src_map = {
            "Менеджер":"Employee","Клієнт":"Client","Тип угоди":"DealType","Угода":"DealNumber",
            "Дата завершення":"DealCompletionDate","Роль менеджера":"ManagerRole","Процент":"PercentValue",
            "Прибуток":"Profit","Бонус база":"BonusBase","Процент оплати":"PercentPaid","Період":"DealCompletionDate",
            "Прибуток новий":"ProfitBecome","Було не виплачено":"NotPayYet","До виплати":"ToPay",
            "Остаток":"Saldo","Тип процента":"TypePercent","Продавець":"SelerFomDeal","Новий бонус":"NewBonus",
        }
        out = pd.DataFrame({c: (dfs[src_map[c]] if src_map.get(c) in dfs.columns else pd.NA) for c in cols_order})

        if "ExchangeRateDifference" in dfs.columns:
            out["Курсова різниця"] = pd.to_numeric(dfs["ExchangeRateDifference"], errors="coerce").round(2)
        elif "ProfitDiference" in dfs.columns:
            out["Курсова різниця"] = pd.to_numeric(dfs["ProfitDiference"], errors="coerce").round(2)

        if "NewBonus" in dfs.columns:
            out["Новий бонус"] = pd.to_numeric(dfs["NewBonus"].astype(str).str.replace(",", ".", regex=False),
                                               errors="coerce")

        for dcol in ["Дата завершення", "Період"]:
            out[dcol] = _fmt_date_series(out[dcol])

        sum_cols = ["Прибуток","Бонус база","Новий бонус","До виплати","Остаток","Курсова різниця","Було не виплачено"]
        totals = {c: round(pd.to_numeric(out[c], errors="coerce").fillna(0).sum(), 2) for c in sum_cols if c in out.columns}
        total_row = {k: "" for k in cols_order}
        total_row.update({"Менеджер": "Разом:", **totals})
        out = pd.concat([out, pd.DataFrame([total_row], columns=out.columns)], ignore_index=True)
        return "Бонуси сейлс менеджера (минулий період)", out

    def build_prev_ops_pct_section(dfs: pd.DataFrame):
        cols_order = [
            "Менеджер","Клієнт","Тип угоди","Угода","Дата завершення","Роль менеджера",
            "Процент","Прибуток","Бонус база","Процент оплати","Період","Прибуток новий",
            "Курсова різниця","Новий бонус","Було не виплачено","До виплати","Остаток"
        ]
        src_map = {
            "Менеджер":"Employee","Клієнт":"Client","Тип угоди":"DealType","Угода":"DealNumber",
            "Дата завершення":"DealCompletionDate","Роль менеджера":"ManagerRole","Процент":"PercentValue",
            "Прибуток":"Profit","Бонус база":"BonusBase","Процент оплати":"PercentPaid","Період":"DealCompletionDate",
            "Прибуток новий":"ProfitBecome","Було не виплачено":"NotPayYet","До виплати":"ToPay","Остаток":"Saldo",
            "Новий бонус":"NewBonus",
        }
        out = pd.DataFrame({c: (dfs[src_map[c]] if src_map.get(c) in dfs.columns else pd.NA) for c in cols_order})

        if "ExchangeRateDifference" in dfs.columns:
            out["Курсова різниця"] = pd.to_numeric(dfs["ExchangeRateDifference"], errors="coerce").round(2)
        elif "ProfitDiference" in dfs.columns:
            out["Курсова різниця"] = pd.to_numeric(dfs["ProfitDiference"], errors="coerce").round(2)

        if "NewBonus" in dfs.columns:
            out["Новий бонус"] = pd.to_numeric(dfs["NewBonus"].astype(str).str.replace(",", ".", regex=False),
                                               errors="coerce")

        for dcol in ["Дата завершення","Період"]:
            out[dcol] = _fmt_date_series(out[dcol])

        sum_cols = ["Прибуток","Бонус база","Новий бонус","До виплати","Остаток","Курсова різниця","Було не виплачено"]
        totals = {c: round(pd.to_numeric(out[c], errors="coerce").fillna(0).sum(), 2) for c in sum_cols if c in out.columns}
        total_row = {k: "" for k in cols_order}
        total_row.update({"Менеджер": "Разом:", **totals})
        out = pd.concat([out, pd.DataFrame([total_row], columns=out.columns)], ignore_index=True)
        return "Процент операційний (минулий період)", out

    sections = [
        make_section(df[cur_mask & sales_mask].copy(),   "Бонуси сейлс менеджера (поточний період)", False),
        make_section(df[cur_mask & ops_mgr_mask].copy(), "Бонуси оперативному менеджеру", False),
        make_section(df[cur_mask & ops_pct_mask].copy(), "Бонуси оперативному менеджеру (процент)", False),
        build_prev_sales_section(df[prev_mask & sales_mask].copy()),
        build_prev_ops_pct_section(df[prev_mask & ops_pct_mask].copy()),
    ]

    # filename
    docnum = None
    if "DocNumber" in df.columns:
        vals = [str(v) for v in df["DocNumber"].dropna().tolist() if str(v).strip()]
        if vals: docnum = vals[0]
    safe_doc = re.sub(r"[^A-Za-z0-9_\-]+", "", docnum) if docnum else "NO_DOC"

    fname = os.path.join(path_dir, f"BonusesDetails_Report_{employee}_{period_ym}_{safe_doc}.xlsx")
    if os.path.exists(fname):
        os.remove(fname)

    # write excel
    with pd.ExcelWriter(fname, engine="xlsxwriter") as writer:
        wb = writer.book
        ws = wb.add_worksheet("Звіт")
        writer.sheets["Звіт"] = ws

        def xwrite(ws, r, c, v, fmt=None):
            try:
                if pd.isna(v) or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                    v = None
            except Exception:
                v = None
            if fmt is None: ws.write(r, c, v)
            else:           ws.write(r, c, v, fmt)

        title_fmt  = wb.add_format({"bold": True, "font_size": 12})
        header_fmt = wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border":1,
                                    "align":"center","valign":"vcenter","text_wrap": True})
        cell_fmt   = wb.add_format({"border":1})
        bold_fmt   = wb.add_format({"bold": True, "border":1})

        ws.merge_range(0, 0, 0, 7, "Зведена інформація", title_fmt)

        headers = [
            "Менеджер","Опис",
            "Нараховано\n(поточний місяць)",
            "До виплати\n(поточний період)",
            "До виплати\n(минулий період)",
            "Невиплачений\nзалишок",
            "Валюта",
        ]
        ws.write_row(1, 0, headers, header_fmt); ws.set_row(1, 40)

        row = 2
        for rdata in summary_rows:
            for c, v in enumerate(rdata): xwrite(ws, row, c, v, cell_fmt)
            row += 1

        xwrite(ws, row, 1, "Разом", bold_fmt)
        xwrite(ws, row, 2, total_accrual, bold_fmt)
        xwrite(ws, row, 3, total_cur, bold_fmt)
        xwrite(ws, row, 4, total_prev, bold_fmt)
        xwrite(ws, row, 5, total_unpaid, bold_fmt)
        xwrite(ws, row, 6, currency_val, bold_fmt)
        row += 2

        ws.merge_range(row, 2, row, 4, "Итого к выплате", bold_fmt)
        xwrite(ws, row, 5, round(total_cur + total_prev, 2), bold_fmt)
        xwrite(ws, row, 6, currency_val, bold_fmt)
        row += 2

        ws.set_column(0, 1, 18); ws.set_column(2, 4, 22); ws.set_column(5, 6, 14)

        sec_title_fmt  = wb.add_format({"bold": True, "font_size": 12})
        sec_header_fmt = wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border":1,
                                        "align":"center","valign":"vcenter","text_wrap": True})
        sec_cell_fmt   = wb.add_format({"border":1})

        for title, df_sec in sections:
            ws.write(row, 0, title, sec_title_fmt); row += 1

            # header
            ws.set_row(row, 28)
            for c, name in enumerate(df_sec.columns.tolist()):
                xwrite(ws, row, c, name, sec_header_fmt)

            # rows
            nrows, ncols = df_sec.shape
            for i in range(nrows):
                for j in range(ncols):
                    xwrite(ws, row + 1 + i, j, df_sec.iat[i, j], sec_cell_fmt)

            row += nrows + 2

    return fname

def generate_excel(employee: str, period_ym: str) -> str:
    """
    Повертає шлях до тимчасового xlsx-файлу. Видаляти файл ПІСЛЯ відправки!
    """
    token = get_power_bi_token()
    raw   = query_bonuses_details(token, employee, period_ym)
    df    = to_dataframe(raw)

    # впорядкування (не обов'язково)
    preferred = [
        "Employee","PeriodYM","Period","DocNumber","DealNumber","DealCompletionDate",
        "ManagerRole","ManagerRoleWithSales","EffectiveManager","Deprtment","DepartmentFromEmp",
        "DealType","Client","Currency","Income","Profit","ProfitBecome","ProfitDiference",
        "ExchangeRateDifference","NewBonus","PercentValue","Bonus","PercentPaid",
        "ToPay","NotPayYet","PayDate","RecordType"
    ]
    if not df.empty:
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        df = df[cols]

    temp_dir = tempfile.mkdtemp(prefix="bonuses_")
    out_file = build_excel(df, employee, period_ym, path_dir=temp_dir)
    return out_file

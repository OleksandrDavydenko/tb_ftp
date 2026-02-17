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
from utils.name_aliases import display_name

DATASET_ID = os.getenv("PBI_DATASET_ID", "8b80be15-7b31-49e4-bc85-8b37a0d98f1c")

# ---------- Power BI queries ----------
def _exec_dax(token: str, dax: str) -> dict:
    """Універсальна функція для виконання DAX запитів"""
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    r = requests.post(url, headers=headers, json=payload)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"❌ DAX запит повернув помилку: {e}")
        print(f"❌ DAX запит: {dax}")
        print(f"❌ Відповідь сервера: {r.text}")
        raise
    return r.json()

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
    return _exec_dax(token, dax)

def query_bonuses_table(token: str, employee: str, period_ym: str) -> dict:
    emp_escaped = employee.replace('"', '""')
    dax = f"""
EVALUATE
FILTER(
  ADDCOLUMNS(
    SELECTCOLUMNS(
      BonusesTable,
      "Employee", BonusesTable[Employee],
      "Date", BonusesTable[Date],
      "Sanction", BonusesTable[Sanction],
      "BonusCorrection", BonusesTable[BonusCorrection]
    ),
    "PeriodYM", FORMAT([Date], "yyyy-MM")
  ),
  [Employee] = "{emp_escaped}" && [PeriodYM] = "{period_ym}"
)
"""
    return _exec_dax(token, dax)

def query_bonuses_table_not_paid(token: str, employee: str, period_ym: str) -> dict:
    emp_escaped = employee.replace('"', '""')
    dax = f"""
EVALUATE
FILTER(
  ADDCOLUMNS(
    SELECTCOLUMNS(
      BonusesTableNotPaid,
      "Employee", BonusesTableNotPaid[Employee],
      "Client", BonusesTableNotPaid[Client],
      "DealCompletionDate", BonusesTableNotPaid[DealCompletionDate],
      "DealNumber", BonusesTableNotPaid[DealNumber],
      "DealType", BonusesTableNotPaid[DealType],
      "ManagerRole", BonusesTableNotPaid[ManagerRole],
      "PercentValue", BonusesTableNotPaid[PercentValue],
      "Profit", BonusesTableNotPaid[Profit],
      "BonusBase", BonusesTableNotPaid[BonusBase],
      "PercentPaid", BonusesTableNotPaid[PercentPaid],
      "Period", BonusesTableNotPaid[Period],
      "ProfitNew", BonusesTableNotPaid[ProfitNew],
      "ExchangeRateDifference", BonusesTableNotPaid[ExchangeRateDifference],
      "NewBonus", BonusesTableNotPaid[NewBonus],
      "notPayed", BonusesTableNotPaid[notPayed],
      "toPay", BonusesTableNotPaid[toPay],
      "Saldo", BonusesTableNotPaid[Saldo],
      "TypePercent", BonusesTableNotPaid[TypePercent],
      "Seller", BonusesTableNotPaid[Seller]
    ),
    "PeriodYM", FORMAT([Period], "yyyy-MM")
  ),
  [Employee] = "{emp_escaped}" && [PeriodYM] = "{period_ym}"
)
"""
    return _exec_dax(token, dax)

def query_plan_for_sellers_new_clients(token: str, employee: str, period_ym: str) -> dict:
    emp_escaped = employee.replace('"', '""')
    dax = f"""
EVALUATE
FILTER(
  ADDCOLUMNS(
    SELECTCOLUMNS(
      PlanForSelersNewClients,
      "Client", PlanForSelersNewClients[Client],
      "DealNumber", PlanForSelersNewClients[DealNumber],
      "DepartmentFromDealType", PlanForSelersNewClients[DepartmentFromDealType],
      "DokNumber", PlanForSelersNewClients[DokNumber],
      "Manager", PlanForSelersNewClients[Manager],
      "Period", PlanForSelersNewClients[Period],
      "ProfitNew", PlanForSelersNewClients[ProfitNew]
    ),
    "PeriodYM", FORMAT([Period], "yyyy-MM")
  ),
  [Manager] = "{emp_escaped}" && [PeriodYM] = "{period_ym}"
)
"""
    return _exec_dax(token, dax)

# ---------- helpers ----------
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
        if col.startswith("BonusesTable[") and col.endswith("]"):
            return col[len("BonusesTable["):-1]
        if col.startswith("BonusesTableNotPaid[") and col.endswith("]"):
            return col[len("BonusesTableNotPaid["):-1]
        if col.startswith("PlanForSelersNewClients[") and col.endswith("]"):
            return col[len("PlanForSelersNewClients["):-1]
        return col.strip("[]")

    records = [{clean(k): v for k, v in rec.items()} for rec in records]
    return pd.DataFrame(records)

def _fmt_date_series(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s, errors="coerce")
    bad = (d.dt.year == 1899) & (d.dt.month == 12) & (d.dt.day == 30)
    d = d.mask(bad)
    return d.dt.strftime("%d.%m.%Y")

# ---------- Excel builder ----------
def build_excel(df: pd.DataFrame, employee: str, period_ym: str,
                sanction_sum: float, correction_sum: float,
                not_paid_df: pd.DataFrame, new_clients_df: pd.DataFrame,
                path_dir: str) -> str:
    nice = display_name(employee)

    cur_mask  = df["RecordType"].fillna("").str.contains("Поточ", case=False)
    prev_mask = ~cur_mask

    r  = df["ManagerRole"].fillna("").str.lower()
    rm = df["ManagerRoleWithSales"].fillna("").str.lower()

    sales_mask   = r.str.contains("сейл") | rm.str.contains("sales", regex=False)
    ops_mgr_mask = (r.str.contains(r"операт|операц", na=False)) & (~r.str.contains("процент", na=False))
    ops_pct_mask = (r.str.contains("процент", na=False)) | rm.str.contains("percent", regex=False, na=False)

    def fnum(x):
        try: return float(x)
        except: return 0.0

    # Функція для отримання суми Остаток з таблиці невиплачених бонусів по ролі
    def get_not_paid_saldo_by_role(role_mask):
        if not_paid_df.empty:
            return 0.0
        
        # Визначаємо роль для фільтрації в not_paid_df
        if role_mask is sales_mask:
            role_filter = not_paid_df["ManagerRole"].fillna("").str.lower().str.contains("сейлс|sales", case=False, na=False)
        elif role_mask is ops_mgr_mask:
            role_filter = not_paid_df["ManagerRole"].fillna("").str.lower().str.contains("оперативний|операційний", case=False, na=False) & \
                         ~not_paid_df["ManagerRole"].fillna("").str.lower().str.contains("процент", case=False, na=False)
        elif role_mask is ops_pct_mask:
            role_filter = not_paid_df["ManagerRole"].fillna("").str.lower().str.contains("процент", case=False, na=False)
        else:
            role_filter = pd.Series(False, index=not_paid_df.index)
        
        filtered = not_paid_df[role_filter]
        return round(pd.to_numeric(filtered.get("Saldo"), errors="coerce").fillna(0).sum(), 2)

    def agg_row(descr, role_mask):
        cur  = df[cur_mask  & role_mask]
        prv  = df[prev_mask & role_mask]
        allr = df[role_mask]

        accrual = round(cur["Bonus"].map(fnum).sum(), 2)
        to_cur  = round(cur["ToPay"].map(fnum).sum(), 2)
        to_prev = round(prv["ToPay"].map(fnum).sum(), 2)
        unpaid_cur = round(accrual - to_cur, 2)
        
        # Отримуємо Остаток з таблиці невиплачених бонусів
        not_paid_saldo = get_not_paid_saldo_by_role(role_mask)
        # Додаємо поточний невиплачений залишок до загального
        unpaid_all = round(unpaid_cur + not_paid_saldo, 2)

        if "Currency" in allr and not allr["Currency"].dropna().empty:
            curr = allr["Currency"].dropna().iloc[0]
        elif "Currency" in df and not df["Currency"].dropna().empty:
            curr = df["Currency"].dropna().iloc[0]
        else:
            curr = ""

        return [nice, descr, accrual, to_cur, to_prev, unpaid_cur, unpaid_all, curr]

    # Створюємо рядки для кожної ролі
    all_rows = [
        ("Оперативний менеджер", ops_mgr_mask),
        ("Процент оперативний", ops_pct_mask),
        ("Сейлс", sales_mask),
    ]
    
    summary_rows = []
    for descr, mask in all_rows:
        row_data = agg_row(descr, mask)
        # Перевіряємо чи всі числові значення дорівнюють 0 (крім перших двох колонок)
        if not (row_data[2] == 0 and row_data[3] == 0 and row_data[4] == 0 and row_data[5] == 0 and row_data[6] == 0):
            summary_rows.append(row_data)

    sanction_sum   = round(float(sanction_sum or 0), 2)
    correction_sum = round(float(correction_sum or 0), 2)

    currency_val = ""
    if "Currency" in df and not df["Currency"].dropna().empty:
        currency_val = df["Currency"].dropna().iloc[0]

    if abs(sanction_sum) != 0:
        summary_rows.append([nice, "Штраф",       sanction_sum,   sanction_sum,   0.0, 0.0, 0.0, currency_val])
    if abs(correction_sum) != 0:
        summary_rows.append([nice, "Конкурс 10%", correction_sum, correction_sum, 0.0, 0.0, 0.0, currency_val])

    # Якщо після фільтрації не залишилося рядків, додаємо хоча б один з нулями
    if not summary_rows:
        summary_rows.append([nice, "Немає даних", 0, 0, 0, 0, 0, currency_val])

    total_accrual = round(sum(r[2] for r in summary_rows), 2)
    total_cur     = round(sum(r[3] for r in summary_rows), 2)
    total_prev    = round(sum(r[4] for r in summary_rows), 2)
    total_unpaid_cur = round(sum(r[5] for r in summary_rows), 2)
    total_unpaid_all = round(sum(r[6] for r in summary_rows), 2)
    currency_val  = summary_rows[0][7] if summary_rows and summary_rows[0][7] else currency_val

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
        
        # Додаткові колонки для таблиць сейлс менеджера
        additional_cols = []
        if "сейлс" in title.lower():
            if "TypePercent" in dfs.columns:
                additional_cols.append(("TypePercent", "Тип відсотку"))
            if "SelerFomDeal" in dfs.columns:
                additional_cols.append(("SelerFomDeal", "Продавець"))
        
        all_cols = base_cols + additional_cols
        
        out = pd.DataFrame({dst: dfs.get(src) for src, dst in all_cols if src in dfs.columns or pd.NA})

        if "Менеджер" in out.columns:
            out["Менеджер"] = out["Менеджер"].apply(
                lambda v: display_name(v) if isinstance(v, str) else v
            )

        if prev:
            if "Period" in dfs.columns:        out["Період"] = dfs["Period"]
            if "ProfitBecome" in dfs.columns:  out["Прибуток новий"] = dfs["ProfitBecome"]
            if "ExchangeRateDifference" in dfs.columns:
                out["Курсова різниця"] = pd.to_numeric(dfs["ExchangeRateDifference"], errors="coerce").round(2)
            elif "ProfitDiference" in dfs.columns:
                out["Курсова різниця"] = pd.to_numeric(dfs["ProfitDiference"], errors="coerce").round(2)
            if "NewBonus" in dfs.columns:
                out["Новий бонус"] = pd.to_numeric(
                    dfs["NewBonus"].astype(str).str.replace(",", ".", regex=False),
                    errors="coerce"
                )

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
            "Остаток":"Saldo","Новий бонус":"NewBonus",
        }
        
        # Додаємо колонки, якщо вони є
        if "TypePercent" in dfs.columns:
            src_map["Тип процента"] = "TypePercent"
        if "SelerFomDeal" in dfs.columns:
            src_map["Продавець"] = "SelerFomDeal"
        
        out = pd.DataFrame({c: (dfs[src_map[c]] if src_map.get(c) in dfs.columns else pd.NA) for c in cols_order})

        if "Менеджер" in out.columns:
            out["Менеджер"] = out["Менеджер"].apply(
                lambda v: display_name(v) if isinstance(v, str) else v
            )

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

        if "Менеджер" in out.columns:
            out["Менеджер"] = out["Менеджер"].apply(
                lambda v: display_name(v) if isinstance(v, str) else v
            )

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
        total_row.update({"Менеджер":"Разом:", **totals})
        out = pd.concat([out, pd.DataFrame([total_row], columns=out.columns)], ignore_index=True)
        return "Процент операційний (минулий період)", out

    def build_not_paid_section(dfs: pd.DataFrame):
        cols_order = [
            "Менеджер","Клієнт","Тип угоди","Угода","Дата завершення","Роль менеджера",
            "Процент","Прибуток","Бонус база","Процент оплати","Період","Прибуток новий",
            "Курсова різниця","Новий бонус","Було не виплачено","До виплати","Остаток",
            "Тип процента","Продавець"
        ]
        
        src_map = {
            "Менеджер": "Employee",
            "Клієнт": "Client",
            "Тип угоди": "DealType",
            "Угода": "DealNumber",
            "Дата завершення": "DealCompletionDate",
            "Роль менеджера": "ManagerRole",
            "Процент": "PercentValue",
            "Прибуток": "Profit",
            "Бонус база": "BonusBase",
            "Процент оплати": "PercentPaid",
            "Період": "Period",
            "Прибуток новий": "ProfitNew",
            "Курсова різниця": "ExchangeRateDifference",
            "Новий бонус": "NewBonus",
            "Було не виплачено": "notPayed",
            "До виплати": "toPay",
            "Остаток": "Saldo",
            "Тип процента": "TypePercent",
            "Продавець": "Seller"
        }
        
        out = pd.DataFrame({c: (dfs[src_map[c]] if src_map.get(c) in dfs.columns else pd.NA) for c in cols_order})

        if "Менеджер" in out.columns:
            out["Менеджер"] = out["Менеджер"].apply(
                lambda v: display_name(v) if isinstance(v, str) else v
            )

        for dcol in ["Дата завершення", "Період"]:
            out[dcol] = _fmt_date_series(out[dcol])

        sum_cols = ["Прибуток","Бонус база","Новий бонус","До виплати","Остаток","Курсова різниця","Було не виплачено"]
        totals = {c: round(pd.to_numeric(out[c], errors="coerce").fillna(0).sum(), 2) for c in sum_cols if c in out.columns}
        total_row = {k: "" for k in cols_order}
        total_row.update({"Менеджер": "Разом:", **totals})
        out = pd.concat([out, pd.DataFrame([total_row], columns=out.columns)], ignore_index=True)
        return "Таблиця невиплачених бонусів", out

    def prepare_new_clients_sheet(dfs: pd.DataFrame):
        """Підготовка даних для листа 'Нові Угоди'"""
        if dfs.empty:
            return pd.DataFrame()
        
        result_df = pd.DataFrame()
        
        # Додаємо стовпець "Угода"
        if "DealNumber" in dfs.columns:
            result_df["Угода"] = dfs["DealNumber"]
        else:
            result_df["Угода"] = ""
        
        # Додаємо стовпець "Клієнт"
        client_col = None
        for col in dfs.columns:
            if 'client' in col.lower() or 'клієнт' in col.lower():
                client_col = col
                break
        
        if client_col:
            result_df["Клієнт"] = dfs[client_col]
        else:
            result_df["Клієнт"] = ""
        
        # Додаємо стовпець "Менеджер"
        manager_col = None
        for col in dfs.columns:
            if 'manager' in col.lower() or 'менеджер' in col.lower():
                manager_col = col
                break
        
        if manager_col and manager_col in dfs.columns:
            result_df["Менеджер"] = dfs[manager_col].apply(
                lambda v: display_name(v) if isinstance(v, str) else v
            )
        else:
            result_df["Менеджер"] = ""
        
        # Додаємо стовпець "Прибуток"
        profit_col = None
        for col in dfs.columns:
            if 'profit' in col.lower() or 'прибуток' in col.lower():
                profit_col = col
                break
        
        if profit_col and profit_col in dfs.columns:
            profit_series = pd.to_numeric(dfs[profit_col], errors="coerce")
            result_df["Прибуток"] = profit_series
            
            result_df["Прибуток"] = result_df["Прибуток"].apply(
                lambda x: f"{x:,.2f}".replace(",", " ").replace(".", ",") if not pd.isna(x) else ""
            )
        else:
            result_df["Прибуток"] = ""
        
        # Додаємо рядок підсумків
        if profit_col and profit_col in dfs.columns and not dfs.empty:
            total_profit = pd.to_numeric(dfs[profit_col], errors="coerce").sum()
            if not pd.isna(total_profit):
                total_row = {
                    "Угода": "",
                    "Клієнт": "",
                    "Менеджер": "",
                    "Прибуток": f"{total_profit:,.2f}".replace(",", " ").replace(".", ",")
                }
                
                total_df = pd.DataFrame([total_row])
                result_df = pd.concat([result_df, total_df], ignore_index=True)
        
        return result_df

    sections = [
        make_section(df[cur_mask & sales_mask].copy(),   "Бонуси сейлс менеджера (поточний період)", False),
        make_section(df[cur_mask & ops_mgr_mask].copy(), "Бонуси оперативному менеджеру", False),
        make_section(df[cur_mask & ops_pct_mask].copy(), "Бонуси оперативному менеджеру (процент)", False),
        build_prev_sales_section(df[prev_mask & sales_mask].copy()),
        build_prev_ops_pct_section(df[prev_mask & ops_pct_mask].copy()),
    ]
    
    if not not_paid_df.empty:
        sections.append(build_not_paid_section(not_paid_df))

    docnum = None
    if "DocNumber" in df.columns:
        vals = [str(v) for v in df["DocNumber"].dropna().tolist() if str(v).strip()]
        if vals: docnum = vals[0]
    safe_doc = re.sub(r"[^A-Za-z0-9_\-]+", "", docnum) if docnum else "NO_DOC"

    safe_nice = re.sub(r"[^A-Za-z0-9_\-]+", "", display_name(employee))
    fname = os.path.join(path_dir, f"BonusesDetails_Report_{safe_nice}_{period_ym}_{safe_doc}.xlsx")
    if os.path.exists(fname):
        os.remove(fname)

    with pd.ExcelWriter(fname, engine="xlsxwriter") as writer:
        wb = writer.book
        
        # ========== ЛИСТ 1: ЗВІТ ==========
        ws = wb.add_worksheet("Звіт")
        writer.sheets["Звіт"] = ws

        def xwrite(ws, r, c, v, fmt=None):
            try:
                if v is None or pd.isna(v) or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
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
            "Невиплачений залишок\n(поточний період)",
            "Невиплачений залишок\n(всі періоди)",
            "Валюта",
        ]
        ws.write_row(1, 0, headers, header_fmt)
        ws.set_row(1, 70)

        row = 2
        for rdata in summary_rows:
            for c, v in enumerate(rdata): xwrite(ws, row, c, v, cell_fmt)
            row += 1

        xwrite(ws, row, 1, "Разом", bold_fmt)
        xwrite(ws, row, 2, total_accrual, bold_fmt)
        xwrite(ws, row, 3, total_cur, bold_fmt)
        xwrite(ws, row, 4, total_prev, bold_fmt)
        xwrite(ws, row, 5, total_unpaid_cur, bold_fmt)
        xwrite(ws, row, 6, total_unpaid_all, bold_fmt)
        xwrite(ws, row, 7, currency_val, bold_fmt)
        row += 2

        ws.merge_range(row, 2, row, 5, "Всього до виплати", bold_fmt)
        xwrite(ws, row, 6, round(total_cur + total_prev, 2), bold_fmt)
        xwrite(ws, row, 7, currency_val, bold_fmt)
        row += 2

        ws.set_column(0, 0, 16)
        ws.set_column(1, 1, 14)
        ws.set_column(2, 2, 16)
        ws.set_column(3, 3, 16)
        ws.set_column(4, 4, 16)
        ws.set_column(5, 5, 20)
        ws.set_column(6, 6, 20)
        ws.set_column(7, 7, 8)

        sec_title_fmt  = wb.add_format({"bold": True, "font_size": 12})
        sec_header_fmt = wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border":1,
                                        "align":"center","valign":"vcenter","text_wrap": True})
        sec_cell_fmt   = wb.add_format({"border":1})

        for title, df_sec in sections:
            ws.write(row, 0, title, sec_title_fmt); row += 1

            ws.set_row(row, 28)
            for c, name in enumerate(df_sec.columns.tolist()):
                xwrite(ws, row, c, name, sec_header_fmt)

            nrows, ncols = df_sec.shape
            for i in range(nrows):
                for j in range(ncols):
                    xwrite(ws, row + 1 + i, j, df_sec.iat[i, j], sec_cell_fmt)

            row += nrows + 2

        # ========== ЛИСТ 2: НОВІ УГОДИ ==========
        if not new_clients_df.empty:
            new_clients_sheet = wb.add_worksheet("Нові Угоди")
            writer.sheets["Нові Угоди"] = new_clients_sheet
            
            prepared_data = prepare_new_clients_sheet(new_clients_df)
            
            if not prepared_data.empty:
                new_clients_sheet.merge_range(0, 0, 0, len(prepared_data.columns)-1, 
                                            "Нові угоди", title_fmt)
                
                for col_idx, column_name in enumerate(prepared_data.columns):
                    new_clients_sheet.write(1, col_idx, column_name, header_fmt)
                
                for row_idx in range(len(prepared_data)):
                    for col_idx, column_name in enumerate(prepared_data.columns):
                        value = prepared_data.iloc[row_idx, col_idx]
                        
                        if row_idx == len(prepared_data) - 1:
                            bold_cell_fmt = wb.add_format({"border":1, "bold": True})
                            new_clients_sheet.write(row_idx + 2, col_idx, value, bold_cell_fmt)
                        else:
                            new_clients_sheet.write(row_idx + 2, col_idx, value, cell_fmt)
                
                column_widths = {
                    "Угода": 20,
                    "Клієнт": 35,
                    "Менеджер": 25,
                    "Прибуток": 15
                }
                
                for i, col in enumerate(prepared_data.columns):
                    width = column_widths.get(col, 12)
                    new_clients_sheet.set_column(i, i, width)

    return fname

# ---------- main API ----------
def generate_excel(employee: str, period_ym: str) -> str:
    """
    Повертає шлях до тимчасового xlsx-файлу. Видаляти файл ПІСЛЯ відправки!
    """
    token = get_power_bi_token()

    # Деталі бонусів
    raw_details = query_bonuses_details(token, employee, period_ym)
    df_details  = to_dataframe(raw_details)

    if df_details.empty:
        return None

    # BonusesTable: санкції/корекції
    raw_tbl = query_bonuses_table(token, employee, period_ym)
    df_tbl  = to_dataframe(raw_tbl)

    if df_tbl.empty:
        sanction_sum = 0.0
        correction_sum = 0.0
    else:
        sanction_sum = round(pd.to_numeric(df_tbl.get("Sanction"), errors="coerce").fillna(0).sum(), 2)
        if "BonusCorrection" in df_tbl.columns:
            correction_sum = round(
                pd.to_numeric(
                    df_tbl["BonusCorrection"].astype(str).str.replace(",", ".", regex=False),
                    errors="coerce"
                ).fillna(0).sum(), 2
            )
        else:
            correction_sum = 0.0

    # Таблиця невиплачених бонусів
    raw_not_paid = query_bonuses_table_not_paid(token, employee, period_ym)
    df_not_paid = to_dataframe(raw_not_paid)
    
    # Нові угоди
    try:
        raw_new_clients = query_plan_for_sellers_new_clients(token, employee, period_ym)
        df_new_clients = to_dataframe(raw_new_clients)
    except Exception as e:
        print(f"⚠️ Помилка при отриманні даних з PlanForSelersNewClients: {e}")
        df_new_clients = pd.DataFrame()

    # впорядкування
    preferred = [
        "Employee","PeriodYM","Period","DocNumber","DealNumber","DealCompletionDate",
        "ManagerRole","ManagerRoleWithSales","EffectiveManager","Deprtment","DepartmentFromEmp",
        "DealType","Client","Currency","Income","Profit","ProfitBecome","ProfitDiference",
        "ExchangeRateDifference","NewBonus","PercentValue","Bonus","PercentPaid",
        "ToPay","NotPayYet","PayDate","RecordType","TypePercent","SelerFomDeal"
    ]
    cols = [c for c in preferred if c in df_details.columns] + [c for c in df_details.columns if c not in preferred]
    df_details = df_details[cols]

    temp_dir = tempfile.mkdtemp(prefix="bonuses_")
    out_file = build_excel(df_details, employee, period_ym, sanction_sum, correction_sum, 
                          df_not_paid, df_new_clients, path_dir=temp_dir)
    return out_file
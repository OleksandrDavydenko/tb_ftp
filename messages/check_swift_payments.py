import os
import time
import html
import requests

from db import get_unnotified_swift_payments, mark_swift_payments_notified, get_active_users

KEY = os.getenv("TELEGRAM_BOT_TOKEN")
ADDITIONAL_TELEGRAM_IDS = [203148640]  # Додаткові Telegram ID, які отримують повідомлення про всі платежі
TG_API = f"https://api.telegram.org/bot{KEY}/sendMessage"


def _fmt_amount(value):
    if value is None:
        return "—"
    try:
        return f"{float(value):,.2f}".replace(",", " ")
    except (TypeError, ValueError):
        return str(value)


def _is_true(value) -> bool:
    """PBI повертає HasSwift рядком ('True'/'False'/'1'/'0'/'Так' тощо)."""
    return str(value).strip().lower() in {"true", "1", "yes", "так", "да", "y"}


def _fmt_comment(value) -> str:
    """Коментар у моноспейсі; якщо порожній — прочерк."""
    text = str(value).strip() if value is not None else ""
    if not text:
        return "—"
    return f"<code>{html.escape(text)}</code>"


def _send(telegram_id: int | str, text: str) -> bool:
    if not KEY:
        return False
    try:
        chat_id = int(telegram_id)
    except Exception:
        return False
    r = requests.post(
        TG_API,
        data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )
    if r.status_code == 200 and r.json().get("ok"):
        return True
    if r.status_code == 429:  # flood control
        wait = int(r.json().get("parameters", {}).get("retry_after", 2))
        time.sleep(wait)
        r = requests.post(
            TG_API,
            data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=15,
        )
        return r.status_code == 200 and r.json().get("ok")
    return False


def check_swift_payments():
    payments = get_unnotified_swift_payments()
    if not payments:
        return

    print(f"📦 SWIFT: отримано {len(payments)} невідправлених платеж(ів) з БД")

    active_map = {str(u["employee_name"]).strip(): u for u in get_active_users()}

    docs_to_mark = []

    for (doc_number, doc_date, currency, amount_currency, amount_usd,
         counterparty, payment_type, account_code, has_swift, employee_name, comment) in payments:

        date_str = doc_date.strftime("%d.%m.%Y") if hasattr(doc_date, "strftime") else str(doc_date)

        amount_str = f"{_fmt_amount(amount_currency)} {currency or ''}".strip()
        comment_str = _fmt_comment(comment)

        # Якщо валюта вже USD — не дублюємо приблизний еквівалент у USD
        is_usd = str(currency or "").strip().upper() == "USD"
        #usd_str = "" if is_usd else f" (≈{_fmt_amount(amount_usd)} USD)"

        if _is_true(has_swift):
            # У платіжного доручення з'явився SWIFT
            msg = (
                "📄 До платіжного доручення з'явився <b>SWIFT</b>:\n"
                f"• Платіжка: <b>{doc_number}</b> від <b>{date_str}</b>\n"
                f"• Контрагент: <b>{counterparty or '—'}</b>\n"
                f"• Сума: <b>{amount_str}</b>\n"
                f"• Відповідальний: {employee_name or '—'}\n"
                #f"• Коментар: {comment_str}"
                
            )
        else:
            # Списання коштів (SWIFT ще немає)
            msg = (
                f"💸 Списання коштів у <b>{currency or '—'}</b>:\n"
                f"• Платіжка: <b>{doc_number}</b> від <b>{date_str}</b>\n"
                f"• Контрагент: <b>{counterparty or '—'}</b>\n"
                f"• Сума: <b>{amount_str}</b>\n"
                f"• Відповідальний: {employee_name or '—'}\n"
                f"• Коментар: {comment_str}"
                
            )

        sent_any = False
        seen_ids = set()

        u = active_map.get(str(employee_name).strip()) if employee_name else None
        if u:
            tg_id = u.get("telegram_id")
            if tg_id and _send(tg_id, msg):
                print(f"✅ SWIFT: відправлено {employee_name} (tg:{tg_id})")
                sent_any = True
                seen_ids.add(tg_id)

        for tg_id in ADDITIONAL_TELEGRAM_IDS:
            if tg_id in seen_ids:
                continue
            if _send(tg_id, msg):
                print(f"✅ SWIFT: відправлено додатковому отримувачу (tg:{tg_id})")
                sent_any = True
                seen_ids.add(tg_id)

        if sent_any:
            docs_to_mark.append((doc_number, doc_date, has_swift, employee_name))

    if docs_to_mark:
        mark_swift_payments_notified(docs_to_mark)
        print(f"✅ SWIFT: оновлено is_notified для {len(docs_to_mark)} платеж(ів)")

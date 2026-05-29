# employee_analytics/monthly_analytics_push.py
# -*- coding: utf-8 -*-
"""
Проактивна місячна аналітика у вигляді дашборду-картинки.

Запускається по календарю (29 числа о 17:00 за Києвом — джоб у telegrambot.py).
Логіка періодів: порівнюємо ПОПЕРЕДНІЙ закритий місяць (M-1) з ще попереднім (M-2).
Напр. запуск 29.05.2026 → дашборд за КВІТЕНЬ, порівняння з БЕРЕЗНЕМ.

Що враховано:
• Кожному співробітнику — окремий дашборд.
• Якщо немає з чим порівнювати (немає метрики за M-2) — блок/елемент порівняння пропускається.
• Якщо немає даних за M-2 взагалі (новачок, перший місяць) — повідомлення НЕ надсилається.
• Бонуси беремо як НАРАХУВАННЯ з рахунку 3330 (fetch_3330) і теж порівнюємо M-1 vs M-2.
• Адаптивність: блоки без даних (бонуси, відпустки/лікарняні) просто не малюються.

ТЕСТУВАННЯ: поки TEST_MODE=True розсилка йде лише на TEST_TELEGRAM_IDS.
Щоб увімкнути для всіх — постав TEST_MODE = False.
"""

import os
import logging
from io import BytesIO
from datetime import datetime

import pytz
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
import matplotlib
from telegram import Bot

from employee_analytics.analytics_table import get_yearly_breakdown, MONTHS_UA
from salary.bonuses_message import fetch_3330
from utils.name_aliases import display_name
from db import get_active_users

try:
    from hr.vacation_sick_report import _fetch_yearly_data
except Exception:  # HR-модуль не критичний — без нього просто не буде блоку
    _fetch_yearly_data = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ──────────────────────────────────────────────────────────────────────────────
# НАЛАШТУВАННЯ
# ──────────────────────────────────────────────────────────────────────────────
TEST_MODE = True
TEST_TELEGRAM_IDS = {152087884}  # сюди свої id для тестування (можна несколько)

KYIV_TZ = pytz.timezone("Europe/Kiev")
_KEY = os.getenv("TELEGRAM_BOT_TOKEN")
bot = Bot(token=_KEY)

MONTHS_UA_SHORT = ["Січ", "Лют", "Бер", "Кві", "Тра", "Чер",
                   "Лип", "Сер", "Вер", "Жов", "Лис", "Гру"]

# Палітра (узгоджена з рештою графіків бота)
WHITE     = (255, 255, 255)
SURFACE   = (241, 243, 245)
TRACK     = (224, 228, 234)
TEXT1     = (31, 41, 51)
TEXT2     = (107, 114, 128)
TEXT3     = (154, 165, 177)
BLUE      = (58, 134, 255)   # поточний місяць
GREY      = (173, 181, 189)  # попередній місяць
GREEN     = (6, 167, 125)
RED       = (239, 35, 60)
GREEN_BG  = (224, 244, 237)
RED_BG    = (252, 235, 235)
AMBER_BG  = (255, 243, 205)
AMBER_TX  = (133, 100, 4)
INFO_BG   = (230, 241, 251)
INFO_TX   = (24, 95, 165)
BORDER    = (228, 231, 236)

# Геометрія
W       = 760
MX      = 28           # зовнішні поля
PADH    = 16           # внутрішній відступ hero
PADC    = 14           # внутрішній відступ карток
TOP     = 22
BOTTOM  = 24
GAP     = 14
HEADER_H = 80
HERO_H   = 152
CARD_H   = 112
GRID_GAP = 12
BONUS_H  = 58
HR_H     = 64


# ──────────────────────────────────────────────────────────────────────────────
# ШРИФТИ (DejaVuSans з matplotlib — має кирилицю)
# ──────────────────────────────────────────────────────────────────────────────
_FONT_DIR = os.path.join(matplotlib.get_data_path(), "fonts", "ttf")


def _font(bold: bool, size: int) -> ImageFont.FreeTypeFont:
    name = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    return ImageFont.truetype(os.path.join(_FONT_DIR, name), size)


F = {
    "month":   _font(True, 22),
    "sub":     _font(False, 15),
    "label":   _font(False, 15),
    "small":   _font(False, 13),
    "tiny":    _font(False, 12),
    "big":     _font(True, 38),
    "val":     _font(True, 22),
    "valsm":   _font(True, 20),
    "pill":    _font(True, 13),
    "avatar":  _font(True, 18),
    "insight": _font(False, 14),
    "insightb": _font(True, 14),
}


# ──────────────────────────────────────────────────────────────────────────────
# ФОРМАТУВАННЯ
# ──────────────────────────────────────────────────────────────────────────────
def _fmt_usd(v) -> str:
    return f"{int(round(v)):,}".replace(",", " ") + " $"


def _fmt_num(v) -> str:
    return f"{int(round(v)):,}".replace(",", " ")


def _fmt_pct(v) -> str:
    return f"{v:.1f}%"


def _pct_delta(curr, prev):
    """Повертає (текст, is_up) або (None, None), якщо порівнювати нема з чим."""
    if prev is None or prev == 0:
        return None, None
    d = (curr - prev) / abs(prev) * 100
    up = d >= 0
    return f"{'↑' if up else '↓'} {abs(d):.0f}%", up


def _pp_delta(curr, prev):
    """Дельта в процентних пунктах (для маржі)."""
    if prev is None:
        return None, None
    d = curr - prev
    up = d >= 0
    return f"{'↑' if up else '↓'} {abs(d):.1f} пп", up


def _wrap(draw, text, font, max_w):
    words = text.split()
    lines, cur = [], ""
    for w in words:
        test = (cur + " " + w).strip()
        if draw.textlength(test, font=font) <= max_w:
            cur = test
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines or [""]


# ──────────────────────────────────────────────────────────────────────────────
# ПРИМІТИВИ МАЛЮВАННЯ
# ──────────────────────────────────────────────────────────────────────────────
def _pill(draw, right_x, y, text, up):
    """Малює капсулу дельти, прив'язану правим краєм до right_x. Повертає її ширину."""
    bg = GREEN_BG if up else RED_BG
    fg = GREEN if up else RED
    tw = draw.textlength(text, font=F["pill"])
    pad = 10
    h = 24
    w = tw + 2 * pad
    x0 = right_x - w
    draw.rounded_rectangle([x0, y, right_x, y + h], radius=12, fill=bg)
    draw.text((x0 + pad, y + h / 2), text, font=F["pill"], fill=fg, anchor="lm")
    return w


def _compare_row(draw, x, y, w, label, value_str, frac, bar_color, bar_h,
                 label_w=44, value_w=104, faded=False):
    """Один рядок порівняння: [мітка] [смуга] [значення справа]."""
    lc = TEXT3 if faded else TEXT2
    draw.text((x, y + bar_h / 2), label, font=F["tiny"], fill=lc, anchor="lm")
    bx0 = x + label_w
    bx1 = x + w - value_w
    bw = max(1, bx1 - bx0)
    draw.rounded_rectangle([bx0, y, bx1, y + bar_h], radius=bar_h // 2, fill=TRACK)
    fill_w = int(bw * max(0.0, min(1.0, frac)))
    if fill_w > 2:
        draw.rounded_rectangle([bx0, y, bx0 + fill_w, y + bar_h], radius=bar_h // 2, fill=bar_color)
    draw.text((x + w, y + bar_h / 2), value_str, font=F["tiny"], fill=lc, anchor="rm")


def _two_bars(draw, x, y, w, prev, curr, prev_lbl, curr_lbl,
              fmt, bar_h=12, gap=8, label_w=44, value_w=104):
    """Дві смуги порівняння (попередній/поточний)."""
    mx = max(prev or 0, curr or 0, 1)
    if prev is not None:
        _compare_row(draw, x, y, w, prev_lbl, fmt(prev), (prev or 0) / mx,
                     GREY, bar_h, label_w, value_w, faded=True)
    yy = y + bar_h + gap
    _compare_row(draw, x, yy, w, curr_lbl, fmt(curr), (curr or 0) / mx,
                 BLUE, bar_h, label_w, value_w)


# ──────────────────────────────────────────────────────────────────────────────
# БЛОКИ
# ──────────────────────────────────────────────────────────────────────────────
def _draw_header(draw, y, d):
    x = MX
    # аватар
    av = 52
    draw.ellipse([x, y, x + av, y + av], fill=INFO_BG)
    draw.text((x + av / 2, y + av / 2), d["initials"], font=F["avatar"],
              fill=INFO_TX, anchor="mm")
    tx = x + av + 16
    draw.text((tx, y + 6), "Аналітика за місяць", font=F["small"], fill=TEXT3, anchor="lm")
    draw.text((tx, y + 26), d["month_label"], font=F["month"], fill=TEXT1, anchor="lm")
    draw.text((tx, y + 48), d["nice_name"], font=F["sub"], fill=TEXT2, anchor="lm")


def _draw_hero(draw, y, d):
    x0, x1 = MX, W - MX
    draw.rounded_rectangle([x0, y, x1, y + HERO_H], radius=12, fill=SURFACE)
    cx = x0 + PADH
    cw = (x1 - x0) - 2 * PADH
    draw.text((cx, y + PADH + 7), "Дохід", font=F["label"], fill=TEXT2, anchor="lm")
    delta_txt, up = _pct_delta(d["income"]["curr"], d["income"]["prev"])
    if delta_txt:
        _pill(draw, x1 - PADH, y + PADH, delta_txt, up)
    draw.text((cx, y + PADH + 44), _fmt_usd(d["income"]["curr"]),
              font=F["big"], fill=TEXT1, anchor="lm")
    _two_bars(draw, cx, y + PADH + 74, cw,
              d["income"]["prev"], d["income"]["curr"],
              d["prev_short"], d["curr_short"], _fmt_usd, bar_h=12, gap=10)


def _draw_metric_card(draw, x0, y, cw, title, curr, prev, fmt, delta_fn,
                      note=None, prev_lbl="", curr_lbl=""):
    x1 = x0 + cw
    draw.rounded_rectangle([x0, y, x1, y + CARD_H], radius=12, fill=SURFACE)
    cx = x0 + PADC
    iw = cw - 2 * PADC
    draw.text((cx, y + PADC + 4), title, font=F["small"], fill=TEXT2, anchor="lm")
    # значення
    val_str = fmt(curr) if curr is not None else "—"
    draw.text((cx, y + PADC + 30), val_str, font=F["valsm"], fill=TEXT1, anchor="lm")
    # дельта
    if curr is not None:
        dt, up = delta_fn(curr, prev)
        if dt:
            _pill(draw, x1 - PADC, y + PADC + 18, dt, up)
    # порівняння / нотатка
    by = y + PADC + 52
    if note is not None:
        for line in _wrap(draw, note, F["tiny"], iw):
            draw.text((cx, by + 6), line, font=F["tiny"], fill=TEXT3, anchor="lm")
            by += 16
    elif curr is not None:
        _two_bars(draw, cx, by, iw, prev, curr, prev_lbl, curr_lbl, fmt,
                  bar_h=8, gap=8, label_w=0, value_w=78)


def _draw_grid(draw, y, d):
    cw = ((W - 2 * MX) - GRID_GAP) // 2
    xL, xR = MX, MX + cw + GRID_GAP
    pl, cl = d["prev_short"], d["curr_short"]
    # рядок 1: прибуток + маржа
    _draw_metric_card(draw, xL, y, cw, "Валовий прибуток",
                      d["gp"]["curr"], d["gp"]["prev"], _fmt_usd, _pct_delta,
                      prev_lbl=pl, curr_lbl=cl)
    _draw_metric_card(draw, xR, y, cw, "Маржинальність",
                      d["margin"]["curr"], d["margin"]["prev"], _fmt_pct, _pp_delta,
                      prev_lbl=pl, curr_lbl=cl)
    # рядок 2: угоди + середній чек
    y2 = y + CARD_H + GRID_GAP
    _draw_metric_card(draw, xL, y2, cw, "Кількість угод",
                      d["deals"]["curr"], d["deals"]["prev"],
                      lambda v: _fmt_num(v), _pct_delta,
                      prev_lbl=pl, curr_lbl=cl)
    _draw_metric_card(draw, xR, y2, cw, "Середній чек",
                      d["avg"]["curr"], d["avg"]["prev"], _fmt_usd, _pct_delta,
                      note=None if d["avg"]["curr"] is not None else "Угод не було",
                      prev_lbl=pl, curr_lbl=cl)


def _draw_bonus(draw, y, d):
    x0, x1 = MX, W - MX
    draw.rounded_rectangle([x0, y, x1, y + BONUS_H], radius=12, fill=AMBER_BG)
    cx = x0 + PADH
    draw.text((cx, y + BONUS_H / 2), "Бонуси (нараховано)",
              font=F["label"], fill=AMBER_TX, anchor="lm")
    val = _fmt_num(d["bonus"]["curr"])
    dt, up = _pct_delta(d["bonus"]["curr"], d["bonus"]["prev"])
    right = x1 - PADH
    if dt:
        pw = _pill(draw, right, y + BONUS_H / 2 - 12, dt, up)
        right = right - pw - 12
    draw.text((right, y + BONUS_H / 2), val, font=F["val"], fill=AMBER_TX, anchor="rm")


def _draw_hr(draw, y, d):
    cw = ((W - 2 * MX) - GRID_GAP) // 2
    chips = []
    if d["vacation"] > 0:
        chips.append(("Відпустка", f"{d['vacation']} дн"))
    if d["sick"] > 0:
        chips.append(("Лікарняні", f"{d['sick']} дн"))
    for i, (lbl, val) in enumerate(chips[:2]):
        x0 = MX + i * (cw + GRID_GAP)
        draw.rounded_rectangle([x0, y, x0 + cw, y + HR_H], radius=12, fill=SURFACE)
        draw.text((x0 + PADC, y + HR_H / 2 - 10), val, font=F["valsm"], fill=TEXT1, anchor="lm")
        draw.text((x0 + PADC, y + HR_H / 2 + 12), lbl, font=F["tiny"], fill=TEXT3, anchor="lm")


def _draw_insight(draw, y, d, max_w):
    draw.line([MX, y, W - MX, y], fill=BORDER, width=1)
    yy = y + 16
    for i, line in enumerate(_wrap(draw, d["insight"], F["insight"], max_w)):
        font = F["insightb"] if i == 0 else F["insight"]
        color = TEXT1 if i == 0 else TEXT2
        draw.text((MX, yy), line, font=font, fill=color, anchor="lm")
        yy += 22


# ──────────────────────────────────────────────────────────────────────────────
# РЕНДЕР ДАШБОРДУ
# ──────────────────────────────────────────────────────────────────────────────
def render_dashboard(d: dict) -> BytesIO:
    has_income = d["income"]["curr"] is not None and d["income"]["curr"] > 0
    has_bonus = d["bonus"]["curr"] is not None and abs(d["bonus"]["curr"]) > 0.01
    has_hr = d["vacation"] > 0 or d["sick"] > 0

    # попередній прохід — рахуємо висоту полотна
    tmp = ImageDraw.Draw(Image.new("RGB", (W, 10), WHITE))
    insight_lines = _wrap(tmp, d["insight"], F["insight"], W - 2 * MX)
    insight_h = 16 + len(insight_lines) * 22 + 6

    h = TOP + HEADER_H + GAP
    if has_income:
        h += HERO_H + GAP
        h += CARD_H + GRID_GAP + CARD_H + GAP
    if has_bonus:
        h += BONUS_H + GAP
    if has_hr:
        h += HR_H + GAP
    h += insight_h + BOTTOM

    img = Image.new("RGB", (W, h), WHITE)
    draw = ImageDraw.Draw(img)

    y = TOP
    _draw_header(draw, y, d); y += HEADER_H + GAP
    if has_income:
        _draw_hero(draw, y, d); y += HERO_H + GAP
        _draw_grid(draw, y, d); y += CARD_H + GRID_GAP + CARD_H + GAP
    if has_bonus:
        _draw_bonus(draw, y, d); y += BONUS_H + GAP
    if has_hr:
        _draw_hr(draw, y, d); y += HR_H + GAP
    _draw_insight(draw, y, d, W - 2 * MX)

    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf


# ──────────────────────────────────────────────────────────────────────────────
# ДАНІ
# ──────────────────────────────────────────────────────────────────────────────
def _subject_and_baseline(now: datetime):
    """M-1 (звітний, закритий місяць) та M-2 (база порівняння)."""
    y, m = now.year, now.month
    m1_m, m1_y = m - 1, y
    if m1_m == 0:
        m1_m, m1_y = 12, y - 1
    m2_m, m2_y = m1_m - 1, m1_y
    if m2_m == 0:
        m2_m, m2_y = 12, m1_y - 1
    return (m1_y, m1_m), (m2_y, m2_m)


def _bonus_accrual(employee: str, year: int, month: int) -> float:
    """Сума НАРАХУВАННЯ бонусів з рахунку 3330 за місяць (кредит, AmountCt)."""
    try:
        df = fetch_3330(employee, datetime(year, month, 1))
        if df is None or df.empty:
            return 0.0
        ct = pd.to_numeric(df.get("AmountCt", 0), errors="coerce").fillna(0.0)
        return float(ct[ct != 0].sum())
    except Exception as e:
        logging.warning(f"[push] bonus 3330 fail для {employee} {year}-{month:02d}: {e}")
        return 0.0


def _initials(nice_name: str) -> str:
    parts = [p for p in nice_name.replace(".", " ").split() if p]
    if not parts:
        return "—"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[1][0]).upper()


def _income_rank(year_breakdown: dict, income_curr: float) -> int | None:
    incomes = [v.get("income", 0) for v in year_breakdown.values() if v.get("income", 0) > 0]
    if income_curr <= 0 or len(incomes) < 2:
        return None
    return sum(1 for v in incomes if v > income_curr) + 1


def _build_insight(rank, margin_curr) -> str:
    parts = []
    if rank == 1:
        parts.append("Найкращий місяць року за доходом.")
    elif rank in (2, 3):
        parts.append(f"{rank}-й найкращий місяць року за доходом.")
    if margin_curr is not None and margin_curr < 20:
        parts.append("Маржа нижче 20% — варто переглянути угоди.")
    return " ".join(parts) if parts else "Стабільний місяць — без різких змін."


def _build_payload(employee: str, now: datetime, year_cache: dict) -> dict | None:
    (m1_y, m1_m), (m2_y, m2_m) = _subject_and_baseline(now)

    # розбивки по роках (з кешем у межах одного запуску для співробітника)
    def breakdown(year):
        key = (employee, year)
        if key not in year_cache:
            year_cache[key] = get_yearly_breakdown(employee, str(year)) or {}
        return year_cache[key]

    bd1 = breakdown(m1_y).get(m1_m)
    bd2 = breakdown(m2_y).get(m2_m)

    income_c = float(bd1["income"]) if bd1 else 0.0
    income_p = float(bd2["income"]) if bd2 else None
    gp_c = float(bd1["gross_profit"]) if bd1 else 0.0
    gp_p = float(bd2["gross_profit"]) if bd2 else None
    deals_c = int(bd1["deal_count"]) if bd1 else 0
    deals_p = int(bd2["deal_count"]) if bd2 else None

    margin_c = (gp_c / income_c * 100) if income_c else None
    margin_p = (gp_p / income_p * 100) if (income_p and gp_p is not None) else None
    avg_c = (income_c / deals_c) if deals_c else None
    avg_p = (income_p / deals_p) if (income_p and deals_p) else None

    bonus_c = _bonus_accrual(employee, m1_y, m1_m)
    bonus_p = _bonus_accrual(employee, m2_y, m2_m)

    # відпустки/лікарняні за звітний місяць
    vac = sick = 0
    if _fetch_yearly_data is not None:
        try:
            hr = _fetch_yearly_data(employee, m1_y) or {}
            hm = hr.get(m1_m, {})
            vac = int(hm.get("vac", 0)) + int(hm.get("lwp", 0))
            sick = int(hm.get("sick", 0))
        except Exception as e:
            logging.warning(f"[push] HR fail для {employee}: {e}")

    # --- рішення про відправку ---
    has_m1 = income_c > 0 or abs(bonus_c) > 0.01 or vac > 0 or sick > 0
    if not has_m1:
        logging.info(f"[push] {employee}: немає даних за {MONTHS_UA[m1_m-1]} — пропуск")
        return None
    has_m2 = (income_p and income_p > 0) or (bonus_p and abs(bonus_p) > 0.01)
    if not has_m2:
        logging.info(f"[push] {employee}: немає даних за {MONTHS_UA[m2_m-1]} (новачок) — пропуск")
        return None

    nice = display_name(employee)
    rank = _income_rank(breakdown(m1_y), income_c)

    return {
        "nice_name": nice,
        "initials": _initials(nice),
        "month_label": f"{MONTHS_UA[m1_m-1]} {m1_y}",
        "prev_short": MONTHS_UA_SHORT[m2_m - 1],
        "curr_short": MONTHS_UA_SHORT[m1_m - 1],
        "income": {"curr": income_c, "prev": income_p},
        "gp": {"curr": gp_c, "prev": gp_p},
        "margin": {"curr": margin_c, "prev": margin_p},
        "deals": {"curr": deals_c, "prev": deals_p},
        "avg": {"curr": avg_c, "prev": avg_p},
        "bonus": {"curr": bonus_c, "prev": bonus_p if (bonus_p and abs(bonus_p) > 0.01) else None},
        "vacation": vac,
        "sick": sick,
        "insight": _build_insight(rank, margin_c),
    }


# ──────────────────────────────────────────────────────────────────────────────
# ГОЛОВНА ТОЧКА ВХОДУ (виклик з планувальника)
# ──────────────────────────────────────────────────────────────────────────────
async def run_monthly_analytics_push():
    now = datetime.now(KYIV_TZ)
    (m1_y, m1_m), (m2_y, m2_m) = _subject_and_baseline(now)
    logging.info(f"[push] Старт: {MONTHS_UA[m1_m-1]} {m1_y} vs {MONTHS_UA[m2_m-1]} {m2_y}")

    users = get_active_users()
    if TEST_MODE:
        users = [u for u in users if u.get("telegram_id") in TEST_TELEGRAM_IDS]
        logging.info(f"[push] TEST_MODE: лише {len(users)} користувач(ів)")

    year_cache = {}
    sent = skipped = 0

    for u in users:
        tid = u.get("telegram_id")
        emp = u.get("employee_name")
        if not tid or not emp:
            continue
        try:
            payload = _build_payload(emp, now, year_cache)
            if payload is None:
                skipped += 1
                continue
            buf = render_dashboard(payload)
            caption = f"📊 <b>Аналітика за {payload['month_label']}</b>\n{payload['nice_name']}"
            await bot.send_photo(chat_id=tid, photo=buf, caption=caption, parse_mode="HTML")
            sent += 1
            logging.info(f"[push] ✅ Надіслано: {emp} (id={tid})")
        except Exception as e:
            logging.error(f"[push] ❌ Помилка для {emp} (id={tid}): {e}")

    logging.info(f"[push] Завершено. Надіслано: {sent}, пропущено: {skipped}")



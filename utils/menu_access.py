"""
Які розділи показувати в головному меню.

Раніше на кожне натискання меню летіло два запити до Power BI — окремо на
кожного користувача. Тепер один запит повертає *перелік усіх* співробітників,
у яких є дебіторка (відповідно аналітика), і цей перелік спільний для всіх:
перевірка доступу стає пошуком у множині, без жодного звернення до PBI.
"""

import asyncio
import logging
import time

import requests

from auth import get_power_bi_token

DATASET_ID = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
_PBI_URL = f'https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries'

# Дебіторка змінюється частіше за склад менеджерів у розрізі прибутку
_DEBT_TTL_SECONDS = 900        # 15 хв
_ANALYTICS_TTL_SECONDS = 21600  # 6 год

# Спільні на весь процес: {'people': set[str], 'ts': float}
_cache: dict[str, dict] = {}

# Дебіторка: хто взагалі має непроінформовані борги.
# CALCULATETABLE замість FILTER — фільтр іде в сховище (VertiPaq), а не в
# формульний рушій; SUMMARIZE по одній колонці — це дешевий DISTINCT.
_DAX_DEBT_PEOPLE = """
EVALUATE
DISTINCT(
  UNION(
    SELECTCOLUMNS(
      CALCULATETABLE(SUMMARIZE(Deb, Deb[Manager]), Deb[Inform] <> 1),
      "Person", Deb[Manager]
    ),
    SELECTCOLUMNS(
      CALCULATETABLE(SUMMARIZE(Deb, Deb[Seller]), Deb[Inform] <> 1),
      "Person", Deb[Seller]
    )
  )
)"""

# Аналітика: хто фігурує в угодах з 2025 року.
# RegistrDate >= DATE(...) замість YEAR(RegistrDate) >= 2025 — порівняння
# колонки з датою фільтрується в сховищі, а YEAR() рахується для кожного рядка.
_DAX_ANALYTICS_PEOPLE = """
EVALUATE
DISTINCT(
  UNION(
    SELECTCOLUMNS(
      CALCULATETABLE(
        SUMMARIZE('GrossProfitFromDeals', 'GrossProfitFromDeals'[Manager]),
        'GrossProfitFromDeals'[RegistrDate] >= DATE(2025, 1, 1)
      ),
      "Person", 'GrossProfitFromDeals'[Manager]
    ),
    SELECTCOLUMNS(
      CALCULATETABLE(
        SUMMARIZE('GrossProfitFromDeals', 'GrossProfitFromDeals'[Seller]),
        'GrossProfitFromDeals'[RegistrDate] >= DATE(2025, 1, 1)
      ),
      "Person", 'GrossProfitFromDeals'[Seller]
    )
  )
)"""


def _fetch_people(dax: str, label: str) -> set[str] | None:
    """Перелік імен з PBI. None — запит не вдався (порожній перелік ≠ помилка)."""
    token = get_power_bi_token()
    if not token:
        return None

    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    try:
        r = requests.post(_PBI_URL, headers=headers, json=payload, timeout=30)
    except Exception as e:
        logging.warning(f"menu_access [{label}]: виняток — {e}")
        return None

    if r.status_code != 200:
        logging.warning(f"menu_access [{label}]: PBI {r.status_code}: {r.text[:200]}")
        return None

    try:
        rows = r.json()['results'][0]['tables'][0].get('rows', [])
    except (ValueError, KeyError, IndexError) as e:
        logging.warning(f"menu_access [{label}]: неочікувана відповідь — {e}")
        return None

    people = {str(v).strip() for row in rows for v in row.values() if v}
    logging.info(f"menu_access [{label}]: у переліку {len(people)} співробітник(ів)")
    return people


def _people(kind: str, dax: str, ttl: int) -> set[str] | None:
    """Перелік із кешу; за потреби оновлює. None — даних ще немає взагалі."""
    entry = _cache.get(kind)
    if entry and time.monotonic() - entry['ts'] < ttl:
        return entry['people']

    people = _fetch_people(dax, kind)
    if people is None:
        # Запит упав — краще віддати трохи застарілий перелік, ніж сховати
        # у людини половину меню через хвилинний збій PBI
        if entry:
            logging.info(f"menu_access [{kind}]: PBI недоступний, лишаю попередній перелік")
            return entry['people']
        return None

    _cache[kind] = {'people': people, 'ts': time.monotonic()}
    return people


def check_analytics(employee_name: str) -> bool:
    people = _people('analytics', _DAX_ANALYTICS_PEOPLE, _ANALYTICS_TTL_SECONDS)
    # Даних немає — показуємо розділ. Сам хендлер однаково перевіряє доступ,
    # і краще зайва кнопка, ніж зникле меню через збій звітності.
    return True if people is None else employee_name in people


def check_debt(employee_name: str) -> bool:
    people = _people('debt', _DAX_DEBT_PEOPLE, _DEBT_TTL_SECONDS)
    return True if people is None else employee_name in people


def get_menu_access(context, employee_name: str) -> dict:
    """
    Доступи для меню. Блокуюча лише тоді, коли спливає TTL спільного кешу.

    З async-коду викликати через get_menu_access_async.
    """
    access = {
        'employee': employee_name,
        'analytics': check_analytics(employee_name),
        'debt': check_debt(employee_name),
    }
    logging.info(f"menu_access: {employee_name} → analytics={access['analytics']}, debt={access['debt']}")
    return access


async def get_menu_access_async(context, employee_name: str) -> dict:
    """Те саме, але в окремому потоці — event loop лишається вільним."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, get_menu_access, context, employee_name)


def refresh_menu_access_cache() -> None:
    """Прогрів кешу за розкладом, щоб черговий користувач не чекав на PBI."""
    _people('debt', _DAX_DEBT_PEOPLE, 0)
    _people('analytics', _DAX_ANALYTICS_PEOPLE, 0)

import asyncio
import logging
import time

import requests

from auth import get_power_bi_token

DATASET_ID = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
_PBI_URL = f'https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries'
_CACHE_KEY = 'menu_access'

# Скільки живе кеш дебіторки. Раніше check_debt бив у Power BI на кожне
# натискання меню — два HTTP-запити (токен + запит) у синхронному коді.
_DEBT_TTL_SECONDS = 600


def _pbi_count(dax: str) -> int:
    token = get_power_bi_token()
    if not token:
        return 0
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    try:
        r = requests.post(_PBI_URL, headers=headers, json=payload, timeout=15)
        if r.status_code != 200:
            logging.warning(f"menu_access PBI {r.status_code}")
            return 0
        rows = r.json()['results'][0]['tables'][0].get('rows', [])
        return int(rows[0].get('[C]', 0) or 0) if rows else 0
    except Exception as e:
        logging.warning(f"menu_access error: {e}")
        return 0


def check_analytics(employee_name: str) -> bool:
    emp = employee_name.replace('"', '""')
    dax = f"""
EVALUATE ROW(
  "C", COUNTROWS(
    FILTER(
      'GrossProfitFromDeals',
      ('GrossProfitFromDeals'[Manager] = "{emp}" || 'GrossProfitFromDeals'[Seller] = "{emp}") &&
      YEAR('GrossProfitFromDeals'[RegistrDate]) >= 2025
    )
  )
)"""
    return _pbi_count(dax) > 0


def check_debt(employee_name: str) -> bool:
    emp = employee_name.replace('"', '""')
    dax = f"""
EVALUATE ROW(
  "C", COUNTROWS(
    FILTER(
      Deb,
      (Deb[Manager] = "{emp}" || Deb[Seller] = "{emp}") && Deb[Inform] <> 1
    )
  )
)"""
    return _pbi_count(dax) > 0


def get_menu_access(context, employee_name: str) -> dict:
    """
    Доступи для меню. Блокуюча: ходить у Power BI.

    З async-коду викликати через get_menu_access_async, інакше замерзне
    увесь event loop і бот перестане відповідати всім користувачам.
    """
    cached = context.user_data.get(_CACHE_KEY) or {}
    fresh = cached.get('employee') == employee_name
    now = time.monotonic()

    if fresh and now - cached.get('debt_ts', 0) < _DEBT_TTL_SECONDS:
        return {'employee': employee_name,
                'analytics': cached['analytics'],
                'debt': cached['debt']}

    if fresh:
        analytics = cached['analytics']
    else:
        logging.info(f"menu_access: перевірка аналітики для {employee_name}")
        analytics = check_analytics(employee_name)

    debt = check_debt(employee_name)
    context.user_data[_CACHE_KEY] = {
        'employee': employee_name,
        'analytics': analytics,
        'debt': debt,
        'debt_ts': now,
    }
    logging.info(f"menu_access: analytics={analytics} (cached={fresh}), debt={debt}")
    return {'employee': employee_name, 'analytics': analytics, 'debt': debt}


async def get_menu_access_async(context, employee_name: str) -> dict:
    """Те саме, але в окремому потоці — event loop лишається вільним."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, get_menu_access, context, employee_name)

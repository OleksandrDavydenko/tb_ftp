import logging
import requests
from auth import get_power_bi_token

DATASET_ID = '8b80be15-7b31-49e4-bc85-8b37a0d98f1c'
_PBI_URL = f'https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries'
_CACHE_KEY = 'menu_access'


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
    cached = context.user_data.get(_CACHE_KEY)
    if cached and cached.get('employee') == employee_name:
        analytics = cached['analytics']
    else:
        logging.info(f"menu_access: перевірка аналітики для {employee_name}")
        analytics = check_analytics(employee_name)
        context.user_data[_CACHE_KEY] = {'employee': employee_name, 'analytics': analytics}

    # Дебіторка — без кешу, перевіряємо щоразу (дані часто змінюються)
    debt = check_debt(employee_name)
    logging.info(f"menu_access: analytics={analytics} (cached={bool(cached)}), debt={debt}")
    return {'employee': employee_name, 'analytics': analytics, 'debt': debt}

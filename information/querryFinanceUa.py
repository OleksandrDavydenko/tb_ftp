import os
import numpy as np
import logging
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from db import add_exchange_rate  # має кидати помилку або повертати True/False

# ===== Налаштування =====
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CHROME_PATH = "/app/.chrome-for-testing/chrome-linux64/chrome"
CHROMEDRIVER_PATH = "/app/.chrome-for-testing/chromedriver-linux64/chromedriver"

CURRENCIES = ["USD", "EUR", "PLN"]
TARGET_URL = "https://miniaylo.finance.ua"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_IDS = [
    "203148640",  # <-- замініть на chat_id Олександра Давиденка
    "225659191", # <-- замініть на chat_id Олександра Ступи
    "597086941"  
]

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.binary_location = CHROME_PATH


def send_telegram_alert(text: str) -> None:
    """Відправити сповіщення кільком адресатам. Якщо токена немає — просто залогуємо попередження."""
    if not TELEGRAM_BOT_TOKEN:
        logging.warning("TELEGRAM_BOT_TOKEN не заданий. Сповіщення не відправлено.")
        return

    for cid in CHAT_IDS:
        cid = str(cid).strip()
        if not cid or not cid.isdigit():
            logging.warning(f"Невалідний chat_id '{cid}'. Пропускаю.")
            continue
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data={"chat_id": cid, "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
            logging.info(f"Сповіщення відправлено chat_id={cid}")
        except Exception as e:
            logging.error(f"Не вдалося надіслати сповіщення chat_id={cid}: {e}")


def detect_and_remove_outliers(data):
    """ Визначає та видаляє викиди за допомогою IQR """
    if len(data) < 3:
        return data  # Якщо даних мало, повертаємо без змін

    Q1 = np.percentile(data, 25)
    Q3 = np.percentile(data, 75)
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    filtered_data = [x for x in data if lower_bound <= x <= upper_bound]
    return filtered_data if filtered_data else data  # Якщо всі викинуті, повертаємо вихідні дані


def parse_currency_table(currency_name, driver):
    """ Парсинг таблиці та очищення даних від викидів """
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'class': 'proposal-table'})
    if not table:
        logging.warning(f"Таблиця для {currency_name} не знайдена!")
        return None

    tbody = table.find('tbody')
    if not tbody:
        logging.warning(f"tbody для {currency_name} не знайдено!")
        return None

    rows = tbody.find_all('tr')
    prices = []
    for row in rows:
        cells = row.find_all('td')
        if len(cells) > 3:
            price_cell = cells[3].find('b')
            price = price_cell.text.strip() if price_cell else None
            if price:
                try:
                    prices.append(float(price.replace(',', '.')))
                except ValueError:
                    logging.error(f"Помилка обробки ціни для {currency_name}: {price}")

    if not prices:
        return None

    filtered_prices = detect_and_remove_outliers(prices)
    return max(filtered_prices) if filtered_prices else None


def store_exchange_rates():
    """ Збереження максимального очищеного курсу для кожної валюти.
        Якщо жодного запису не збережено — шлемо Telegram-алерт.
    """
    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)

    saved = {}   # { "USD": float|None, ... } — що реально записали
    errors = []  # список текстових помилок для діагностики у сповіщенні

    try:
        driver.get(TARGET_URL)
        time.sleep(5)

        # Закриваємо overlay, якщо є
        try:
            WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CLASS_NAME, "fc-dialog-overlay")))
            close_button = driver.find_element(By.CLASS_NAME, "fc-close")
            close_button.click()
            logging.info("Overlay знайдено і закрито.")
            time.sleep(1)
        except Exception:
            logging.info("Overlay не знайдено або вже закрито.")

        # Перелік вкладок валют
        currency_tabs = driver.find_elements(By.CSS_SELECTOR, "ul.currency-tab li[data-currency]")
        if not currency_tabs:
            msg = "Не знайдено вкладок з валютами (ul.currency-tab li[data-currency])."
            logging.warning(msg)
            errors.append(msg)

        tabs_by_code = {t.get_attribute("data-currency"): t for t in currency_tabs}

        for code in CURRENCIES:
            tab = tabs_by_code.get(code)
            if not tab:
                warn = f"Вкладка для {code} не знайдена на сторінці."
                logging.warning(warn)
                errors.append(warn)
                saved[code] = None
                continue

            try:
                logging.info(f"Перемикаємося на вкладку {code}")
                driver.execute_script("arguments[0].click();", tab)
                time.sleep(3)

                max_price = parse_currency_table(code, driver)
                if max_price is None:
                    warn = f"Для {code} не отримано жодної ціни."
                    logging.warning(warn)
                    errors.append(warn)
                    saved[code] = None
                    continue

                # Спроба запису в БД
                try:
                    res = add_exchange_rate(code, max_price)
                    # Якщо add_exchange_rate нічого не повертає — вважатимемо успіхом за відсутності виключення
                    ok = bool(res) if isinstance(res, bool) else True
                    if ok:
                        saved[code] = max_price
                        logging.info(f"Записано курс {code} = {max_price}")
                    else:
                        err = f"add_exchange_rate повернув False для {code}."
                        logging.error(err)
                        errors.append(err)
                        saved[code] = None
                except Exception as e:
                    err = f"Помилка запису в БД для {code}: {e}"
                    logging.error(err)
                    errors.append(err)
                    saved[code] = None

            except Exception as e:
                err = f"Помилка під час обробки {code}: {e}"
                logging.error(err)
                errors.append(err)
                saved[code] = None

    except Exception as e:
        err = f"Глобальна помилка парсингу валют: {e}"
        logging.error(err)
        errors.append(err)
    finally:
        driver.quit()

    # Підсумок: якщо жодної валюти не записали — Telegram-алерт
    saved_count = sum(1 for v in saved.values() if v is not None)
    if saved_count == 0:
        # Сформуємо коротке діагностичне повідомлення
        lines = [
            "<b>⚠️ Не записано курси валют у БД</b>",
            f"URL: {TARGET_URL}",
            "Що намагалися зберегти: " + ", ".join(CURRENCIES),
            "Результат: жодного запису.",
        ]
        if saved:
            details = "; ".join([f"{k}: {'OK ' + str(v) if v is not None else '—'}" for k, v in saved.items()])
            lines.append("Деталі: " + details)
        if errors:
            # Обрізаємо до розумної довжини, щоб не переливати логів у телеграм
            joined = "\n".join(errors)
            if len(joined) > 1200:
                joined = joined[:1200] + " …(обрізано)"
            lines.append("\n<code>" + joined + "</code>")

        send_telegram_alert("\n".join(lines))
    else:
        logging.info(f"Успішно збережено валют: {saved_count} / {len(CURRENCIES)}")

    return saved_count
        

if __name__ == "__main__":
    store_exchange_rates()

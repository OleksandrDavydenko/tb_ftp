from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from db import add_exchange_rate
import logging
import time

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Налаштування Selenium
CHROME_PATH = "/app/.chrome-for-testing/chrome-linux64/chrome"
CHROMEDRIVER_PATH = "/app/.chrome-for-testing/chromedriver-linux64/chromedriver"

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.binary_location = CHROME_PATH

def close_overlay(driver):
    """
    Закриває overlay, якщо воно є.
    """
    try:
        overlay = WebDriverWait(driver, 2).until(
            EC.presence_of_element_located((By.CLASS_NAME, "fc-dialog-overlay"))
        )
        close_button = driver.find_element(By.CLASS_NAME, "fc-close")
        close_button.click()
        logging.info("Overlay знайдено і закрито.")
        time.sleep(1)  # Затримка після закриття
    except Exception:
        logging.info("Overlay не знайдено або вже закрито.")

def parse_currency_table(currency_name, driver):
    """
    Парсинг таблиці для валюти.
    """
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'class': 'proposal-table'})
    if not table:
        logging.warning(f"Таблиця для {currency_name} не знайдена!")
        return None

    rows = table.find('tbody').find_all('tr')
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
    return max(prices) if prices else None

def click_tab(tab, driver):
    """
    Виконує клік по вкладці, використовуючи JavaScript, якщо звичайний клік не вдається.
    """
    try:
        tab.click()  # Спроба звичайного кліку
    except Exception:
        logging.warning(f"Не вдалося клікнути на вкладку {tab.get_attribute('data-currency')} звичайним методом, використовую JavaScript.")
        driver.execute_script("arguments[0].click();", tab)  # Примусовий клік через JavaScript

def store_exchange_rates():
    """
    Зберігає максимальні курси для кожної валюти (USD, EUR, PLN).
    """
    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get("https://miniaylo.finance.ua")
        time.sleep(5)

        # Закриваємо overlay, якщо є
        close_overlay(driver)

        # Знаходимо вкладки валют
        currency_tabs = driver.find_elements(By.CSS_SELECTOR, "ul.currency-tab li[data-currency]")

        for tab in currency_tabs:
            currency_name = tab.get_attribute("data-currency")
            if currency_name in ["USD", "EUR", "PLN"]:  # Обробляємо лише ці валюти
                try:
                    logging.info(f"Перемикаємося на вкладку {currency_name}")
                    click_tab(tab, driver)  # Використовуємо універсальний клік
                    time.sleep(3)  # Затримка для завантаження таблиці
                    max_price = parse_currency_table(currency_name, driver)
                    if max_price is not None:
                        add_exchange_rate(currency_name, max_price)
                        logging.info(f"Записано курс {currency_name} - {max_price}")
                except Exception as e:
                    logging.error(f"Помилка під час обробки {currency_name}: {e}")
    except Exception as e:
        logging.error(f"Глобальна помилка парсингу валют: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    store_exchange_rates()

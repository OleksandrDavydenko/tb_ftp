from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from db import add_exchange_rate  # Імпортуємо функцію для запису в БД
import logging
import time

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Налаштування Selenium
CHROME_PATH = "/app/.chrome-for-testing/chrome-linux64/chrome"
CHROMEDRIVER_PATH = "/app/.chrome-for-testing/chromedriver-linux64/chromedriver"

options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Без графічного інтерфейсу
options.add_argument('--disable-gpu')  # Вимикаємо GPU
options.add_argument('--no-sandbox')  # Вимикаємо ізоляцію (Heroku)
options.add_argument('--disable-dev-shm-usage')  # Вимикаємо загальний доступ до пам'яті
options.binary_location = CHROME_PATH  # Вказуємо шлях до Chrome

def parse_currency_table(currency_name, driver):
    """
    Парсинг таблиці для валюти та отримання максимального курсу.
    """
    try:
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

                try:
                    if price:
                        prices.append(float(price.replace(',', '.')))
                except ValueError:
                    logging.error(f"Помилка обробки ціни для {currency_name}: {price}")

        return max(prices) if prices else None
    except Exception as e:
        logging.error(f"Помилка парсингу таблиці для {currency_name}: {e}")
        return None

def store_exchange_rates():
    """
    Зберігає максимальні курси для кожної валюти (USD, EUR, PLN) у таблицю ExchangeRates.
    """
    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get("https://miniaylo.finance.ua")
        time.sleep(5)

        # Знаходимо вкладки валют
        currency_tabs = driver.find_elements(By.CSS_SELECTOR, "ul.currency-tab li[data-currency]")

        for tab in currency_tabs:
            currency_name = tab.get_attribute("data-currency")
            if currency_name in ["USD", "EUR", "PLN"]:  # Обробляємо лише ці валюти
                try:
                    tab.click()  # Клікаємо на вкладку валюти
                    time.sleep(2)

                    # Парсимо курс для поточної валюти
                    max_price = parse_currency_table(currency_name, driver)
                    if max_price is not None:
                        # Записуємо курс у базу даних
                        add_exchange_rate(currency_name, max_price)
                        logging.info(f"Записано курс {currency_name} - {max_price}")
                except Exception as e:
                    logging.error(f"Помилка під час обробки {currency_name}: {e}")

    except Exception as e:
        logging.error(f"Глобальна помилка парсингу валют: {e}")
    finally:
        driver.quit()

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

def click_tab_with_retry(tab, driver, currency_name):
    """
    Клікає по вкладці з кількома спробами.
    """
    for attempt in range(3):  # Робимо до 3 спроб
        try:
            # Скролимо до елемента, щоб уникнути помилок click intercepted
            ActionChains(driver).move_to_element(tab).perform()
            tab.click()
            time.sleep(5)  # Додано затримку в 5 секунд після кліку
            logging.info(f"Успішно переключилися на вкладку {currency_name}.")
            return True
        except Exception as e:
            logging.warning(f"Спроба {attempt + 1} для {currency_name} не вдалася: {e}")
            time.sleep(2)
    logging.error(f"Не вдалося переключитися на вкладку {currency_name} після кількох спроб.")
    return False

def store_exchange_rates():
    """
    Зберігає максимальні курси для кожної валюти (USD, EUR, PLN) у таблицю ExchangeRates.
    """
    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get("https://miniaylo.finance.ua")
        time.sleep(5)  # Чекаємо, поки сторінка завантажиться

        # Знаходимо вкладки валют
        currency_tabs = driver.find_elements(By.CSS_SELECTOR, "ul.currency-tab li[data-currency]")

        for tab in currency_tabs:
            currency_name = tab.get_attribute("data-currency")
            if currency_name in ["USD", "EUR", "PLN"]:  # Обробляємо лише ці валюти
                if click_tab_with_retry(tab, driver, currency_name):
                    max_price = parse_currency_table(currency_name, driver)
                    if max_price is not None:
                        add_exchange_rate(currency_name, max_price)
                        logging.info(f"Записано курс {currency_name} - {max_price}")
    except Exception as e:
        logging.error(f"Глобальна помилка парсингу валют: {e}")
    finally:
        driver.quit()

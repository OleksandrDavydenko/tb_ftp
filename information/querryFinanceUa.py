from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Налаштування Selenium для Heroku
CHROME_PATH = "/app/.chrome-for-testing/chrome-linux64/chrome"
CHROMEDRIVER_PATH = "/app/.chrome-for-testing/chromedriver-linux64/chromedriver"

options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Запуск без графічного інтерфейсу
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')  # Вимикаємо ізоляцію (Heroku)
options.add_argument('--disable-dev-shm-usage')  # Вимикаємо загальний доступ до пам'яті
options.binary_location = CHROME_PATH  # Шлях до Chrome

# Ініціалізація драйвера
driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, options=options)

# Словник для збереження даних
parsed_data = {}

def parse_currency_table(currency_name):
    """Парсинг таблиці після вибору валюти."""
    html = driver.page_source

    # Парсимо HTML через BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Знаходимо таблицю
    table = soup.find('table', {'class': 'proposal-table'})
    if not table:
        logging.warning(f"Таблиця для валюти {currency_name} не знайдена!")
        return

    logging.info(f"Таблиця для валюти {currency_name} знайдена, обробляємо дані...")
    rows = table.find('tbody').find_all('tr')

    # Ініціалізація для поточної валюти
    if currency_name not in parsed_data:
        parsed_data[currency_name] = []

    # Обробка рядків таблиці
    for row in rows:
        cells = row.find_all('td')
        if len(cells) > 3:
            time_cell = cells[0].text.strip()  # Час
            direction_cell = cells[1].text.strip()  # Напрямок (купівля/продаж)
            currency_span = cells[2].find('span')
            currency = currency_span.text.strip() if currency_span else "Немає валюти"  # Валюта
            price_cell = cells[3].find('b')
            price = price_cell.text.strip() if price_cell else "Немає даних"  # Курс

            # Додавання даних у словник
            parsed_data[currency_name].append({
                "time": time_cell,
                "direction": direction_cell,
                "currency": currency,
                "price": price
            })
            logging.info(f'Час: {time_cell}, Напрямок: {direction_cell}, Валюта: {currency}, Курс: {price}')

def process_parsed_data(data):
    """Обробка зібраних даних для визначення максимального курсу продажу."""
    processed_data = {}

    for currency, entries in data.items():
        sell_prices = []

        for entry in entries:
            try:
                price = float(entry['price'].replace(',', '.'))  # Конвертація в float
                if 'продам' in entry['direction'].lower():
                    sell_prices.append(price)
            except ValueError:
                logging.warning(f"Помилка обробки курсу: {entry['price']} для валюти {currency}")

        # Обчислення максимального продажу
        max_sell = max(sell_prices) if sell_prices else None

        processed_data[currency] = {
            "max_sell": max_sell
        }

    return processed_data

try:
    logging.info("Відкриваємо сторінку через Selenium...")
    driver.get("https://miniaylo.finance.ua")

    # Чекаємо завантаження сторінки
    time.sleep(5)

    # Знаходимо список валют і клікаємо по кожній
    currency_tabs = driver.find_elements(By.CSS_SELECTOR, "ul.currency-tab li[data-currency]")

    for tab in currency_tabs:
        # Отримуємо назву валюти
        currency_name = tab.get_attribute("data-currency")
        logging.info(f"\nПеремикаємося на валюту: {currency_name}")

        # Клікаємо по вкладці
        tab.click()

        # Чекаємо завантаження нових даних
        time.sleep(2)

        # Парсимо таблицю для цієї валюти
        parse_currency_table(currency_name)

    # Обробка даних
    processed_data = process_parsed_data(parsed_data)

    # Вивід оброблених даних
    logging.info("\nОброблені дані:")
    for currency, data in processed_data.items():
        logging.info(f"{currency}: Максимальний курс продажу: {data['max_sell']}")

except Exception as e:
    logging.error(f"Виникла помилка: {e}")
finally:
    driver.quit()

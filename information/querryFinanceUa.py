from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.base import ConflictingIdError
from pytz import timezone
import time
from datetime import datetime
from ..database import add_exchange_rate  # Імпортуємо функцію для запису в БД

# Налаштування Selenium
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Запуск без графічного інтерфейсу
options.add_argument('--disable-gpu')
driver = webdriver.Chrome(options=options)  # Переконайтеся, що ChromeDriver встановлений

def parse_currency_table(currency_name):
    """Парсинг таблиці для валюти та отримання максимального курсу."""
    # Отримуємо HTML-код сторінки
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    # Знаходимо таблицю
    table = soup.find('table', {'class': 'proposal-table'})
    if not table:
        return None

    rows = table.find('tbody').find_all('tr')
    prices = []

    # Обробка рядків таблиці
    for row in rows:
        cells = row.find_all('td')
        if len(cells) > 3:
            price_cell = cells[3].find('b')
            price = price_cell.text.strip() if price_cell else None  # Курс

            try:
                if price:
                    prices.append(float(price.replace(',', '.')))
            except ValueError:
                continue

    # Повертаємо максимальний курс
    return max(prices) if prices else None

def store_exchange_rates():
    """Зберігає максимальні курси для кожної валюти у таблицю ExchangeRates."""
    try:
        # Відкриваємо сторінку
        driver.get("https://miniaylo.finance.ua")
        time.sleep(5)  # Чекаємо завантаження сторінки

        # Знаходимо список валют
        currency_tabs = driver.find_elements(By.CSS_SELECTOR, "ul.currency-tab li[data-currency]")

        for tab in currency_tabs:
            # Отримуємо назву валюти
            currency_name = tab.get_attribute("data-currency")

            # Клікаємо по вкладці
            tab.click()
            time.sleep(2)  # Чекаємо завантаження нових даних

            # Парсимо таблицю для цієї валюти
            max_price = parse_currency_table(currency_name)

            # Якщо знайдено курс, додаємо до таблиці
            if max_price is not None:
                timestamp = datetime.now()  # Поточний час
                add_exchange_rate(currency_name, max_price)  # Запис у базу даних
                print(f"{timestamp}: Записано курс {currency_name} - {max_price}")
    except Exception as e:
        print(f"Виникла помилка під час обробки валют: {e}")
    finally:
        driver.quit()

# Налаштування планувальника завдань
scheduler = BlockingScheduler()

# Часовий пояс Києва
kyiv_timezone = timezone('Europe/Kiev')

try:
    # Додаємо завдання, яке запускає `store_exchange_rates` щодня о 10:00 ранку за київським часом
    scheduler.add_job(
        store_exchange_rates,
        'cron',
        hour=17,
        minute=0,
        timezone=kyiv_timezone,
        id='daily_exchange_rates'
    )
except ConflictingIdError:
    print("Завдання з таким ID вже існує.")

if __name__ == "__main__":
    print("Планувальник завдань запущено. Завдання: запис курсів валют щодня о 10:00 (за київським часом).")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Планувальник зупинено.")

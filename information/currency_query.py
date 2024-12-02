import sqlite3

def get_latest_currency_rates():
    """
    Повертає останні курси USD і EUR з бази даних.
    """
    connection = sqlite3.connect("your_database.db")  # Замініть на шлях до вашої бази даних
    cursor = connection.cursor()

    try:
        cursor.execute("""
            SELECT currency, rate, timestamp
            FROM ExchangeRates
            WHERE currency IN ('USD', 'EUR')
            ORDER BY timestamp DESC
        """)
        results = cursor.fetchall()
        
        # Розподіляємо результати по валютах
        usd_rate = next((row for row in results if row[0] == "USD"), None)
        eur_rate = next((row for row in results if row[0] == "EUR"), None)
        return {"rate": usd_rate[1], "timestamp": usd_rate[2]} if usd_rate else None, \
               {"rate": eur_rate[1], "timestamp": eur_rate[2]} if eur_rate else None
    finally:
        connection.close()

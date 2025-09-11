import mysql.connector
from config import DB_CONFIG


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

# GET portolio list
def get_portfolio():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM portfolio")
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tickers


# GET latest close price of ticker
def get_latest_price(ticker):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT date, close FROM stock_{ticker}
        ORDER BY date DESC LIMIT 1
    """)
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result


# GET ALL latest prices of tickers in portfolio
def get_all_latest_prices():
    prices = {}
    for ticker in get_portfolio():
        prices[ticker] = get_latest_price(ticker)
        
     
# if __name__ == "__main__":
#     print(get_all_latest_prices())
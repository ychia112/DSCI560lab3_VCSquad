import mysql.connector
import pandas as pd
from config import DB_CONFIG


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

# portfolio actions
# GET portfolio list
def get_portfolio():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM portfolio")
    result = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return result

# Insert stock into portfolio
def add_stock(ticker):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT IGNORE INTO portfolio (ticker) VALUES (%s)", (ticker,))
    conn.commit()
    cursor.close()
    conn.close()

# Delete stock from portfolio
def remove_stock(ticker):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM portfolio WHERE ticker=%s", (ticker,))
    conn.commit()
    cursor.close()
    conn.close()


# Searching
# GET whole stock_prices table
def get_all_data():
    conn = get_connection()
    query = """
        SELECT ticker, date, open, high, low, close, volume
        FROM stock_prices
        ORDER BY ticker, date
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# GET latest close price of ticker
def get_latest_price(ticker):
    conn = get_connection()
    query = """
        SELECT date, close
        FROM stock_prices
        WHERE ticker=%s
        ORDER BY date DESC LIMIT 1
    """
    df = pd.read_sql(query, conn, params=[ticker])
    conn.close()
    return df


# GET specific one stock history data
def get_stock_history(ticker, start=None, end=None):
    conn = get_connection()
    query = """
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker=%s
    """
    params = [ticker]

    if start and end:
        query += " AND date BETWEEN %s AND %s"
        params.extend([start, end])
    elif start:
        query += " AND date >= %s"
        params.append(start)
    elif end:
        query += " AND date <= %s"
        params.append(end)

    query += " ORDER BY date ASC"

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


# GET ALL latest prices of tickers in portfolio
def get_all_latest_prices():
    conn = get_connection()
    query = """
        SELECT t1.ticker, t1.date, t1.close
        FROM stock_prices t1
        INNER JOIN (
            SELECT ticker, MAX(date) as latest_date
            FROM stock_prices
            GROUP BY ticker
        ) t2
        ON t1.ticker = t2.ticker AND t1.date = t2.latest_date
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df
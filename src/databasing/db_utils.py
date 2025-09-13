import mysql.connector
import pandas as pd
from mysql.connector import Error
from config import DB_CONFIG


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

# portfolio actions
# GET portfolio list
def get_portfolio():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM portfolio", conn)
    conn.close()
    return df

# Insert stock into portfolio
def add_stock(ticker):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT IGNORE INTO portfolio (ticker) VALUES (%s)", (ticker,))
        conn.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

# Delete stock from portfolio
def remove_stock(ticker):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM portfolio WHERE ticker = %s", (ticker,))
        conn.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()


# Searching
# GET whole stock_prices table
def get_all_data():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM stock_prices ORDER BY ticker, dt", conn)
    conn.close()
    return df

# GET latest close price of ticker
def get_latest_price(ticker):
    conn = get_connection()
    query = """
        SELECT dt, close
        FROM stock_prices
        WHERE ticker=%s
        ORDER BY dt DESC LIMIT 1
    """
    df = pd.read_sql(query, conn, params=[ticker])
    conn.close()
    return df


# GET specific one stock history data
def get_stock_history(ticker: str, start_date: str = None, end_date: str = None):
    conn = get_connection()
    query = """
        SELECT ticker, dt, open, high, low, close, adj_close, volume, `interval`
        FROM stock_prices
        WHERE ticker = %s
    """
    params = [ticker]

    if start_date and end_date:
        query += " AND dt BETWEEN %s AND %s"
        params.extend([start_date, end_date])
    elif start_date:
        query += " AND dt >= %s"
        params.append(start_date)
    elif end_date:
        query += " AND dt <= %s"
        params.append(end_date)

    query += " ORDER BY dt ASC"

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


# GET ALL latest prices of tickers in portfolio
def get_all_latest_prices():
    conn = get_connection()
    query = """
        SELECT t1.ticker, t1.dt, t1.close, t1.adj_close, t1.volume
        FROM stock_prices t1
        INNER JOIN (
            SELECT ticker, MAX(dt) as latest_date
            FROM stock_prices
            GROUP BY ticker
        ) t2
        ON t1.ticker = t2.ticker AND t1.dt = t2.latest_date
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple, Any
import mysql.connector as mysql
from mysql.connector import MySQLConnection, CMySQLConnection, Error  # type: ignore
from dotenv import load_dotenv
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# Load .env if present
load_dotenv()

# ---------- Configuration ----------

@dataclass(frozen=True)
class DBConfig:
    host: str = os.getenv("MYSQL_HOST", "localhost")
    port: int = int(os.getenv("MYSQL_PORT", "3306"))
    user: str = os.getenv("MYSQL_USER", "root")
    password: str = os.getenv("MYSQL_PASSWORD", "")
    database: str = os.getenv("MYSQL_DB", "stocks_db")

def connect_db(cfg: Optional[DBConfig] = None) -> MySQLConnection:
    """
    Create a MySQL connection using the provided DBConfig (or env defaults).
    Caller is responsible for closing the connection.
    """
    cfg = cfg or DBConfig()
    return mysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        autocommit=False,
    )

# ---------- Shared SQL Snippets ----------

# Reusable UPSERT for stock_prices table
UPSERT_STOCK_PRICES_SQL = """
INSERT INTO stock_prices
(ticker, dt, `open`, high, low, `close`, adj_close, volume, `interval`)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  `open`=VALUES(`open`),
  `high`=VALUES(`high`),
  `low`=VALUES(`low`),
  `close`=VALUES(`close`),
  `adj_close`=VALUES(`adj_close`),
  `volume`=VALUES(`volume`);
"""

SQL_GET_LAST_DT = """
SELECT MAX(dt) FROM stock_prices WHERE ticker=%s AND `interval`=%s
"""

# ---------- Small utility helpers ----------

def exec_many(
    conn: MySQLConnection,
    sql: str,
    rows: Sequence[Sequence[Any]],
    batch_size: int = 2000,
) -> int:
    """
    Execute executemany in batches. Returns total affected rows (input length).
    Rolls back and re-raises on error.
    """
    if not rows:
        return 0
    cur = conn.cursor()
    total = 0
    try:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            cur.executemany(sql, chunk)
            conn.commit()
            total += len(chunk)
        return total
    except mysql.Error:
        conn.rollback()
        raise
    finally:
        cur.close()

def fetch_scalar(conn: MySQLConnection, sql: str, params: Sequence[Any] = ()) -> Any:
    """
    Run a SELECT that returns a single value (first column of first row).
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()

def fetch_all(conn: MySQLConnection, sql: str, params: Sequence[Any] = ()) -> List[tuple]:
    """
    Run a SELECT and return all rows as list of tuples.
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return list(cur.fetchall())
    finally:
        cur.close()

def get_last_dt(conn: MySQLConnection, ticker: str, interval: str) -> Optional[Any]:
    """
    Convenience wrapper around SQL_GET_LAST_DT.
    Returns Python datetime or None.
    """
    return fetch_scalar(conn, SQL_GET_LAST_DT, (ticker, interval))

# ---------- Domain-specific row builder (optional but handy) ----------

def build_stock_price_rows(df) -> List[tuple]:
    """
    Convert a tidy pandas DataFrame with columns:
      ['ticker','dt','open','high','low','close','adj_close','volume','interval']
    into a list of tuples for UPSERT_STOCK_PRICES_SQL.

    Note: This function is optional—use it if you prefer a common row format builder.
    """
    import pandas as pd  # local import to keep this module lightweight if pandas isn't installed elsewhere

    rows: List[tuple] = []
    for r in df.itertuples(index=False):
        # Ensure naive datetime for MySQL DATETIME
        dt = pd.Timestamp(r.dt).to_pydatetime().replace(tzinfo=None)
        rows.append(
            (
                r.ticker,
                dt,
                None if pd.isna(r.open) else float(r.open),
                None if pd.isna(r.high) else float(r.high),
                None if pd.isna(r.low) else float(r.low),
                None if pd.isna(r.close) else float(r.close),
                None if pd.isna(r.adj_close) else float(r.adj_close),
                None if pd.isna(r.volume) else int(r.volume),
                r.interval,
            )
        )
    return rows

# portfolio actions
# GET portfolio list
def get_portfolio():
    conn = connect_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM portfolio")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(rows)

# Insert stock into portfolio
def add_stock(ticker):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM stock_prices WHERE ticker=%s", (ticker,))
        exists = cursor.fetchone()[0] > 0
        if not exists:
            print(f"{ticker} not found in stock_prices, cannot add.")
            return

        cursor.execute("INSERT IGNORE INTO portfolio (ticker) VALUES (%s)", (ticker,))
        conn.commit()
        print(f"Added {ticker} to portfolio")
    except Error as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

# Delete stock from portfolio
def remove_stock(ticker):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM portfolio WHERE ticker=%s", (ticker,))
        exists = cursor.fetchone()[0] > 0
        if not exists:
            print(f"{ticker} not found in portfolio, nothing removed.")
            return

        cursor.execute("DELETE FROM portfolio WHERE ticker = %s", (ticker,))
        conn.commit()
        print(f"Removed {ticker} from portfolio")
    except Error as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()


# Searching
# GET whole stock_prices table
def get_all_data():
    conn = connect_db()
    df = pd.read_sql("SELECT * FROM stock_prices ORDER BY ticker, dt", conn)
    conn.close()
    return df

# GET latest close price of ticker
def get_latest_price(ticker):
    conn = connect_db()
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
    conn = connect_db()
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
    conn = connect_db()
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
import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG

def insert_dataframe(df):
    """_summary_
    Insert the data we fetch from yfinance into database (stock_prices)

    Args:
        df (pd.DataFrame): input dataframe, columns needs to contain ticker, dt, open, high, low, close, volume.
    """
    
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stock_prices 
                    (ticker, dt, open, high, low, close, adj_close, volume, `interval`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open=VALUES(open),
                    high=VALUES(high),
                    low=VALUES(low),
                    close=VALUES(close),
                    adj_close=VALUES(adj_close),
                    volume=VALUES(volume)
            """, (
                row["ticker"],
                row["dt"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["adj_close"],
                row["volume"],
                row.get("interval", "1d")
            ))

        conn.commit()
        
        print(f"Data inserted successfully!")
    
    except Error as e:
        print(f"Database error: {e}")
        
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
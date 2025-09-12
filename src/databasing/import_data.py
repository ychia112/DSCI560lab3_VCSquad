import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG

def insert_dataframe(df):
    """_summary_
    Usage: insert the data we fetch from yfinance into database (stock_prices)

    Args:
        df (pd.DataFrame): input dataframe, columns needs to contain ticker, date, open, high, low, close, volume.
    """
    
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open=VALUES(open),
                    high=VALUES(high),
                    low=VALUES(low),
                    close=VALUES(close),
                    volume=VALUES(volume)
            """, (
                row["ticker"],
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"]
            ))

        conn.commit()
        
        print(f"Data inserted successfully!")
    
    except Error as e:
        print(f"Database error: {e}")
        
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
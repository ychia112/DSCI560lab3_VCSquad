import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG


def create_stock_table(cursor, ticker):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS stock_{ticker} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            open DECIMAL(10, 2),
            high DECIMAL(10, 2),
            low DECIMAL(10, 2),
            close DECIMAL(10, 2),
            volume BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_price (date)
        )
    """)
    
    
# insert csv
def insert_stock_csv(cursor, ticker, csv_path):
    df = pd.read_csv(csv_path)
    df.columns = [col.strip().capitalize() for col in df.columns]
    df['Date'] = pd.to_datetime(df['Date']).dt.date

    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO stock_{ticker} (date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                open=VALUES(open),
                high=VALUES(high),
                low=VALUES(low),
                close=VALUES(close),
                volume=VALUES(volume)
        """, (
            row['Date'],
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Volume']
        ))
        
        
# Bulk inport
def bulk_import(folder="./csv_files"): # raw data folder
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        if not os.path.exists(folder):
            print(f"Folder not found: {folder}")
            return

        files = [f for f in os.listdir(folder) if f.endswith(".csv")]
        if not files:
            print(f"No CSV files found in {folder}")
            return

        for file in files:
            ticker = file.split(".")[0].upper()
            csv_path = os.path.join(folder, file)

            print(f"Importing {ticker} from {file} ...")
            create_stock_table(cursor, ticker)
            insert_stock_csv(cursor, ticker, csv_path)
            cursor.execute(
                "INSERT IGNORE INTO portfolio_stocks (ticker) VALUES (%s)",
                (ticker,)
            )
            print(f"Finished importing {ticker}")

        conn.commit()
        print("\nAll CSV files have been imported successfully!")

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            
            
# if __name__ == "__main__":
#     bulk_import("./csv_files")
import argparse
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

from .db_utils import (
    DBConfig,
    connect_db,
    UPSERT_STOCK_PRICES_SQL,
    get_last_dt,
    exec_many,
    build_stock_price_rows,
)


def parse_args():
    ap = argparse.ArgumentParser(
        description="Update stock_prices table with the latest data from yfinance."
    )
    ap.add_argument("--tickers", nargs="+", required=True, help="List of tickers (e.g., AAPL MSFT)")
    ap.add_argument(
        "--interval",
        type=str,
        default="1d",
        choices=["1m","2m","5m","15m","30m","60m","90m","1h","1d","5d","1wk","1mo","3mo"],
        help="Data frequency / interval"
    )
    return ap.parse_args()


def tidy(df1: pd.DataFrame, tk: str, interval: str) -> pd.DataFrame:
    """
    Convert a single yfinance DataFrame into tidy format for DB insertion.
    """
    if df1.empty:
        return pd.DataFrame(columns=["ticker","dt","open","high","low","close","adj_close","volume","interval"])
    out = df1.reset_index().rename(columns={
        "Date": "dt", "Datetime": "dt",
        "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Adj Close": "adj_close", "Volume": "volume"
    })
    out["ticker"] = tk
    out["interval"] = interval
    if pd.api.types.is_datetime64tz_dtype(out["dt"]):
        out["dt"] = out["dt"].dt.tz_convert(None)
    return out[["ticker","dt","open","high","low","close","adj_close","volume","interval"]]


def main():
    args = parse_args()
    cfg = DBConfig()
    conn = connect_db(cfg)

    total = 0
    try:
        for tk in args.tickers:
            # Find the last datetime stored for this ticker/interval
            last_dt = get_last_dt(conn, tk, args.interval)
            start = (last_dt + timedelta(seconds=1)) if last_dt else datetime(2015,1,1)
            end = datetime.today()

            if start >= end:
                print(f"[skip] {tk}: up-to-date (last_dt={last_dt})")
                continue

            # Fetch new data from yfinance
            raw = yf.Ticker(tk).history(
                start=start, end=end, interval=args.interval, auto_adjust=False
            )
            df = tidy(raw, tk, args.interval)
            if df.empty:
                print(f"[warn] {tk}: no new rows fetched.")
                continue

            # Convert to row tuples and upsert into DB
            rows = build_stock_price_rows(df)
            inserted = exec_many(conn, UPSERT_STOCK_PRICES_SQL, rows, batch_size=2000)
            total += inserted
            print(f"[ok] {tk}: upserted {inserted} rows")

        print(f"[done] Total upserted rows: {total}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

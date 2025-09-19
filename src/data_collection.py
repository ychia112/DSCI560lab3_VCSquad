from __future__ import annotations
import os
import json
import argparse
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

from .db_utils import (
    DBConfig,
    connect_db,
    UPSERT_STOCK_PRICES_SQL,
    exec_many,
    build_stock_price_rows,
)

load_dotenv()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Fetch stock prices with yfinance and load into MySQL (optionally export to CSV)."
    )
    ap.add_argument("--config", type=str, default=None, help="Path to seed_config.json")
    ap.add_argument("--tickers", nargs="+", default=None, help="List of tickers (e.g., AAPL MSFT)")
    ap.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (yfinance end may be exclusive)")
    ap.add_argument(
        "--interval",
        type=str,
        default=None,
        choices=["1m","2m","5m","15m","30m","60m","90m","1h","1d","5d","1wk","1mo","3mo"],
        help="Data frequency / interval"
    )
    ap.add_argument("--validate", action="store_true", help="Validate tickers on yfinance before fetching")
    ap.add_argument("--export-csv", type=str, default=None, help="Optional CSV output path")
    return ap.parse_args()


def load_seed(args: argparse.Namespace) -> Tuple[List[str], str, str, str, bool]:
    """Load defaults from config file (if any) and merge with CLI arguments."""
    seed = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, "r") as f:
            seed = json.load(f)

    tickers = args.tickers or seed.get("tickers", ["GOOG","AMZN","AAPL","MSFT","NFLX"])
    start = args.start or seed.get("start", "2015-01-01")
    end = args.end or seed.get("end", datetime.today().strftime("%Y-%m-%d"))
    interval = args.interval or seed.get("interval", "1d")
    validate = args.validate or bool(seed.get("validate", False))
    return [t.strip().upper() for t in tickers], start, end, interval, validate


def validate_ticker(ticker: str) -> bool:
    """Check if a ticker has recent data available on yfinance."""
    try:
        df = yf.Ticker(ticker).history(period="5d")
        return not df.empty
    except Exception:
        return False


def tidy_from_yf(raw, interval: str, tickers: List[str]) -> pd.DataFrame:
    """
    Convert yfinance output (single or multi-ticker) into a tidy DataFrame:
    ['ticker','dt','open','high','low','close','adj_close','volume','interval']
    """
    frames = []

    def tidy_one(df1: pd.DataFrame, tk: str) -> pd.DataFrame:
        if df1 is None or df1.empty:
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

    if hasattr(raw, "columns") and isinstance(raw.columns, pd.MultiIndex):
        for tk in tickers:
            if tk in raw.columns.levels[0]:
                frames.append(tidy_one(raw[tk].copy(), tk))
            else:
                print(f"[warn] Ticker {tk} not in yfinance response.")
    else:
        tk = tickers[0] if tickers else ""
        frames.append(tidy_one(getattr(raw, "copy", lambda: raw)(), tk))

    if not frames:
        return pd.DataFrame(columns=["ticker","dt","open","high","low","close","adj_close","volume","interval"])

    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["dt"])
    df = df.drop_duplicates(subset=["ticker","dt","interval"]).sort_values(["ticker","dt"]).reset_index(drop=True)

    for col in ["open","high","low","close","adj_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    return df


def main() -> None:
    args = parse_args()
    tickers, start, end, interval, do_validate = load_seed(args)

    if do_validate:
        v, inv = [], []
        for t in tickers:
            (v if validate_ticker(t) else inv).append(t)
        if inv:
            print(f"[warn] Invalid/empty on yfinance (skipped): {inv}")
        tickers = v
        if not tickers:
            print("[exit] No valid tickers. Nothing to fetch.")
            return

    print(f"[info] Downloading {tickers}  {start}→{end}  interval={interval}")
    raw = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        group_by="ticker",
        threads=True,
        progress=False,
    )

    df = tidy_from_yf(raw, interval, tickers)
    if df.empty:
        print("[exit] No data returned.")
        return

    if args.export_csv:
        os.makedirs(os.path.dirname(args.export_csv), exist_ok=True)
        df.to_csv(args.export_csv, index=False)
        print(f"[info] CSV exported: {args.export_csv}")

    cfg = DBConfig()
    conn = connect_db(cfg)
    try:
        rows = build_stock_price_rows(df)
        n = exec_many(conn, UPSERT_STOCK_PRICES_SQL, rows, batch_size=2000)
        print(f"[done] Upserted rows: {n}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional
from .db_utils import connect_db, DBConfig


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run SMA crossover strategy on stock data"
    )
    
    # Ticker selection
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--tickers", 
        nargs="+", 
        help="List of specific tickers to analyze (e.g., AAPL GOOG MSFT)"
    )
    group.add_argument(
        "--portfolio", 
        action="store_true", 
        help="Use all tickers from the portfolio table"
    )
    
    # Date range
    parser.add_argument(
        "--start", 
        type=str, 
        default="2022-01-01",
        help="Start date (YYYY-MM-DD) [default: 2022-01-01]"
    )
    parser.add_argument(
        "--end", 
        type=str, 
        default=datetime.today().strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD) [default: today]"
    )
    
    # Strategy parameters
    parser.add_argument(
        "--short-window", 
        type=int, 
        default=20,
        help="Short SMA window [default: 20]"
    )
    parser.add_argument(
        "--long-window", 
        type=int, 
        default=50,
        help="Long SMA window [default: 50]"
    )
    parser.add_argument(
        "--cash-buffer", 
        type=float, 
        default=0.2,
        help="Cash buffer ratio (0.0-1.0) [default: 0.2]"
    )
    parser.add_argument(
        "--rebalance", 
        choices=["daily", "on_signal"], 
        default="daily",
        help="Rebalancing frequency [default: daily]"
    )
    
    # Output
    parser.add_argument(
        "--output-dir", 
        type=str, 
        default="base_algo_result",
        help="Output directory [default: base_algo_result]"
    )
    
    return parser.parse_args()


def read_prices(
    tickers: Optional[List[str]] = None, 
    start: Optional[str] = None, 
    end: Optional[str] = None,
    database: str = "stock_db",
    table: str = "stock_prices"
) -> pd.DataFrame:
    """
    Load prices from MySQL using the existing db_utils infrastructure.
    
    Args:
        tickers: List of ticker symbols to load
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        database: Database name (defaults to stock_db from schema)
        table: Table name (defaults to stock_prices from schema)
    
    Returns:
        DataFrame with columns: ['dt', 'ticker', 'close']
    """
    # Use existing DBConfig but override database if needed
    cfg = DBConfig()
    if database != cfg.database:
        # Create a modified config
        from dataclasses import replace
        cfg = replace(cfg, database=database)
    
    conn = connect_db(cfg)
    cur = conn.cursor()
    
    try:
        # Detect available columns (following the pattern from original script)
        cur.execute(f"SHOW COLUMNS FROM {table}")
        cols = [row[0].lower() for row in cur.fetchall()]
        
        # Find symbol column
        sym_col = "ticker" if "ticker" in cols else ("symbol" if "symbol" in cols else None)
        if sym_col is None:
            raise RuntimeError(f"Table `{table}` must have 'ticker' or 'symbol' column.")
        
        # Find price column (prefer adj_close over close)
        price_col = "adj_close" if "adj_close" in cols else ("close" if "close" in cols else None)
        if price_col is None:
            raise RuntimeError(f"Table `{table}` must have 'adj_close' or 'close' column.")
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        if tickers:
            placeholders = ",".join(["%s"] * len(tickers))
            where_conditions.append(f"{sym_col} IN ({placeholders})")
            params.extend(tickers)
        
        if start:
            where_conditions.append("dt >= %s")
            params.append(start)
            
        if end:
            where_conditions.append("dt <= %s")
            params.append(end)
        
        where_sql = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Execute query
        sql = f"""
        SELECT dt, {sym_col} AS ticker, {price_col} AS close 
        FROM {table} 
        {where_sql} 
        ORDER BY dt, {sym_col}
        """
        
        df = pd.read_sql(sql, conn, params=params, parse_dates=["dt"])
        
    finally:
        cur.close()
        conn.close()
    
    return df.dropna().sort_values(["ticker", "dt"]).reset_index(drop=True)


def get_portfolio_tickers() -> List[str]:
    """Get list of tickers from the portfolio table."""
    conn = connect_db()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT DISTINCT ticker FROM portfolio ORDER BY ticker")
        tickers = [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()
    
    return tickers


def ensure_sorted(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure dataframe is sorted by ticker and date."""
    z = df.copy()
    z["dt"] = pd.to_datetime(z["dt"])
    return z.sort_values(["ticker", "dt"]).reset_index(drop=True)


def business_day_align(df: pd.DataFrame) -> pd.DataFrame:
    """
    Forward-fill prices to align all tickers to business days.
    This ensures all tickers have the same date index for portfolio calculations.
    """
    out = []
    for ticker, group in df.groupby("ticker", sort=False):
        # Create business day index from min to max date
        idx = pd.date_range(group["dt"].min(), group["dt"].max(), freq="B")
        
        # Reindex and forward-fill missing values
        aligned = (group.set_index("dt")
                  .reindex(idx)
                  .ffill()
                  .reset_index()
                  .rename(columns={"index": "dt"}))
        
        aligned["ticker"] = ticker
        out.append(aligned[["dt", "ticker", "close"]])
    
    return pd.concat(out, ignore_index=True)


def sma_crossover(
    df: pd.DataFrame, 
    short_window: int = 20, 
    long_window: int = 50
) -> pd.DataFrame:
    """
    Generate SMA crossover signals.
    
    Strategy:
    - Buy signal: short SMA crosses above long SMA
    - Sell signal: short SMA crosses below long SMA
    - Position taken the day after signal (to avoid look-ahead bias)
    
    Args:
        df: Price dataframe with ['dt', 'ticker', 'close']
        short_window: Short SMA period
        long_window: Long SMA period
    
    Returns:
        DataFrame with additional columns: ['sma_s', 'sma_l', 'signal', 'position']
    """
    z = ensure_sorted(df)
    
    # Calculate SMAs for each ticker
    z["sma_s"] = z.groupby("ticker")["close"].transform(
        lambda s: s.rolling(short_window, min_periods=1).mean()
    )
    z["sma_l"] = z.groupby("ticker")["close"].transform(
        lambda s: s.rolling(long_window, min_periods=1).mean()
    )
    
    # Generate signals: 1 when short SMA > long SMA, 0 otherwise
    z["signal"] = (z["sma_s"] > z["sma_l"]).astype(int)
    
    # Position taken next day (lagged signal to avoid look-ahead bias)
    z["position"] = z.groupby("ticker")["signal"].shift(1).fillna(0).astype(int)
    
    return z


def actions_table(df_with_pos: pd.DataFrame) -> pd.DataFrame:
    """
    Create a table of trading actions (buy/sell/hold) for each date and ticker.
    
    Args:
        df_with_pos: DataFrame with position column
    
    Returns:
        Wide-format DataFrame with dates as index and tickers as columns
    """
    z = ensure_sorted(df_with_pos)
    
    # Calculate position changes
    z["pos_prev"] = z.groupby("ticker")["position"].shift(1).fillna(0).astype(int)
    diff = z["position"] - z["pos_prev"]
    
    # Map position changes to actions
    z["action"] = diff.map({1: "buy", -1: "sell"}).fillna("hold")
    
    # Pivot to wide format
    wide = (z.pivot(index="dt", columns="ticker", values="action")
            .sort_index()
            .rename_axis(index="date", columns=None))
    
    return wide


def weights_table(
    df_with_pos: pd.DataFrame, 
    tickers: Optional[List[str]] = None,
    cash_buffer: float = 0.2, 
    rebalance: str = "daily"
) -> pd.DataFrame:
    """
    Calculate portfolio weights based on positions.
    
    Args:
        df_with_pos: DataFrame with position column
        tickers: List of tickers to include (defaults to all unique tickers)
        cash_buffer: Proportion to keep in cash (0.0-1.0)
        rebalance: 'daily' or 'on_signal'
    
    Returns:
        DataFrame with portfolio weights (including CASH column)
    """
    z = ensure_sorted(df_with_pos)
    
    if tickers is None:
        tickers = sorted(z["ticker"].unique().tolist())
    
    # Create wide position matrix
    pos_wide = (z.pivot(index="dt", columns="ticker", values="position")
                .reindex(columns=tickers)
                .fillna(0.0)
                .sort_index())
    
    # Calculate number of positions held each day
    k = pos_wide.sum(axis=1)
    
    # Allocate to stocks (remaining goes to cash)
    alloc_stocks = 1.0 - cash_buffer
    
    # Equal weight among held positions
    weights_stocks = pos_wide.div(k.replace(0, np.nan), axis=0).fillna(0.0) * alloc_stocks
    
    # Cash allocation
    cash_col = pd.Series(
        np.where(k.values > 0, cash_buffer, 1.0),
        index=pos_wide.index,
        name="CASH",
        dtype=float
    )
    
    # Combine cash and stock weights
    weights = pd.concat([cash_col, weights_stocks], axis=1)
    
    # Handle rebalancing frequency
    if rebalance == "on_signal":
        # Only rebalance when positions change
        pos_prev = pos_wide.shift(1).fillna(0.0)
        changed = (pos_wide != pos_prev).any(axis=1)
        
        for i in range(1, len(weights)):
            if not changed.iloc[i]:
                weights.iloc[i] = weights.iloc[i - 1]
        
        # Ensure weights sum to 1
        weights = weights.div(weights.sum(axis=1), axis=0)
    
    weights.index.name = "date"
    return weights


def main():
    args = parse_args()
    
    # Determine which tickers to analyze
    if args.portfolio:
        tickers = get_portfolio_tickers()
        if not tickers:
            print("Portfolio is empty. Please add stocks to portfolio first.")
            print("Use: python -m src.portfolio_manager --add AAPL MSFT GOOG")
            return
        print(f"Using portfolio tickers: {tickers}")
    elif args.tickers:
        tickers = [t.upper() for t in args.tickers]
        print(f"Using specified tickers: {tickers}")
    else:
        print("Please specify either --tickers or --portfolio")
        return
    
    # Load price data
    print(f"Loading price data from {args.start} to {args.end}...")
    df = read_prices(
        tickers=tickers,
        start=args.start,
        end=args.end
    )
    
    if df.empty:
        print("No price data found for the specified parameters.")
        return
    
    print(f"Loaded {len(df)} price records for {df['ticker'].nunique()} tickers")
    
    # Align to business days
    print("Aligning to business days...")
    df = business_day_align(df)
    
    # Generate SMA signals
    print(f"Generating SMA crossover signals (short={args.short_window}, long={args.long_window})...")
    sig = sma_crossover(df, short_window=args.short_window, long_window=args.long_window)
    
    # Calculate trading actions and portfolio weights
    print("Calculating trading actions and portfolio weights...")
    actions = actions_table(sig)
    weights = weights_table(
        sig, 
        tickers=tickers,
        cash_buffer=args.cash_buffer, 
        rebalance=args.rebalance
    )
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Save results
    actions_file = os.path.join(args.output_dir, "signals_actions.csv")
    weights_file = os.path.join(args.output_dir, "daily_weights.csv")
    signals_file = os.path.join(args.output_dir, "detailed_signals.csv")
    
    actions.to_csv(actions_file)
    weights.to_csv(weights_file)
    
    # Also save detailed signal data for analysis
    sig_summary = sig[["dt", "ticker", "close", "sma_s", "sma_l", "signal", "position"]].copy()
    sig_summary.to_csv(signals_file, index=False)
    
    print(f"\nResults saved to {args.output_dir}/:")
    print(f"  - {actions_file}")
    print(f"  - {weights_file}")
    print(f"  - {signals_file}")
    
    # Display summary statistics
    total_signals = sig.groupby("ticker")["signal"].sum()
    print(f"\nStrategy Summary:")
    print(f"  Date range: {sig['dt'].min().date()} to {sig['dt'].max().date()}")
    print(f"  Cash buffer: {args.cash_buffer:.1%}")
    print(f"  Rebalance: {args.rebalance}")
    print(f"\nBuy signals by ticker:")
    for ticker, count in total_signals.items():
        print(f"  {ticker}: {count}")


if __name__ == "__main__":
    main()
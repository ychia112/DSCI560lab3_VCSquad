import argparse
from datetime import datetime
import os
import pandas as pd
import yfinance as yf

from trading_env import TradingEnvironment, Evaluation
# 2 algorithm
#import algo_1 as algo1
#import algo_2 as algo2

# Helper Functions
def load_prices(tickers):
    today = datetime.today().strftime("%Y-%m-%d")
    data = yf.download(tickers, end=today, auto_adjust=False, progress=False)  
    prices_open = data["Open"]
    prices_close = data["Close"]
    return prices_open, prices_close

def load_prices_csv(open_path="data/prices_open.csv", close_path="data/prices_close.csv"):
    prices_open = pd.read_csv(open_path, index_col=0, parse_dates=True)
    prices_close = pd.read_csv(close_path, index_col=0, parse_dates=True)
    return prices_open, prices_close

def infer_tickers_from_pred(df):
    return [c for c in df.columns if str(c).upper() != "CASH"]

def align_signals_to_prices(signals_or_weights, prices_close):
    idx = signals_or_weights.index.intersection(prices_close.index)
    return signals_or_weights.loc[idx]
    

# Flows
def run_live_flow(pred_file, mode, cash):
    pred = pd.read_csv(pred_file, index_col=0, parse_dates=True)

    # Normalize Date format
    pred.index = pd.to_datetime(pred.index, errors="coerce")
    pred.index = pred.index.normalize()
    
    today = pd.to_datetime(datetime.today().strftime("%Y-%m-%d"))
    pred = pred.loc[pred.index <= today]
    if pred.empty:
        print("No valid rows yet (all predictions are in the future).")
        return None

    tickers = infer_tickers_from_pred(pred)
    prices_open, prices_close = load_prices(tickers)

    pred = align_signals_to_prices(pred, prices_close)
    if pred.empty:
        print("No overlapping dates between predictions and available prices.")
        return None

    env = TradingEnvironment(initial_cash=cash, mode=mode, tickers=tickers)
    ledger = env.run_backtest(prices_open, prices_close, pred)

    # Evaluation
    values = Evaluation.value_series(ledger)
    print("\nLatest Performance (up to today):")
    print(ledger.tail(3))
    print()
    print("Annualized Return:", Evaluation.annualized_return(values))
    print("Sharpe Ratio:", Evaluation.sharpe(values))
    print("Max Drawdown:", Evaluation.max_drawdown(values))
    print()
    
    # print("pred last index:", pred.index.max())
    # print("prices_close last index:", prices_close.index.max())
    # print("intersection last index:", pred.index.intersection(prices_close.index).max())

    return ledger


def run_backtest_flow(algo, mode, cash):
    prices_open, prices_close = load_prices_csv()
    tickers = list(prices_close.columns)

    # choose algo based on string
    if algo == "sma":
        my_algo = algo1.MyAlgorithm(tickers)
    elif algo == "arima":
        my_algo = algo2.MyAlgorithm(tickers)
    else:
        raise ValueError("Unsupported algo choice")
    
    outputs = my_algo.generate(prices_open, prices_close, mode=mode)

    env = TradingEnvironment(initial_cash=cash, mode=mode, tickers=tickers)
    ledger = env.run_backtest(prices_open, prices_close, outputs)

    # Evaluation
    values = Evaluation.value_series(ledger)
    print(ledger.tail())
    print("\nFinal portfolio value:", float(values.iloc[-1]))
    print("Annualized Return:", Evaluation.annualized_return(values))
    print("Sharpe Ratio:", Evaluation.sharpe(values))
    print("Max Drawdown:", Evaluation.max_drawdown(values))
    print()

    return ledger




# CLI
def main():
    parser = argparse.ArgumentParser(description="Unified entry: backtest & live evaluation")
    subparsers = parser.add_subparsers(dest="cmd")
    subparsers.required = True
    
    
    p_live = subparsers.add_parser("live", help="Run live evaluation using predicted data")
    p_live.add_argument("--file", required=True, help="CSV of predicted signals/weights")
    p_live.add_argument("--mode", choices=["signals", "weights"], required=True)
    p_live.add_argument("--cash", type=float, default=100000)
    
    
    p_back = subparsers.add_parser("backtest", help="Run historical backtest using CSV data")
    p_back.add_argument("--algo", choices=["sma", "arima"], default="sma")
    p_back.add_argument("--mode", choices=["signals", "weights"], default="signals")
    p_back.add_argument("--cash", type=float, default=100000)
    
    
    args = parser.parse_args()

    if args.cmd == "live":
        run_live_flow(pred_file=args.file, mode=args.mode, cash=args.cash)
    elif args.cmd == "backtest":
        run_backtest_flow(algo=args.algo, mode=args.mode, cash=args.cash)

if __name__ == "__main__":
    main()
import os, warnings
import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path
from statsmodels.tsa.statespace.sarimax import SARIMAX
warnings.filterwarnings("ignore")

TICKERS       = ["AAPL", "GOOG", "MSFT", "NFLX", "AMZN"]
START         = "2015-01-01"
END           = None

YEARS_AHEAD   = 2
TRADING_DAYS  = 252
FORECAST_STEPS = YEARS_AHEAD * TRADING_DAYS

TEST_DAYS     = 252  # Full trading year

# Small, fast set of ARIMA orders; drift via trend = "t" to avoid flat forecast
CANDIDATE_ORDERS = [(1,1,1), (0,1,1), (1,1,0), (2,1,1), (1,1,2), (2,1,0), (0,1,2)]
DRIFT_TREND = "t"
FIT_KW = dict(disp=False, method="lbfgs", maxiter=200)

FAST_MA = 10
SLOW_MA = 30

OUTPUT_DIR    = # CHANGE: Your directory
FORECAST_CSV  = "Stocks_prices.csv"
SIGNALS_CSV   = "Stocks_signals.csv"

def safe_to_csv(frame: pd.DataFrame, path: str) -> str:
    try:
        frame.to_csv(path, index=False)
        return path
    except PermissionError:
        from datetime import datetime
        base_dir, base_name = os.path.split(path)
        stem, ext = os.path.splitext(base_name)
        alt = os.path.join(base_dir, f"{stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}")
        frame.to_csv(alt, index=False)
        return alt

def next_bday(d: pd.Timestamp) -> pd.Timestamp:
    return pd.bdate_range(start=d + pd.Timedelta(days=1), periods=1)[0]

def fit_quick_sarimax_with_drift(y: pd.Series):
    best, best_aic = None, np.inf
    fallback = None
    for order in CANDIDATE_ORDERS + [(0,1,0)]: 
        try:
            res = SARIMAX(y, order=order, trend=DRIFT_TREND,
                          enforce_stationarity=False,
                          enforce_invertibility=False).fit(**FIT_KW)
            if np.isfinite(res.aic):
                if order == (0,1,0):
                    fallback = (res, order)
                elif res.aic < best_aic:
                    best, best_aic = (res, order), res.aic
        except Exception:
            continue
    if best is not None:
        return best
    if fallback is not None:
        return fallback
    raise RuntimeError("No model converged.")

def ma_crossover_signals(close: pd.Series, fast: int = FAST_MA, slow: int = SLOW_MA) -> pd.Series:
    px = pd.to_numeric(close, errors="coerce").astype(float)
    fast_ma = px.rolling(fast).mean()
    slow_ma = px.rolling(slow).mean()
    diff = fast_ma - slow_ma
    sig = np.where(diff > 0, "buy", np.where(diff < 0, "sell", "hold"))
    sig[(fast_ma.isna() | slow_ma.isna()).values] = "hold"
    return pd.Series(sig, index=px.index, dtype=object)

def mae(y_true: pd.Series, y_pred: pd.Series) -> float:
    y_true = pd.to_numeric(y_true, errors="coerce")
    y_pred = pd.to_numeric(y_pred, errors="coerce")
    e = (y_true - y_pred).abs()
    return float(e.mean())

histories = {}
last_dates = {}

for t in TICKERS:
    df = yf.download(t, start=START, end=END, progress=False, auto_adjust=False)
    if df.empty:
        print(f"[WARN] No data for {t} — skipping.")
        continue
    if isinstance(df.columns, pd.MultiIndex):
        df = df.xs(t, level=1, axis=1)
    df = df.reset_index()
    df["Date"]  = pd.to_datetime(df["Date"])
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df.dropna(subset=["Close"]).reset_index(drop=True)
    if df.empty:
        print(f"[WARN] {t} has no valid Close after cleaning — skipping.")
        continue
    histories[t]  = df[["Date", "Close"]].copy()
    last_dates[t] = df["Date"].iloc[-1]

if not histories:
    raise RuntimeError("No tickers produced data. Check tickers/dates/internet.")

all_hist_dates = pd.DatetimeIndex([])
for t, df in histories.items():
    all_hist_dates = all_hist_dates.union(pd.DatetimeIndex(df["Date"]))

global_last_hist = max(last_dates.values())
future_dates = pd.bdate_range(start=next_bday(global_last_hist), periods=FORECAST_STEPS)
global_dates = all_hist_dates.union(future_dates).sort_values()

date_str = pd.DatetimeIndex(global_dates).strftime("%Y-%m-%d")
forecast_df = pd.DataFrame({"date": date_str})
signals_df  = pd.DataFrame({"date": date_str})

for t in TICKERS:
    if t not in histories:
        forecast_df[f"{t}_actual"] = np.nan
        forecast_df[f"{t}_pred"]   = np.nan
        signals_df[t] = "hold"
        continue

    h = histories[t].set_index("Date")["Close"].astype(float)
    hist_aligned = h.reindex(global_dates)
    y = hist_aligned.dropna()
    if y.empty:
        print(f"[WARN] {t} has no valid history; skipping.")
        forecast_df[f"{t}_actual"] = pd.to_numeric(hist_aligned, errors="coerce").round(2).values
        forecast_df[f"{t}_pred"]   = np.nan
        signals_df[t] = "hold"
        continue

    if len(y) > TEST_DAYS + 5:
        split = len(y) - TEST_DAYS
        y_train = y.iloc[:split]
        y_test  = y.iloc[split:]
        n_test  = len(y_test)
        try:
            res_train, _ = fit_quick_sarimax_with_drift(y_train)
            y_pred = pd.Series(
                res_train.get_forecast(steps=n_test).predicted_mean.values,
                index=y_test.index
            )
        except Exception:
            tail  = y_train.tail(20)
            slope = (tail.iloc[-1] - tail.iloc[0]) / max(len(tail) - 1, 1) if len(tail) >= 2 else 0.0
            base  = y_train.iloc[-1]
            y_pred = pd.Series(base + slope * np.arange(1, n_test + 1, dtype=float), index=y_test.index)
        metric_val = mae(y_test, y_pred)
        print(f"[MAE] {t}: {metric_val:.4f} over last {n_test} days")
    else:
        print(f"[MAE] {t}: not enough history to evaluate (have {len(y)} rows)")

    try:
        res, _ = fit_quick_sarimax_with_drift(y)
    except Exception as e:
        print(f"[WARN] {t} fit failed ({e}); using linear drift fallback.")
        res = None

    last_t_date  = y.index[-1]
    steps_needed = len(pd.bdate_range(start=next_bday(last_t_date), end=future_dates[-1]))
    if steps_needed < 0:
        steps_needed = 0

    if steps_needed > 0:
        fut_idx = pd.bdate_range(start=next_bday(last_t_date), periods=steps_needed)
        if res is not None:
            f_vals = res.get_forecast(steps=steps_needed).predicted_mean.values
        else:
            tail  = y.tail(20)
            slope = (tail.iloc[-1] - tail.iloc[0]) / max(len(tail) - 1, 1) if len(tail) >= 2 else 0.0
            base  = y.iloc[-1]
            f_vals = base + slope * np.arange(1, steps_needed + 1, dtype=float)
        f_future = pd.Series(f_vals, index=fut_idx)
    else:
        f_future = pd.Series(dtype=float)

    if res is not None:
        ins = res.get_prediction(start=y.index[0], end=y.index[-1]).predicted_mean
        ins_pred = pd.Series(ins.values, index=y.index)
    else:
        ins_pred = y.shift(1)
        if len(ins_pred) > 0:
            ins_pred.iloc[0] = y.iloc[0]

    pred_series = pd.Series(index=global_dates, dtype=float)
    pred_series.loc[ins_pred.index] = ins_pred.values
    if not f_future.empty:
        pred_series.loc[f_future.index] = f_future.values
    pred_series = pred_series.reindex(global_dates)

    forecast_df[f"{t}_actual"] = pd.to_numeric(hist_aligned, errors="coerce").round(2).values
    forecast_df[f"{t}_pred"]   = pd.to_numeric(pred_series,   errors="coerce").round(2).values

    signals_df[t] = ma_crossover_signals(pred_series).values

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
p_path = safe_to_csv(forecast_df, os.path.join(OUTPUT_DIR, FORECAST_CSV))
s_path = safe_to_csv(signals_df,  os.path.join(OUTPUT_DIR, SIGNALS_CSV))

print(f"Saved prices  to: {p_path}")
print(f"Saved signals to: {s_path}")

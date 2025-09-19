- Install necessary packages

pip install numpy pandas yfinance statsmodels scipy patsy

- Change user directory
OUTPUT_DIR = # Your directory to save output

- What the script does:
1. Download daily close prices via yfinance API
2. Build a unified calendar (union of all real trading dates seen across tickers + next ~504 business day)
3. Fit a small set of ARIMA models with a time trend and choose the best by AIC; fall back to a simple linear drift if fitting fails
4. Forecast future closes
5. Signals: buy if 10-day MA > 30-day MA, sell if 10-day MA < 30-day MA, otherwise hold
6. Evaluate: print MAE for the last 252 trading days per ticker
7. Save two CSVs: forecasted closes and signals

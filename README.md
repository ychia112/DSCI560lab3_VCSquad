# Data Collection Initial Edition 

Before data basing pipeline created, I create a temporary database `stock_db` by running `schema.sql`.

You can first create a `.env` file like this:
```
MYSQL_HOST = localhost
MYSQL_USER = #yourUserName
MYSQL_PASSWORD = #yourPassword
MYSQL_DB = stock_db
MYSQL_PORT = 3306
```

Then run this:

```bash
mysql -u username -p < schema.sql
```

After created the db, you can run the data collection pipeline from the root of repo:
```bash
python -m src.data_collection --config seed_config.json --export-csv artifacts/test_collection.csv
```
It'll save a csv file in `artifacts/` and also save the data into the database.

If you want to get the latest data, run `update_latest.py` when data_collection.py has been done.
```
python -m src.update_latest --tickers GOOG AMZN AAPL MSFT NFLX --interval 1d
```

# Common Algorithm

After `update_latest.py` is compiled, you will get the latest stock information from yahoo finance into the MySQL database. You can now run the `base_algorithm.py` to use SMA to generate signals for buy/sell.

## SMA

This module implements a Simple Moving Average (SMA) Crossover Strategy, a classic technical analysis approach for algorithmic trading.

### Strategy Logic

**Signal Generation**:
   - **Buy Signal**: Generated when the short-term SMA crosses above the long-term SMA
   - **Sell Signal**: Generated when the short-term SMA crosses below the long-term SMA


## Usage

### Basic Syntax

```bash
python -m src.base_algorithm [options]
```

### Required Parameters (Choose One)

**Specify Individual Stocks**:
```bash
python -m src.base_algorithm --tickers AAPL MSFT GOOG
```

**Use Portfolio Stocks**: 
Before using `--portfolio`, first create a portfolio:
```
Add one or more stock to the portfolio:
$ python3 -m src.portfolio_manager --add  <stocks1> (<stock2> …) 
Remove one or more stock from portfolio:
$ python3 -m src.portfolio_manager --remove <stocks1> (<stock2>…)
Get portfolio list:
$ python3 -m src.portfolio_manager --list 
```

```bash
python -m src.base_algorithm --portfolio
```

## CLI Parameters

### Stock Selection

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--tickers` | Specify individual ticker symbols | `--tickers AAPL MSFT GOOG NFLX` |
| `--portfolio` | Use all stocks from database portfolio | `--portfolio` |

### Date Range

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `--start` | Start date (YYYY-MM-DD) | `2022-01-01` | `--start 2020-01-01` |
| `--end` | End date (YYYY-MM-DD) | Today | `--end 2025-09-20` |

### Strategy Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `--short-window` | Short SMA period (days) | `20` | `--short-window 10` |
| `--long-window` | Long SMA period (days) | `50` | `--long-window 100` |
| `--cash-buffer` | Cash buffer ratio (0.0-1.0) | `0.2` | `--cash-buffer 0.1` |
| `--rebalance` | Rebalancing frequency | `daily` | `--rebalance on_signal` |

### Output Settings

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `--output-dir` | Output directory for results | `base_algo_result` | `--output-dir my_results` |

## Usage Examples

### Example 1: Basic Usage
```bash
python -m src.base_algorithm --tickers AAPL MSFT --start 2023-01-01
```

### Example 2: Custom Strategy Parameters
```bash
python -m src.base_algorithm --tickers AAPL MSFT GOOG \
  --short-window 10 --long-window 30 \
  --cash-buffer 0.15 --start 2022-01-01
```

### Example 3: Portfolio-based Strategy
```bash
python -m src.base_algorithm --portfolio \
  --rebalance on_signal --output-dir portfolio_results
```

### Example 4: Long-term Backtest
```bash
python -m src.base_algorithm --tickers AAPL MSFT GOOG NFLX AMZN \
  --start 2020-01-01 --end 2024-01-01 \
  --short-window 20 --long-window 50 \
  --cash-buffer 0.25
```

### Example 5: Aggressive Short-term Strategy
```bash
python -m src.base_algorithm --tickers TSLA NVDA \
  --short-window 5 --long-window 20 \
  --cash-buffer 0.05 --rebalance daily
```

## Output Files

The strategy generates three CSV files in the specified output directory:

1. **`signals_actions.csv`**: Daily trading actions (buy/sell/hold) for each ticker
2. **`daily_weights.csv`**: Daily portfolio weight allocation including cash position
3. **`detailed_signals.csv`**: Detailed signal analysis with SMA values and positions

## Strategy Components

### Cash Buffer
- **Purpose**: Risk control by avoiding full investment
- **Example**: `--cash-buffer 0.2` keeps 20% in cash, invests 80% in stocks

### Rebalancing Frequency
- **`daily`**: Recalculates weights every trading day
- **`on_signal`**: Only rebalances when position signals change

### Moving Average Windows
- **Short Window**: More responsive, captures short-term trends
- **Long Window**: Less responsive, filters out noise
- **Common Combinations**: 5/20, 10/30, 20/50, 50/200


### Integration with Other Modules
The generated CSV files can be used with the trading environment:
```bash
# Run live evaluation with generated signals
python -m Lab4_src.main live --file base_algo_result/signals_actions.csv --mode signals
```
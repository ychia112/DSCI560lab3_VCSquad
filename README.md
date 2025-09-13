# Data Collection Initial Edition 

Before data basing pipeline created, I create a temporary database `stock_db` by running `schema.sql`.

You can first create a `.env` file like this:
```
MYSQL_HOST = localhost
MYSQL_USER = #yourUserName
MYSQL_PASSWORD = #yourPassword
MYSQL_DB = stocks_db
MYSQL_PORT = 3306
```

Then run this:

```bash
mysql -u username -p < temp_create_stockdb.sql
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
# Fill in missing open column
UPDATE stock_prices sp
JOIN (
	SELECT id, LAG(close) OVER (PARTITION BY ticker ORDER BY dt) AS prev_close
	FROM stock_prices
) x ON sp.id = x.id
SET sp.open = x.prev_close
WHERE sp.open IS NULL AND x.prev_close IS NOT NULL;

# Fill in missing close column
UPDATE stock_prices sp
JOIN (
  SELECT id, LEAD(open) OVER (PARTITION BY ticker ORDER BY dt) AS next_open
  FROM stock_prices
) x ON sp.id = x.id
SET sp.close = x.next_open
WHERE sp.close IS NULL AND x.next_open IS NOT NULL;

# Fill in missing high column
UPDATE stock_prices
SET high = GREATEST(open, close)
WHERE high IS NULL AND open IS NOT NULL AND close IS NOT NULL;

# Fill in missing low column
UPDATE stock_prices
SET low = LEAST(open, close)
WHERE low IS NULL AND open IS NOT NULL AND close IS NOT NULL;

# Daily return
SELECT ticker, dt, close, LAG(close) OVER (PARTITION BY ticker ORDER BY dt) AS prev_close, 
 (close / LAG(close) OVER (PARTITION BY ticker ORDER BY dt) - 1) AS daily_return
FROM stock_prices
ORDER BY ticker, dt;

# Monthly return
WITH month_end AS (
    SELECT ticker, DATE_FORMAT(dt, '%Y-%m') AS yearmonth,
        LAST_VALUE(close) OVER (PARTITION BY ticker, DATE_FORMAT(dt, '%Y-%m') ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS month_close,
        ROW_NUMBER() OVER (PARTITION BY ticker, DATE_FORMAT(dt, '%Y-%m') ORDER BY dt DESC) AS rownum
    FROM stock_prices
)
SELECT ticker, yearmonth, month_close, 
	(month_close / LAG(month_close) OVER (PARTITION BY ticker ORDER BY yearmonth) - 1) AS monthly_return
FROM month_end
WHERE rownum = 1
ORDER BY ticker, yearmonth;

# Moving averages
SELECT ticker, dt,
       AVG(close) OVER (PARTITION BY ticker ORDER BY dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
       AVG(close) OVER (PARTITION BY ticker ORDER BY dt ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50
FROM stock_prices;

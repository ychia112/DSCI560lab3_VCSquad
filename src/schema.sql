-- Create database
CREATE DATABASE IF NOT EXISTS stock_db;

-- Use database
USE stock_db;

-- Create Tables
-- 1. Portfolio
CREATE TABLE IF NOT EXISTS portfolio(
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_ticker (ticker)
);

-- 2. Stock_list (Not used for now, kept as backup)
-- List all the stock in database (ensures no duplicates)
CREATE TABLE IF NOT EXISTS stock_list(
    ticker VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100),
    sector VARCHAR(50)
);


-- Insert some stock for testing
-- INSERT INTO stock_list (ticker, name, sector) VALUES
-- ('AAPL', 'Apple Inc.', 'Technology'),
-- ('MSFT', 'Microsoft Corp.', 'Technology'),
-- ('TSLA', 'Tesla Inc.', 'Automotive');


-- 3. stock_prices
-- Stores all the stock data into database
CREATE TABLE IF NOT EXISTS stock_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    dt DATETIME NOT NULL,
    open DECIMAL(10,4),
    high DECIMAL(10,4),
    low DECIMAL(10,4),
    close DECIMAL(10,4),
    adj_close DECIMAL(10,4),
    volume BIGINT,
    `interval` VARCHAR(8) NOT NULL DEFAULT '1d',
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_price (ticker, dt, `interval`)
);


-- # Stock (Generate dynamicly in import_data.py)
-- just show the template
-- table name: stock_<TICKER>
-- For example: Apple Inc. (AAPL)
-- CREATE TABLE IF NOT EXISTS stock_AAPL (
--     id INT AUTO_INCREMENT PRIMARY KEY,
--     date DATE NOT NULL,
--     open DECIMAL(10, 2),
--     high DECIMAL(10, 2),
--     low DECIMAL(10, 2),
--     close DECIMAL(10, 2),
--     volume BIGINT,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     UNIQUE KEY unique_date (date)
-- );

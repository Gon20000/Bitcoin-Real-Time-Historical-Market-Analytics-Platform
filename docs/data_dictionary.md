# Bronze Layer: Batch (Historical CSV) Data Dictionary

| table_name                    | column_name            | data_type   | business_definition_and_rules                                                                                               |
|:------------------------------|:-----------------------|:------------|:----------------------------------------------------------------------------------------------------------------------------|
| raw_binance_klines_historical | open_time              | BIGINT      | Candle open time in 16-digit microsecond UNIX epoch (e.g., 1792540815000000). Needs division by 1e6 for Gold UTC alignment. |
| raw_binance_klines_historical | open_price             | VARCHAR     | First traded price within the 1-second window. Kept as string to prevent float degradation before Silver.                   |
| raw_binance_klines_historical | high_price             | VARCHAR     | Highest traded price within the 1-second window.                                                                            |
| raw_binance_klines_historical | low_price              | VARCHAR     | Lowest traded price within the 1-second window.                                                                             |
| raw_binance_klines_historical | close_price            | VARCHAR     | Last traded price within the 1-second window.                                                                               |
| raw_binance_klines_historical | base_asset_volume      | VARCHAR     | Total volume of Bitcoin (BTC) traded.                                                                                       |
| raw_binance_klines_historical | close_time             | BIGINT      | Candle close time in 16-digit microsecond UNIX epoch.                                                                       |
| raw_binance_klines_historical | quote_asset_volume     | VARCHAR     | Total volume in USDT traded.                                                                                                |
| raw_binance_klines_historical | number_of_trades       | BIGINT      | Total count of individual trade executions.                                                                                 |
| raw_binance_klines_historical | taker_buy_base_volume  | VARCHAR     | Volume of BTC purchased via aggressive market buy orders.                                                                   |
| raw_binance_klines_historical | taker_buy_quote_volume | VARCHAR     | Volume of USDT spent via aggressive market buy orders.                                                                      |
| raw_binance_klines_historical | ignore                 | VARCHAR     | Legacy field from Binance API. Maintained for structural parity with Silver layer before removal in Gold.                   |

---

# Bronze Layer: Streaming (Live WebSocket) Data Dictionary

| table_name                | column_name   | data_type   | business_definition_and_rules                                                                     |
|:--------------------------|:--------------|:------------|:--------------------------------------------------------------------------------------------------|
| raw_binance_klines_stream | e             | VARCHAR     | Event type (always 'kline').                                                                      |
| raw_binance_klines_stream | E             | BIGINT      | Event time in 13-digit millisecond UNIX epoch.                                                    |
| raw_binance_klines_stream | s             | VARCHAR     | Trading symbol (e.g., 'BTCUSDT'). Used as Kafka partition key.                                    |
| raw_binance_klines_stream | k.t           | BIGINT      | Kline open time in 13-digit millisecond UNIX epoch. Needs division by 1e3 for Gold UTC alignment. |
| raw_binance_klines_stream | k.T           | BIGINT      | Kline close time in 13-digit millisecond UNIX epoch.                                              |
| raw_binance_klines_stream | k.s           | VARCHAR     | Trading symbol (duplicate inside 'k' object).                                                     |
| raw_binance_klines_stream | k.i           | VARCHAR     | Interval (always '1s').                                                                           |
| raw_binance_klines_stream | k.f           | BIGINT      | First trade ID within the candle.                                                                 |
| raw_binance_klines_stream | k.L           | BIGINT      | Last trade ID within the candle.                                                                  |
| raw_binance_klines_stream | k.o           | VARCHAR     | Open price as string.                                                                             |
| raw_binance_klines_stream | k.c           | VARCHAR     | Close price as string.                                                                            |
| raw_binance_klines_stream | k.h           | VARCHAR     | High price as string.                                                                             |
| raw_binance_klines_stream | k.l           | VARCHAR     | Low price as string.                                                                              |
| raw_binance_klines_stream | k.v           | VARCHAR     | Base asset volume (BTC).                                                                          |
| raw_binance_klines_stream | k.n           | BIGINT      | Number of trades.                                                                                 |
| raw_binance_klines_stream | k.x           | BOOLEAN     | CRITICAL: Is this kline closed? PySpark must filter 'true' for idempotency.                       |
| raw_binance_klines_stream | k.q           | VARCHAR     | Quote asset volume (USDT).                                                                        |
| raw_binance_klines_stream | k.V           | VARCHAR     | Taker buy base asset volume.                                                                      |
| raw_binance_klines_stream | k.Q           | VARCHAR     | Taker buy quote asset volume.                                                                     |
| raw_binance_klines_stream | k.B           | VARCHAR     | Ignore (legacy parameter from Binance API).                                                       |

---

# Gold Layer Data Dictionary

## Overview
The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of **dimension tables** and **fact tables** for specific business metrics.

---

### 1. **dim_date**
- **Purpose:** Stores calendar date attributes supporting multi-timezone analysis (UTC and Cairo local time) and temporal aggregations.
- **Columns:**

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| date_key         | INT           | Surrogate key uniquely identifying each date record in the dimension table (e.g., 20240101).  |
| calendar_date    | DATE          | The calendar date, formatted as YYYY-MM-DD (e.g., 2024-01-01).                                |
| day_of_week      | INT           | The numeric day of the week (e.g., 1 for Monday, 7 for Sunday).                               |
| day_of_month     | INT           | The day of the month as an integer value (e.g., 15).                                          |
| month_of_year    | INT           | The month of the year as a numeric value (e.g., 12).                                          |
| month_name       | NVARCHAR(50)  | The full name of the calendar month (e.g., 'January', 'December').                            |
| quarter          | INT           | The calendar quarter of the year (e.g., 1, 2, 3, 4).                                          |
| year             | INT           | The four-digit calendar year (e.g., 2024).                                                    |
| is_weekend       | BOOLEAN       | Indicates whether the date falls on a weekend (e.g., 'TRUE', 'FALSE').                        |

---

### 2. **dim_time**
- **Purpose:** Provides second-level granular time dimension (86,400 seconds per day) to support ultra-high-frequency time-of-day analytics.
- **Columns:**

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| time_key         | INT           | Surrogate key uniquely identifying each second of the day in the dimension table (e.g., 143005). |
| time_of_day      | NVARCHAR(50)  | The time of day in 24-hour format, formatted as HH:MM:SS (e.g., '14:30:05').                  |
| hour_of_day      | INT           | The hour component of the timestamp in 24-hour format (e.g., 14).                             |
| minute_of_hour   | INT           | The minute component of the hour (e.g., 30).                                                  |
| second_of_minute | INT           | The second component of the minute (e.g., 5).                                                 |

---

### 3. **dim_market_regime**
- **Purpose:** Provides technical indicators and market regime classifications to categorize trading conditions and market states.
- **Columns:**

| Column Name          | Data Type     | Description                                                                                   |
|----------------------|---------------|-----------------------------------------------------------------------------------------------|
| market_regime_key    | INT           | Surrogate key uniquely identifying each market regime combination in the dimension table.     |
| rsi_regime           | NVARCHAR(50)  | The classification of market momentum based on RSI (e.g., 'Bullish', 'Bearish', 'Overbought').|
| imbalance_regime     | NVARCHAR(50)  | The classification of order flow imbalance (e.g., 'Heavy Buying', 'Moderate Selling', 'Neutral'). |
| volatility_regime    | NVARCHAR(50)  | The classification of market volatility (e.g., 'Extreme', 'High', 'Moderate', 'Low / Squeeze'). |
| spread_category      | NVARCHAR(50)  | The category of the candlestick price spread (e.g., 'Wide', 'Normal', 'Compressed').         |
| is_volatility_spike  | BOOLEAN       | Indicates whether an abnormal volatility spike occurred (e.g., 'TRUE', 'FALSE').              |
| extreme_wick_flag    | BOOLEAN       | Indicates whether the candle has an extreme noise wick requiring cleaning (e.g., 'TRUE', 'FALSE'). |

---

### 4. **fact_btc_klines_1s**
- **Purpose:** Stores 1-second grain transactional and analytical candlestick metrics for Bitcoin market analysis.
- **Columns:**

| Column Name                     | Data Type        | Description                                                                                   |
|---------------------------------|------------------|-----------------------------------------------------------------------------------------------|
| fact_key                        | BIGINT           | Surrogate key uniquely identifying each 1-second candle record in the fact table.             |
| date_utc_key                    | INT              | Surrogate key linking the candle record to the date dimension table using UTC date.           |
| date_eg_key                     | INT              | Surrogate key linking the candle record to the date dimension table using Cairo local date.   |
| time_utc_key                    | INT              | Surrogate key linking the candle record to the time dimension table using UTC time.           |
| time_eg_key                     | INT              | Surrogate key linking the candle record to the time dimension table using Cairo local time.   |
| market_regime_key               | INT              | Surrogate key linking the candle record to the market regime dimension table.                 |
| open                            | DECIMAL(18, 8)   | The opening price of BTC/USDT at the start of the 1-second candle (e.g., 65000.50000000).      |
| high                            | DECIMAL(18, 8)   | The highest traded price of BTC/USDT during the 1-second candle (e.g., 65010.25000000).       |
| low                             | DECIMAL(18, 8)   | The lowest traded price of BTC/USDT during the 1-second candle (e.g., 64995.00000000).        |
| close                           | DECIMAL(18, 8)   | The closing price of BTC/USDT at the end of the 1-second candle (e.g., 65005.10000000).        |
| log_return                      | DECIMAL(18, 8)   | The logarithmic return from the previous 1-second candle (e.g., 0.00007076).                  |
| candle_spread                   | DECIMAL(18, 8)   | The price spread of the candle, calculated as high minus low in USDT (e.g., 15.25000000).      |
| volume                          | DECIMAL(18, 8)   | The total volume of base asset (BTC) traded during the 1-second candle (e.g., 1.45000000).    |
| quote_asset_volume              | DECIMAL(18, 8)   | The total volume of quote asset (USDT) traded during the 1-second candle (e.g., 94250.75000000).|
| number_of_trades                | BIGINT           | The total number of completed trades executed during the 1-second candle (e.g., 42).          |
| taker_buy_base_asset_volume     | DECIMAL(18, 8)   | The volume of BTC bought by market takers executing against ask orders (e.g., 0.85000000).     |
| taker_sell_base_asset_volume    | DECIMAL(18, 8)   | The volume of BTC sold by market takers executing against bid orders (e.g., 0.60000000).     |
| taker_buy_quote_asset_volume    | DECIMAL(18, 8)   | The volume of USDT spent by market takers executing against ask orders (e.g., 55250.00000000). |
| taker_sell_quote_asset_volume   | DECIMAL(18, 8)   | The volume of USDT received by market takers executing against bid orders (e.g., 39000.75000000).|
| avg_trade_size                  | DECIMAL(18, 8)   | The average trade size in BTC per transaction, calculated as volume / trades (e.g., 0.03452381). |
| buy_sell_imbalance              | DECIMAL(18, 8)   | The order flow imbalance ratio between taker buy and taker sell volumes (e.g., 0.17241379).   |
| rsi_14_sec                      | DECIMAL(18, 8)   | The Relative Strength Index calculated over a 14-second rolling window (e.g., 58.45000000).   |
| realized_volatility_60s         | DECIMAL(18, 8)   | The rolling realized volatility of 1-second log returns over 60 seconds (e.g., 0.00421000).   |

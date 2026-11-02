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

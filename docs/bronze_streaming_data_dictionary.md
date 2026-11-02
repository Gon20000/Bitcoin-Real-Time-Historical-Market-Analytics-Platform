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

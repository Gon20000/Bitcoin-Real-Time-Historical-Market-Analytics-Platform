{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with stg as (
    select *
    from {{ ref('stg_binance_klines_1s') }}
),

base_metrics AS(
    SELECT
    open_time,
    open_price AS open,
    high_price AS high,
    low_price AS low,
    close_price AS close,
    base_asset_volume AS volume,
    quote_asset_volume,
    number_of_trades,
    taker_buy_base_asset_volume,
    CAST(base_asset_volume - taker_buy_base_asset_volume AS DECIMAL(18,8)) AS taker_sell_base_asset_volume,
    taker_buy_quote_asset_volume,
    CAST(quote_asset_volume - taker_buy_quote_asset_volume AS DECIMAL(18,8)) AS taker_sell_quote_asset_volume,
    CAST(high_price - low_price AS DECIMAL(18,8)) AS candle_spread,
    CASE
        WHEN number_of_trades > 0 THEN CAST(base_asset_volume / CAST(number_of_trades AS DECIMAL(18,8)) AS DECIMAL(18,8))
        ELSE CAST(0 AS DECIMAL(18,8))
    END AS avg_trade_size,
    CASE
        WHEN base_asset_volume > 0 THEN CAST((taker_buy_base_asset_volume - (base_asset_volume - taker_buy_base_asset_volume)) / base_asset_volume AS DECIMAL(18, 8))
        ELSE CAST(0 AS DECIMAL(18,8))
    END AS buy_sell_imbalance,
    LAG(close_price,1) OVER (ORDER BY open_time) AS prev_close
    FROM stg
),

calculated_returns AS (
    SELECT
        *,
        CASE
            WHEN prev_close IS NOT NULL AND prev_close > 0 AND close > 0
                THEN CAST(LN(CAST(close AS DOUBLE) / CAST(prev_close AS DOUBLE)) AS DECIMAL(18, 8))
            ELSE CAST(0 AS DECIMAL(18, 8))
        END AS log_return,
        CASE WHEN close > prev_close THEN close - prev_close ELSE CAST(0 AS DECIMAL(18, 8)) END AS price_gain,
        CASE WHEN close < prev_close THEN prev_close - close ELSE CAST(0 AS DECIMAL(18, 8)) END AS price_loss
    FROM base_metrics
),

rolling_indicators AS (
    SELECT
        *,
        CAST(SQRT(SUM(POW(CAST(log_return AS DOUBLE), 2)) OVER (
            ORDER BY open_time ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        )) AS DECIMAL(18, 8)) AS realized_volatility_60s,
        AVG(price_gain) OVER (ORDER BY open_time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
        AVG(price_loss) OVER (ORDER BY open_time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14,
        CAST(
            SUM(close * volume) OVER (ORDER BY open_time ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
            / NULLIF(SUM(volume) OVER (ORDER BY open_time ROWS BETWEEN 59 PRECEDING AND CURRENT ROW), 0)
        AS DECIMAL(18, 8)) AS vwap_60s
    FROM calculated_returns
),

vol_ma_step AS (
    SELECT
        *,
        AVG(realized_volatility_60s) OVER (ORDER BY open_time ROWS BETWEEN 299 PRECEDING AND CURRENT ROW) AS ma_vol_300s,
        CASE
            WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN CAST(50 AS DECIMAL(18, 8))
            WHEN avg_loss_14 = 0 THEN CAST(100 AS DECIMAL(18, 8))
            WHEN avg_gain_14 = 0 THEN CAST(0 AS DECIMAL(18, 8))
            ELSE CAST(100.0 - (100.0 / (1.0 + (CAST(avg_gain_14 AS DOUBLE) / CAST(avg_loss_14 AS DOUBLE)))) AS DECIMAL(18, 8))
        END AS rsi_14_sec
    FROM rolling_indicators
),

regime_categories AS (
    SELECT
        *,
        CASE
            WHEN rsi_14_sec >= 70 THEN 'Overbought'
            WHEN rsi_14_sec > 55 THEN 'Bullish'
            WHEN rsi_14_sec >= 45 THEN 'Neutral'
            WHEN rsi_14_sec > 30 THEN 'Bearish'
            ELSE 'Oversold'
        END AS rsi_regime,
        CASE
            WHEN buy_sell_imbalance > 0.50 THEN 'Heavy Buying'
            WHEN buy_sell_imbalance > 0.15 THEN 'Moderate Buying'
            WHEN buy_sell_imbalance >= -0.15 THEN 'Neutral'
            WHEN buy_sell_imbalance >= -0.50 THEN 'Moderate Selling'
            ELSE 'Heavy Selling'
        END AS imbalance_regime,
        CASE
            WHEN realized_volatility_60s > 0.015 THEN 'Extreme'
            WHEN realized_volatility_60s > 0.008 THEN 'High'
            WHEN realized_volatility_60s > 0.003 THEN 'Moderate'
            ELSE 'Low / Squeeze'
        END AS volatility_regime,
        CASE
            WHEN candle_spread > 20 THEN 'Wide'
            WHEN candle_spread >= 5 THEN 'Normal'
            ELSE 'Compressed'
        END AS spread_category,
        CASE
            WHEN ma_vol_300s IS NOT NULL AND ma_vol_300s > 0 AND realized_volatility_60s > (2.5 * ma_vol_300s) THEN TRUE
            ELSE FALSE
        END AS is_volatility_spike,
        CASE
            WHEN candle_spread > 50 THEN TRUE
            ELSE FALSE
        END AS extreme_wick_flag
    FROM vol_ma_step
)

SELECT
    -- 1. Keys (Surrogate & Foreign Keys)
    CAST(DATE_FORMAT(f.open_time, '%Y%m%d%H%i%s') AS BIGINT) AS fact_key,
    CAST(DATE_FORMAT(f.open_time, '%Y%m%d') AS INTEGER) AS date_utc_key,
    CAST(DATE_FORMAT(f.open_time AT TIME ZONE 'Africa/Cairo', '%Y%m%d') AS INTEGER) AS date_eg_key,
    CAST(DATE_FORMAT(f.open_time, '%H%i%s') AS INTEGER) AS time_utc_key,
    CAST(DATE_FORMAT(f.open_time AT TIME ZONE 'Africa/Cairo', '%H%i%s') AS INTEGER) AS time_eg_key,
    dmr.market_regime_key,
    -- 2. Core OHLCV Metrics
    f.open,
    f.high,
    f.low,
    f.close,
    f.log_return,
    f.candle_spread,
    f.volume,
    f.quote_asset_volume,
    f.number_of_trades,
    f.taker_buy_base_asset_volume,
    f.taker_sell_base_asset_volume,
    f.taker_buy_quote_asset_volume,
    f.taker_sell_quote_asset_volume,
    -- 3. Advanced Financial Features
    f.avg_trade_size,
    f.buy_sell_imbalance,
    f.rsi_14_sec,
    f.realized_volatility_60s,
    f.vwap_60s
FROM regime_categories f
INNER JOIN {{ ref('dim_market_regime') }} dmr
    ON dmr.rsi_regime = f.rsi_regime
   AND dmr.imbalance_regime = f.imbalance_regime
   AND dmr.volatility_regime = f.volatility_regime
   AND dmr.spread_category = f.spread_category
   AND dmr.is_volatility_spike = f.is_volatility_spike
   AND dmr.extreme_wick_flag = f.extreme_wick_flag

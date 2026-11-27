{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='fact_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

WITH stg AS (
    SELECT *
    FROM {{ ref('stg_binance_klines_1s') }}
    {% if is_incremental() %}
    -- Lookback buffer of 600 seconds derived from existing fact_key watermark in {{ this }}
    WHERE open_time >= (
        SELECT DATE_PARSE(CAST(MAX(fact_key) AS VARCHAR), '%Y%m%d%H%i%s') - INTERVAL '600' SECOND
        FROM {{ this }}
    )
    {% endif %}
),

base_metrics AS (
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
        CAST(base_asset_volume - taker_buy_base_asset_volume AS DECIMAL(18, 8)) AS taker_sell_base_asset_volume,
        taker_buy_quote_asset_volume,
        CAST(quote_asset_volume - taker_buy_quote_asset_volume AS DECIMAL(18, 8)) AS taker_sell_quote_asset_volume,
        CAST(high_price - low_price AS DECIMAL(18, 8)) AS candle_spread,
        CASE
            WHEN number_of_trades > 0 THEN CAST(base_asset_volume / CAST(number_of_trades AS DECIMAL(18, 8)) AS DECIMAL(18, 8))
            ELSE CAST(0 AS DECIMAL(18, 8))
        END AS avg_trade_size,
        CASE
            WHEN base_asset_volume > 0 THEN CAST((taker_buy_base_asset_volume - (base_asset_volume - taker_buy_base_asset_volume)) / base_asset_volume AS DECIMAL(18, 8))
            ELSE CAST(0 AS DECIMAL(18, 8))
        END AS buy_sell_imbalance,
        LAG(close_price, 1) OVER (ORDER BY open_time) AS prev_close
    FROM stg
),

calculated_returns AS (
    SELECT
        *,
        -- DECIMAL division preserves precision; DOUBLE is isolated strictly to the transcendental LN() boundary
        CASE
            WHEN prev_close IS NOT NULL AND prev_close > 0 AND close > 0
                THEN CAST(LN(CAST(close / prev_close AS DOUBLE)) AS DECIMAL(18, 8))
            ELSE CAST(0 AS DECIMAL(18, 8))
        END AS log_return,
        CASE WHEN close > prev_close THEN close - prev_close ELSE CAST(0 AS DECIMAL(18, 8)) END AS price_gain,
        CASE WHEN close < prev_close THEN prev_close - close ELSE CAST(0 AS DECIMAL(18, 8)) END AS price_loss
    FROM base_metrics
),

rolling_indicators AS (
    SELECT
        *,
        -- Log return squaring and window summation are purely DECIMAL; DOUBLE is isolated strictly to SQRT()
        CAST(SQRT(CAST(SUM(log_return * log_return) OVER (
            ORDER BY open_time ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS DOUBLE)) AS DECIMAL(18, 8)) AS realized_volatility_60s,
        AVG(price_gain) OVER (ORDER BY open_time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
        AVG(price_loss) OVER (ORDER BY open_time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14
    FROM calculated_returns
),

vol_ma_step AS (
    SELECT
        *,
        AVG(realized_volatility_60s) OVER (ORDER BY open_time ROWS BETWEEN 299 PRECEDING AND CURRENT ROW) AS ma_vol_300s,
        AVG(candle_spread) OVER (ORDER BY open_time ROWS BETWEEN 299 PRECEDING AND CURRENT ROW) AS ma_spread_300s,
        -- RSI is calculated entirely in DECIMAL arithmetic to prevent floating-point drift
        CASE
            WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN CAST(50 AS DECIMAL(18, 8))
            WHEN avg_loss_14 = 0 THEN CAST(100 AS DECIMAL(18, 8))
            WHEN avg_gain_14 = 0 THEN CAST(0 AS DECIMAL(18, 8))
            ELSE CAST(
                CAST(100 AS DECIMAL(18, 8)) - (
                    CAST(100 AS DECIMAL(18, 8)) / (
                        CAST(1 AS DECIMAL(18, 8)) + (avg_gain_14 / avg_loss_14)
                    )
                ) AS DECIMAL(18, 8)
            )
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
            WHEN ma_spread_300s IS NOT NULL
                AND ma_spread_300s > 0
                AND candle_spread > (5 * ABS(close - open))
                AND candle_spread > (3 * ma_spread_300s)
            THEN TRUE
            ELSE FALSE
        END AS extreme_wick_flag
    FROM vol_ma_step
)

SELECT
    -- 1. Keys (Surrogate & Foreign Keys)
    CAST(DATE_FORMAT(f.open_time, '%Y%m%d%H%i%s') AS BIGINT) AS fact_key,
    CAST(DATE_FORMAT(f.open_time, '%Y%m%d') AS INTEGER) AS date_utc_key,
    CAST(DATE_FORMAT(WITH_TIMEZONE(f.open_time, 'UTC') AT TIME ZONE 'Africa/Cairo', '%Y%m%d') AS INTEGER) AS date_eg_key,
    CAST(DATE_FORMAT(f.open_time, '%H%i%s') AS INTEGER) AS time_utc_key,
    CAST(DATE_FORMAT(WITH_TIMEZONE(f.open_time, 'UTC') AT TIME ZONE 'Africa/Cairo', '%H%i%s') AS INTEGER) AS time_eg_key,
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
    f.realized_volatility_60s
FROM regime_categories f
INNER JOIN {{ ref('dim_market_regime') }} dmr
    ON dmr.rsi_regime = f.rsi_regime
   AND dmr.imbalance_regime = f.imbalance_regime
   AND dmr.volatility_regime = f.volatility_regime
   AND dmr.spread_category = f.spread_category
   AND dmr.is_volatility_spike = f.is_volatility_spike
   AND dmr.extreme_wick_flag = f.extreme_wick_flag
{% if is_incremental() %}
-- Refresh the 300-second warm-up boundary and insert all new records derived from fact_key watermark
WHERE f.open_time >= (
    SELECT DATE_PARSE(CAST(MAX(fact_key) AS VARCHAR), '%Y%m%d%H%i%s') - INTERVAL '300' SECOND
    FROM {{ this }}
)
{% endif %}

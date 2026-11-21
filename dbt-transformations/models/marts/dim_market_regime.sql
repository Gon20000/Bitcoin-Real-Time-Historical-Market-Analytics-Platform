{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH rsi_values AS (
    SELECT rsi_regime
    FROM UNNEST(ARRAY['Overbought', 'Bullish', 'Neutral', 'Bearish', 'Oversold']) AS T(rsi_regime)
),

imbalance_values AS (
    SELECT imbalance_regime
    FROM UNNEST(ARRAY['Heavy Buying', 'Moderate Buying', 'Neutral', 'Moderate Selling', 'Heavy Selling']) as t(imbalance_regime)
),

volatility_values AS (
    SELECT volatility_regime
    FROM UNNEST(ARRAY['Extreme', 'High', 'Moderate', 'Low / Squeeze']) AS t(volatility_regime)
),

spread_values AS (
    SELECT spread_category
    FROM UNNEST(ARRAY['Wide', 'Normal', 'Compressed']) AS t(spread_category)
),

spike_values AS (
    SELECT is_volatility_spike
    FROM UNNEST(ARRAY[true, false]) AS t(is_volatility_spike)
),

wick_values AS (
    SELECT extreme_wick_flag
    FROM UNNEST(ARRAY[true, false]) AS t(extreme_wick_flag)
),

all_combinations AS (
    SELECT
        r.rsi_regime,
        i.imbalance_regime,
        v.volatility_regime,
        s.spread_category,
        k.is_volatility_spike,
        w.extreme_wick_flag
    FROM rsi_values r
    CROSS JOIN imbalance_values i
    CROSS JOIN volatility_values v
    CROSS JOIN spread_values s
    CROSS JOIN spike_values k
    CROSS JOIN wick_values w
)

SELECT
    CAST(ROW_NUMBER() OVER(
        ORDER BY
            rsi_regime,
            imbalance_regime,
            volatility_regime,
            spread_category,
            is_volatility_spike,
            extreme_wick_flag)
     AS INTEGER) AS market_regime_key,
    rsi_regime,
    imbalance_regime,
    volatility_regime,
    spread_category,
    is_volatility_spike,
    extreme_wick_flag
FROM all_combinations

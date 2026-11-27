SELECT
    fact_key,
    high,
    low
FROM {{ ref('fct_btc_klines_1s') }}
WHERE high < low

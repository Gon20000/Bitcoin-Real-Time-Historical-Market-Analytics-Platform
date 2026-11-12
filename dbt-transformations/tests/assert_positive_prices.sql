SELECT
    fact_key,
    open,
    high,
    low,
    close
FROM {{ ref('fct_btc_klines_1s') }}
WHERE open <= 0
   OR high <= 0
   OR low <= 0
   OR close <= 0

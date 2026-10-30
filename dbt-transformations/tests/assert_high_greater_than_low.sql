-- Test returns rows where high price is less than low price (should return 0 rows to pass)
select
    kline_id,
    open_time,
    high_price,
    low_price
from {{ ref('fct_btc_klines_1s') }}
where high_price < low_price

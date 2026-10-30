-- Test returns rows where price is non-positive (should return 0 rows to pass)
select
    kline_id,
    open_time,
    open_price,
    high_price,
    low_price,
    close_price
from {{ ref('fct_btc_klines_1s') }}
where open_price <= 0
   or high_price <= 0
   or low_price <= 0
   or close_price <= 0

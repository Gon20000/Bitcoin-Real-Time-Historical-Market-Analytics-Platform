-- Test returns rows where taker buy volume exceeds total volume (should return 0 rows to pass)
select
    kline_id,
    open_time,
    base_asset_volume,
    taker_buy_base_asset_volume
from {{ ref('fct_btc_klines_1s') }}
where taker_buy_base_asset_volume > base_asset_volume

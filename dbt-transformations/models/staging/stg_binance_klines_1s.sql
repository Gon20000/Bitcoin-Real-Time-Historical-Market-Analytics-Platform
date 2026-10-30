{{
    config(
        materialized='view',
        schema='silver'
    )
}}

with source_data as (
    select * from {{ source('silver', 'binance_klines_1s') }}
),

standardized as (
    select
        cast(open_time as timestamp(3)) as open_time,
        cast(open_price as decimal(18, 8)) as open_price,
        cast(high_price as decimal(18, 8)) as high_price,
        cast(low_price as decimal(18, 8)) as low_price,
        cast(close_price as decimal(18, 8)) as close_price,
        cast(base_asset_volume as decimal(18, 8)) as base_asset_volume,
        cast(close_time as timestamp(3)) as close_time,
        cast(quote_asset_volume as decimal(18, 8)) as quote_asset_volume,
        cast(number_of_trades as bigint) as number_of_trades,
        cast(taker_buy_base_asset_volume as decimal(18, 8)) as taker_buy_base_asset_volume,
        cast(taker_buy_quote_asset_volume as decimal(18, 8)) as taker_buy_quote_asset_volume,
        cast(is_candle_closed as boolean) as is_candle_closed
    from source_data
    where is_candle_closed = true
)

select * from standardized

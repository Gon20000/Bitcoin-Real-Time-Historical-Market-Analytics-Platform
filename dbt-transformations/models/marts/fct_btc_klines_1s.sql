{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with base_data as (
    select
        open_time,
        close_time,
        open_price,
        high_price,
        low_price,
        close_price,
        base_asset_volume,
        quote_asset_volume,
        number_of_trades,
        taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume,
        is_candle_closed
    from {{ ref('stg_binance_klines_1s') }}
),

returns_calc as (
    select
        *,
        lag(close_price, 1) over (order by open_time) as prev_close_price
    from base_data
),

log_returns as (
    select
        *,
        case
            when prev_close_price is not null and prev_close_price > 0 and close_price > 0
            then cast(ln(cast(close_price as double) / cast(prev_close_price as double)) as decimal(18, 8))
            else cast(0 as decimal(18, 8))
        end as log_return
    from returns_calc
),

rolling_metrics as (
    select
        *,
        -- Realized Volatility: Short-Term (15s) and Long-Term (60s)
        cast(sqrt(sum(pow(cast(log_return as double), 2)) over (
            order by open_time rows between 14 preceding and current row
        )) as decimal(18, 8)) as realized_volatility_15s,

        cast(sqrt(sum(pow(cast(log_return as double), 2)) over (
            order by open_time rows between 59 preceding and current row
        )) as decimal(18, 8)) as realized_volatility_60s,

        -- Trend: Simple Moving Averages (5, 20, 50 periods)
        cast(avg(close_price) over (
            order by open_time rows between 4 preceding and current row
        ) as decimal(18, 8)) as sma_5,

        cast(avg(close_price) over (
            order by open_time rows between 19 preceding and current row
        ) as decimal(18, 8)) as sma_20,

        cast(avg(close_price) over (
            order by open_time rows between 49 preceding and current row
        ) as decimal(18, 8)) as sma_50,

        -- Standard deviation for Bollinger Bands
        cast(coalesce(stddev_samp(cast(close_price as double)) over (
            order by open_time rows between 19 preceding and current row
        ), 0.0) as decimal(18, 8)) as stddev_20,

        -- VWAP (Volume Weighted Average Price) over rolling 60s
        cast(
            sum(close_price * base_asset_volume) over (
                order by open_time rows between 59 preceding and current row
            ) / nullif(sum(base_asset_volume) over (
                order by open_time rows between 59 preceding and current row
            ), 0)
        as decimal(18, 8)) as vwap_60s,

        -- Taker flow ratio
        case
            when base_asset_volume > 0
            then cast(taker_buy_base_asset_volume / base_asset_volume as decimal(18, 8))
            else cast(0 as decimal(18, 8))
        end as taker_buy_ratio
    from log_returns
),

enriched_gold as (
    select
        -- Surrogate & Natural Keys
        cast(date_format(open_time, '%Y%m%d%H%i%s') as bigint) as kline_id,
        cast(date_format(open_time, '%Y%m%d') as integer) as date_key,
        cast(date_format(open_time, '%H%i%s') as integer) as time_key,
        open_time,
        close_time,

        -- OHLCV Core Metrics (DECIMAL 18, 8)
        open_price,
        high_price,
        low_price,
        close_price,
        base_asset_volume,
        quote_asset_volume,
        number_of_trades,
        taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume,

        -- Feature Engineering
        log_return,
        realized_volatility_15s,
        realized_volatility_60s,
        sma_5,
        sma_20,
        sma_50,
        cast(sma_20 + (cast(2 as decimal(18, 8)) * stddev_20) as decimal(18, 8)) as bollinger_upper_20,
        cast(sma_20 - (cast(2 as decimal(18, 8)) * stddev_20) as decimal(18, 8)) as bollinger_lower_20,
        vwap_60s,
        taker_buy_ratio,

        -- Market Regime Classification
        case
            when close_price > (sma_20 + (cast(2 as decimal(18, 8)) * stddev_20)) and realized_volatility_60s > 0.005 then 3
            when sma_5 > sma_20 and sma_20 > sma_50 then 1
            when sma_5 < sma_20 and sma_20 < sma_50 then 2
            when realized_volatility_60s < 0.001 then 4
            else 5
        end as market_regime_key,

        is_candle_closed
    from rolling_metrics
)

select * from enriched_gold

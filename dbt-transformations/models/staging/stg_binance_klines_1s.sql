{{
    config(
        materialized='view',
        schema='silver'
    )
}}

WITH source_data AS (
    SELECT *
    FROM {{ source('silver', 'binance_klines_1s') }}
),

standardized AS (
    SELECT
        CAST(open_time AS TIMESTAMP(3)) AS open_time,
        CAST(open_price AS DECIMAL(18,8)) AS open_price,
        CAST(high_price AS DECIMAL(18,8)) AS high_price,
        CAST(low_price AS DECIMAL(18,8)) AS low_price,
        CAST(close_price AS DECIMAL(18,8)) AS close_price,
        CAST(base_asset_volume AS DECIMAL(18,8)) AS base_asset_volume,
        CAST(close_time AS TIMESTAMP(3)) AS close_time,
        CAST(quote_asset_volume AS DECIMAL(18,8)) AS quote_asset_volume,
        CAST(number_of_trades AS BIGINT) AS number_of_trades,
        CAST(taker_buy_base_asset_volume AS DECIMAL(18,8)) AS taker_buy_base_asset_volume,
        CAST(taker_buy_quote_asset_volume AS DECIMAL(18,8)) AS taker_buy_quote_asset_volume,
        CAST(is_candle_closed AS BOOLEAN) AS is_candle_closed
    FROM source_data
    WHERE is_candle_closed = TRUE
)

SELECT *
FROM standardized

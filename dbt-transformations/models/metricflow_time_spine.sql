{{
    config(
        materialized='table',
        schema='gold'
    )
}}

SELECT
    calendar_date AS date_day
FROM {{ ref('dim_date') }}

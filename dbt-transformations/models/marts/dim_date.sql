{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH date_sequence AS (
    SELECT
        calendar_date
    FROM
        UNNEST(
            SEQUENCE(
                DATE('2017-01-01'),
                DATE('2030-12-31'),
                INTERVAL '1' DAY
            )
        ) AS T(calendar_date)
)

SELECT
    CAST(date_format(calendar_date, '%Y%m%d') AS INTEGER) AS date_key,
    calendar_date,
    CAST(EXTRACT(YEAR FROM calendar_date) AS INTEGER) AS year,
    CAST(EXTRACT(QUARTER FROM calendar_date) AS INTEGER) AS quarter,
    CAST(EXTRACT(MONTH FROM calendar_date) AS INTEGER) AS month_of_year,
    DATE_FORMAT(calendar_date, '%M') AS month_name,
    CAST(EXTRACT(DAY FROM calendar_date) AS INTEGER) AS day_of_month,
    CAST(DAY_OF_WEEK(calendar_date) AS INTEGER) AS day_of_week,
    DATE_FORMAT(calendar_date, '%W') AS day_name,
    CASE
        WHEN DAY_OF_WEEK(calendar_date) IN (6, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM date_sequence

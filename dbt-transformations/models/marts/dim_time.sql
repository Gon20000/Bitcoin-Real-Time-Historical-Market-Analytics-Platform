{{
    config(
        materialized='table',
        schema='gold'
    )

}}
WITH hours AS (
    SELECT
        h AS hour_of_day
    FROM
        UNNEST(SEQUENCE(0, 23)) AS t(h)
),

minutes AS (
    SELECT
        m AS minute_of_hour
    FROM
        UNNEST(SEQUENCE(0, 59)) AS t(m)
),

second AS (
   SELECT
        s AS second_of_minute
    FROM UNNEST(SEQUENCE(0, 59)) AS t(s)
),

time_breakdown AS(
     SELECT
        h.hour_of_day,
        m.minute_of_hour,
        s.second_of_minute
    FROM hours h
    CROSS JOIN minutes m
    CROSS JOIN second s
  )


SELECT
    CAST((hour_of_day  * 10000) + (minute_of_hour  * 100) + second_of_minute  AS INTEGER) AS time_key,
    CONCAT(
        LPAD(CAST(hour_of_day AS VARCHAR), 2, '0'), ':',
        LPAD(CAST(minute_of_hour AS VARCHAR), 2, '0'), ':',
        LPAD(CAST(second_of_minute AS VARCHAR), 2, '0')
    ) AS time_of_day,
    hour_of_day,
    minute_of_hour,
    second_of_minute
FROM time_breakdown

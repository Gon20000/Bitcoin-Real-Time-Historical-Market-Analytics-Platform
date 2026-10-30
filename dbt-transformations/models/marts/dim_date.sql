{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with date_sequence as (
    select
        calendar_date
    from
        unnest(sequence(date '2017-01-01', date '2030-12-31', interval '1' day)) as t(calendar_date)
)

select
    cast(date_format(calendar_date, '%Y%m%d') as integer) as date_key,
    calendar_date as date_utc,
    cast(from_unixtime(to_unixtime(cast(calendar_date as timestamp(3))), 'Africa/Cairo') as date) as date_eg,
    cast(extract(year from calendar_date) as integer) as year,
    cast(extract(quarter from calendar_date) as integer) as quarter,
    cast(extract(month from calendar_date) as integer) as month,
    date_format(calendar_date, '%M') as month_name,
    cast(extract(day from calendar_date) as integer) as day,
    cast(extract(day_of_week from calendar_date) as integer) as day_of_week,
    date_format(calendar_date, '%W') as day_name,
    case when extract(day_of_week from calendar_date) in (6, 7) then true else false end as is_weekend,
    case when calendar_date = last_day_of_month(calendar_date) then true else false end as is_month_end
from date_sequence

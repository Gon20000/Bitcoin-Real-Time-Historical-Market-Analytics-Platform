{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with second_sequence as (
    select
        s as second_of_day
    from
        unnest(sequence(0, 86399)) as t(s)
),

time_breakdown as (
    select
        second_of_day,
        cast(second_of_day / 3600 as integer) as hour_24,
        cast((second_of_day % 3600) / 60 as integer) as minute_val,
        cast(second_of_day % 60 as integer) as second_val
    from second_sequence
)

select
    cast((hour_24 * 10000) + (minute_val * 100) + second_val as integer) as time_key,
    concat(
        lpad(cast(hour_24 as varchar), 2, '0'), ':',
        lpad(cast(minute_val as varchar), 2, '0'), ':',
        lpad(cast(second_val as varchar), 2, '0')
    ) as time_utc,
    concat(
        lpad(cast((hour_24 + 2) % 24 as varchar), 2, '0'), ':',
        lpad(cast(minute_val as varchar), 2, '0'), ':',
        lpad(cast(second_val as varchar), 2, '0')
    ) as time_eg,
    hour_24,
    minute_val as minute,
    second_val as second,
    case
        when hour_24 between 0 and 7 then 'Asian Session'
        when hour_24 between 8 and 12 then 'European Morning'
        when hour_24 between 13 and 16 then 'US-EU Overlap'
        when hour_24 between 17 and 20 then 'US Afternoon'
        else 'Pacific / Off-Peak'
    end as trading_session
from time_breakdown

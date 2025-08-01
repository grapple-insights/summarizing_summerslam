{{ config(materialized="table", tags=["dimension"]) }}

with calendar as (
    select
        d as date_day,
        -- Surrogate key as integer in yyyymmdd format
        cast(format_date('%Y%m%d', d) as int64) as dim_date_key,
        extract(year from d) as year,
        extract(month from d) as month,
        extract(day from d) as day,
        extract(quarter from d) as quarter,
        format_date('%A', d) as day_name,
        extract(dayofweek from d) as day_of_week,     -- 1=Sunday, 7=Saturday
        extract(week from d) as week_of_year,
        date_trunc(d, month) as first_day_of_month,
        last_day(d) as last_day_of_month,
        case 
            when extract(dayofweek from d) in (1,7) then true
            else false
        end as is_weekend
    from unnest(generate_date_array('1988-01-01', '2035-12-31', interval 1 day)) as d
)

select
    dim_date_key,
    date_day,
    year,
    month,
    day,
    quarter,
    day_name,
    day_of_week,
    week_of_year,
    first_day_of_month,
    last_day_of_month,
    is_weekend
from calendar

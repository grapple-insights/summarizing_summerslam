{{ config(materialized="view", tags=["reporting", "mart"]) }}

with base as (
    select * from {{ ref('fact_match_metrics') }}
),

joined as (
    select
        -- Event dimension
        e.event_name,
        e.event_year,
        e.event_date,
        p.promotion,
        
        -- Title and match type
        t.title,               -- <-- Add the title column here
        mt.match_type,         -- <-- Keep match_type column

        -- Core match data
        base.match_number,
        base.match_duration_seconds,
        base.is_draw,
        base.is_championship_match,
        base.is_special_guest_referee
    from base
    left join {{ ref('dim_event') }}       e  on e.dim_event_key = base.dim_event_key
    left join {{ ref('dim_date') }}        d  on d.dim_date_key = base.dim_date_key
    left join {{ ref('dim_promotion') }}   p  on p.dim_promotion_key = base.dim_promotion_key
    left join {{ ref('dim_title') }}       t  on t.dim_title_key = base.dim_title_key
    left join {{ ref('dim_match_type') }}  mt on mt.dim_match_type_key = base.dim_match_type_key
)

select *
from joined
order by event_year, event_name, match_number

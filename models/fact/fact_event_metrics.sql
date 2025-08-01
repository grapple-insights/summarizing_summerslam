{{ config(materialized="table", tags=["fact"]) }}

with stg as (
    select *
    from {{ ref('stage_raw_infobox') }}
),

-- Join to dim_event
event_dim as (
    select
        stg.*,
        event_dim.dim_event_key
        -- Do NOT select stg.event_name, stg.event_year, stg.event_date, stg.promotion again!
    from stg
    left join {{ ref('dim_event') }} as event_dim
      on stg.event_name = event_dim.event_name
     and stg.event_year = event_dim.event_year
     and stg.event_date = event_dim.event_date
     and stg.promotion = event_dim.promotion
),

-- Join to dim_date
date_dim as (
    select
        event_dim.*,
        date_dim.dim_date_key
    from event_dim
    left join {{ ref('dim_date') }} as date_dim
      on event_dim.event_date = date_dim.date_day
),

-- Join to dim_location
location_dim as (
    select
        date_dim.*,
        location_dim.dim_location_key
    from date_dim
    left join {{ ref('dim_location') }} as location_dim
      on date_dim.event_city = location_dim.event_city
     and ifnull(date_dim.event_state,'') = ifnull(location_dim.event_state,'')
     and ifnull(date_dim.event_country,'') = ifnull(location_dim.event_country,'')
),

-- Join to dim_venue
venue_dim as (
    select
        location_dim.*,
        venue_dim.dim_venue_key
    from location_dim
    left join {{ ref('dim_venue') }} as venue_dim
      on location_dim.event_venue = venue_dim.event_venue
     and ifnull(location_dim.event_city,'') = ifnull(venue_dim.event_city,'')
     and ifnull(location_dim.event_state,'') = ifnull(venue_dim.event_state,'')
     and ifnull(location_dim.event_country,'') = ifnull(venue_dim.event_country,'')
),

-- Join to dim_promotion
final_fact as (
    select
        -- Add the hash key using the same grain as dim_event_key
        {{ dbt_utils.generate_surrogate_key([
            'venue_dim.event_name',
            'venue_dim.event_year',
            'venue_dim.event_date',
            'venue_dim.promotion'
        ]) }} as fact_event_metrics_key,

        venue_dim.dim_event_key,
        venue_dim.dim_date_key,
        venue_dim.dim_location_key,
        venue_dim.dim_venue_key,
        promo.dim_promotion_key,
        venue_dim.event_attendance,
        venue_dim.event_buy_rate
    from venue_dim
    left join {{ ref('dim_promotion') }} as promo
      on venue_dim.promotion = promo.promotion
    -- No WHERE clause! Include all events regardless of missing metrics
)

select distinct
    fact_event_metrics_key,
    dim_event_key,
    dim_date_key,
    dim_location_key,
    dim_venue_key,
    dim_promotion_key,
    event_attendance,
    event_buy_rate
from final_fact

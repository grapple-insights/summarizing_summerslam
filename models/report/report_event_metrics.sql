{{ config(materialized="table", tags=["report"]) }}

with fact as (
    select *
    from {{ ref('fact_event_metrics') }}
),

dim_event as (
    select *
    from {{ ref('dim_event') }}
),

dim_date as (
    select *
    from {{ ref('dim_date') }}
),

dim_location as (
    select *
    from {{ ref('dim_location') }}
),

dim_venue as (
    select *
    from {{ ref('dim_venue') }}
),

dim_promotion as (
    select *
    from {{ ref('dim_promotion') }}
)

select
    -- Fact table metrics
    fact.event_attendance,
    fact.event_buy_rate,

    -- Event details
    event.event_name,
    event.event_year,
    event.event_date,

    -- Date details
    date.month as event_month,
    date.day_name as event_day_name,

    -- Location details
    location.event_city,
    location.event_state,
    location.event_country,

    -- Venue details
    venue.event_venue,

    -- Promotion details
    promotion.promotion

from fact
left join dim_event event
  on fact.dim_event_key = event.dim_event_key
left join dim_date date
  on fact.dim_date_key = date.dim_date_key
left join dim_location location
  on fact.dim_location_key = location.dim_location_key
left join dim_venue venue
  on fact.dim_venue_key = venue.dim_venue_key
left join dim_promotion promotion
  on fact.dim_promotion_key = promotion.dim_promotion_key

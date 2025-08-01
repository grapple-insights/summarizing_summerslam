{{ config(materialized="table", tags=["dimension"]) }}

with base as (

    select distinct
        event_venue,
        event_city,
        event_state,
        event_country
    from {{ ref('stage_raw_infobox') }}
    where event_venue is not null and trim(event_venue) != ''

),

with_keys as (

    select
        {{ dbt_utils.generate_surrogate_key(['event_venue', 'event_city', 'event_state', 'event_country']) }} as dim_venue_key,
        event_venue,
        event_city,
        event_state,
        event_country
    from base

)

select * from with_keys

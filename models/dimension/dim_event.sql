{{ config(materialized="table", tags=["dimension"]) }}

with base as (

    select distinct
        event_name,
        event_year,
        event_date,
        promotion
    from {{ ref('stage_raw_infobox') }}

),

with_keys as (

    select
        {{ dbt_utils.generate_surrogate_key(['event_name','event_year','event_date','promotion']) }} as dim_event_key,
        event_name,
        event_year,
        event_date,
        promotion
    from base

)

select * from with_keys
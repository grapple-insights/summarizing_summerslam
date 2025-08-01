{{ config(materialized="table", tags=["dimension"]) }}

with base as (

    select distinct
        promotion
    from {{ ref('stage_raw_infobox') }}
    where promotion is not null and trim(promotion) != ''

),

-- Optionally, put in a "promotion_clean" logic if you normalize names (else, use as-is)
with_clean as (
    select
        -- Example: normalize existing promotions as you do in your stage model
        case
            when promotion = 'WWE' or promotion = 'World Wrestling Entertainment' then 'WWE'
            when promotion = 'World Wrestling Federation' then 'WWF'
            else promotion
        end as promotion_clean
    from base
),

deduped as (
    select distinct
        promotion_clean
    from with_clean
),

with_keys as (

    select
        {{ dbt_utils.generate_surrogate_key(['promotion_clean']) }} as dim_promotion_key,
        promotion_clean as promotion
    from deduped

)

select *
from with_keys

{{ config(materialized="table", tags=["fact"]) }}

with stage as (
    select * from {{ ref('stage_raw_results') }}
),

stage_with_event as (
    select
        s.*,
        e.dim_event_key,
        e.event_date,
        e.promotion as event_promotion
    from stage s
    left join {{ ref('dim_event') }} e
        on e.event_name = s.event_name
        and SAFE_CAST(e.event_year AS INT64) = s.event_year
),

fact_base as (
    select
        -- Deterministic match-level PK
        {{ dbt_utils.generate_surrogate_key(['swe.event_name', 'swe.event_year', 'swe.match_number']) }} as fact_result_metrics_key,

        -- Only dim-keys, the grain, and facts (NO free-text)
        swe.dim_event_key,
        d.dim_date_key,
        p.dim_promotion_key,
        t.dim_title_key,
        mt.dim_match_type_key,    
        swe.match_number,

        -- Numeric or Boolean Facts
        swe.is_draw,
        swe.is_championship_match,
        swe.is_special_guest_referee,
        swe.match_duration_seconds
    from stage_with_event swe

    -- Date dimension
    left join {{ ref('dim_date') }} d
        on d.date_day = swe.event_date

    -- Promotion dimension
    left join {{ ref('dim_promotion') }} p
        on lower(trim(p.promotion)) = lower(trim(
                case
                    when swe.event_promotion = 'WWE' or swe.event_promotion = 'World Wrestling Entertainment' then 'WWE'
                    when swe.event_promotion = 'World Wrestling Federation' then 'WWF'
                    else swe.event_promotion
                end
            ))

    -- Title dimension
    left join {{ ref('dim_title') }} t
        on t.title = swe.title

    -- Match type dimension: robust normalization in join!
    left join {{ ref('dim_match_type') }} mt
      on lower(
           regexp_replace(
             regexp_replace(
               regexp_replace(trim(swe.match_type), r'[_\-]', ' '),      -- underscores/hyphens to space
               r'[\,\.]', ''                                             -- remove commas & periods (removed 4th arg)
             ),
             r'\s+', ' '                                                -- collapse spaces
           )
         )
       =
         lower(
           regexp_replace(
             regexp_replace(
               regexp_replace(trim(mt.match_type), r'[_\-]', ' '),      -- underscores/hyphens to space
               r'[\,\.]', ''                                            -- remove commas & periods (removed 4th arg)
             ),
             r'\s+', ' '                                                -- collapse spaces
           )
         )

)

select *
from fact_base
order by dim_event_key, match_number

{{ config(materialized="table", tags=["dimension"]) }}

with participants as (
    select match_winner as wrestler_name from {{ ref('stage_raw_results') }}
    where match_winner is not null and trim(match_winner) != ''

    union all

    select match_loser as wrestler_name from {{ ref('stage_raw_results') }}
    where match_loser is not null and trim(match_loser) != ''

    union all

    select draw_participant_1 as wrestler_name from {{ ref('stage_raw_results') }}
    where draw_participant_1 is not null and trim(draw_participant_1) != ''

    union all

    select draw_participant_2 as wrestler_name from {{ ref('stage_raw_results') }}
    where draw_participant_2 is not null and trim(draw_participant_2) != ''
),

cleaned_names as (
    select
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                -- *** keep only up to first parenthesis group ***
                                regexp_replace(
                                    wrestler_name,
                                    r'^([^(]*\([^)]*\)).*',
                                    r'\1'
                                ),
                                r'(?i)(\(|\[)[ ]*with[^)\]]*(\)|\])', ''
                            ),
                            r'(?i)(\(|\[)c(\)|\])', ''
                        ),
                        r'[”"]', ''
                    ),
                    r'[-–—‑‒]', '-'
                ),
                r'\s+', ' '
            )
        ) as wrestler_name_cleaned
    from participants
    where wrestler_name is not null and wrestler_name != ''
)

select
    {{ dbt_utils.generate_surrogate_key(['wrestler_name_cleaned']) }} as dim_wrestler_key,
    wrestler_name_cleaned as wrestler_name
from cleaned_names
where wrestler_name_cleaned is not null and wrestler_name_cleaned != ''
group by wrestler_name_cleaned
order by wrestler_name_cleaned

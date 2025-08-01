with source as (
    select * from {{ source('analytics_prod', 'raw_results') }}
),

filtered as (
    select
        * 
    from source
    where 
        lower(trim(`results`)) != 'results'
        and lower(trim(`no`)) != 'no.'
        and lower(trim(`no`)) != 'no'
),

step1 as (
    select
        regexp_extract(event, r'^(.*?)\s*\(') as event_name,
        regexp_extract(event, r'\((\d{4})\)') as event_year_str,
        safe_cast(regexp_extract(event, r'\((\d{4})\)') as int64) as event_year,
        safe_cast(regexp_replace(`no`, r'\D', '') as int64) as match_number,
        trim(regexp_replace(`results`, r'\[.*?\]', '')) as results_cleaned,
        `results` as results_raw,
        trim(regexp_replace(`stipulations`, r'\[.*?\]', '')) as stipulations_cleaned,
        `times` as match_duration_str,
        trim(regexp_replace(`times`, r'\[.*?\]', '')) as match_duration_cleaned,
        event
    from filtered
),

step2 as (
    select
        *,
        case
            when lower(results_cleaned) like '%defeated%' then 
                trim(regexp_replace(
                    regexp_extract(results_cleaned, r'^(.*?) defeated'),
                    r'[\[\]]', ''
                ))
            when lower(results_cleaned) like '% won by %' then
                trim(regexp_replace(
                    regexp_extract(results_cleaned, r'^(.*?) won by'),
                    r'[\[\]]', ''
                ))
            else 
                null 
        end as match_winner_extracted,

        case 
            when lower(results_cleaned) like '%defeated%' then
                trim(regexp_replace(
                    regexp_extract(results_cleaned, r'defeated (.*?)(?: by |$)'),
                    r'[\[\]]', ''
                ))
            else 
                null
        end as match_loser_extracted,

        case
            when lower(results_cleaned) like '%defeated%' and regexp_contains(results_cleaned, r' by ([^[]+)') then
                trim(lower(regexp_extract(results_cleaned, r' by ([^,.\[]+)')))
            when lower(results_cleaned) like '%vs%' then
                'draw'
            else
                null
        end as result_type_extracted,

        case
            when lower(results_cleaned) like '%vs%' then true
            else false
        end as is_draw
    from step1
),

step3 as (
    select
        *,
        case when is_draw then null else match_winner_extracted end as match_winner_final,
        case when is_draw then null else match_loser_extracted end as match_loser_final,
        case 
            when is_draw then 'draw'
            else result_type_extracted
        end as result_type_final,
        stipulations_cleaned as stipulations_final,
        instr(lower(stipulations_cleaned), 'championship') > 0 as is_championship_match_final,
        instr(lower(stipulations_cleaned), 'special guest referee') > 0 as is_special_guest_referee,
        lower(
          coalesce(
            trim(
              regexp_extract(
                lower(stipulations_cleaned),
                r"^([a-z0-9,'’\.&\-\s]+?)\s+match\b",
                1
              )
            ),
            trim(
              regexp_extract(
                lower(stipulations_cleaned),
                r"^([a-z0-9,'’\.&\-\s]+?)\s+(?:for|since|if|when|because|after|where|with)\b",
                1
              )
            ),
            trim(lower(stipulations_cleaned))
          )
        ) as match_type_final,

        trim(
            regexp_replace(
                regexp_extract(stipulations_cleaned, r'(?i)the\s+(.+?)\s+championship'),
                r'\bchampionship\b', ''
            )
        ) as title_final,

        -- Clean draw opponent parsing with correct BigQuery regex
        case
            when is_draw then
                trim(regexp_extract(results_cleaned, r'^(.*?)\s*(?:vs\.|versus)\s'))
            else null
        end as draw_participant_1,

        case
            when is_draw then
                trim(
                    regexp_extract(
                        results_cleaned,
                        r'(?:vs\.|versus)\s*(.*?)(?: ended| by| in|$)'
                    )
                )
            else null
        end as draw_participant_2

    from step2
),

final as (
    select
        event,
        event_name,
        event_year,
        match_number,
        results_cleaned as results,
        match_winner_final as match_winner,
        match_loser_final as match_loser,
        result_type_final as result_type,
        is_draw,
        stipulations_final as stipulations,
        is_championship_match_final as is_championship_match,
        is_special_guest_referee,
        trim(match_type_final) as match_type,
        title_final as title,
        match_duration_str,
        case
            when 
                regexp_contains(match_duration_cleaned, r'\d{1,2}:\d{2}')
                and length(trim(match_duration_cleaned)) > 0
            then
                (
                    safe_cast(regexp_extract(match_duration_cleaned, r'(\d{1,2}):\d{2}') as int64) * 60
                    + safe_cast(regexp_extract(match_duration_cleaned, r'\d{1,2}:(\d{2})') as int64)
                )
            else null
        end as match_duration_seconds,
        draw_participant_1,
        draw_participant_2
    from step3
)

select
    event,
    event_name,
    event_year,
    match_number,
    results,
    match_winner,
    match_loser,
    result_type,
    is_draw,
    stipulations,
    is_championship_match,
    is_special_guest_referee,
    match_type,
    title,
    match_duration_seconds,
    draw_participant_1,
    draw_participant_2
from final


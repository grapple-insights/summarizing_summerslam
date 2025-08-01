{{ config(materialized="table", tags=["dimension"]) }}

with source as (
    select * from {{ ref('stage_raw_results') }}
),

type_values as (
    select
        trim(match_type) as match_type_raw
    from source
    where match_type is not null and trim(match_type) != ''
),

-- Step 1: Remove ALL possible markdown links (e.g. [Label](URL))
remove_markdown as (
    select
        regexp_replace(
            match_type_raw,
            r'\[([^\]]+)\]\([^)]+\)',
            r'\1'
        ) as match_type_nolink
    from type_values
),

-- Step 2: Normalize:
-- - Lowercase
-- - Remove all extra whitespace
-- - Remove all commas
-- - Standardize all forms of "and"/", and"/",and" to " and "
standardized as (
    select
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    lower(match_type_nolink),
                    r'\s*,\s*and\s+', ' and '
                ),
                r'\s+and\s+', ' and '
            ),
            r',', ''  -- Remove ALL commas
        ) as match_type_nowhitespace
    from remove_markdown
),

-- Step 3: Extract only left of first " for " as match type
domain_parsed as (
    select
        *,
        case
            when regexp_contains(match_type_nowhitespace, r' for ') then
                trim(regexp_extract(match_type_nowhitespace, r'^(.*?)\s+for\s+'))
            else
                trim(match_type_nowhitespace)
        end as match_type_extracted
    from standardized
),

final_types as (
    select
        match_type_extracted as match_type
    from domain_parsed
    where match_type_extracted is not null
      and trim(match_type_extracted) != ''
    group by match_type_extracted
)

select
    {{ dbt_utils.generate_surrogate_key(['match_type']) }} as dim_match_type_key,
    initcap(match_type) as match_type
from final_types
order by match_type

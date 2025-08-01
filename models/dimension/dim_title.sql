{{ config(materialized="table", tags=["dimension"]) }}

with source as (
  select * from {{ ref('stage_raw_results') }}
),

title_values as (
  select
    trim(title) as title
  from source
  where title is not null and trim(title) != ''
),

distinct_titles as (
  select
    title
  from title_values
  group by title
)

select
  {{ dbt_utils.generate_surrogate_key(['title']) }} as dim_title_key,
  title
from distinct_titles
order by title

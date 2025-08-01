{{ config(materialized="table", tags=["bridge"]) }}

with stage as (
  select * from {{ ref('stage_raw_results') }}
),

-- Unpivot all possible participants to get each role in every match
all_participants as (
  select event_name, event_year, match_number, 'winner' as participant_role, match_winner as wrestler_name
  from stage
  where match_winner is not null and trim(match_winner) != ''

  union all

  select event_name, event_year, match_number, 'loser' as participant_role, match_loser as wrestler_name
  from stage
  where match_loser is not null and trim(match_loser) != ''

  union all

  select event_name, event_year, match_number, 'draw_participant_1' as participant_role, draw_participant_1 as wrestler_name
  from stage
  where draw_participant_1 is not null and trim(draw_participant_1) != ''

  union all

  select event_name, event_year, match_number, 'draw_participant_2' as participant_role, draw_participant_2 as wrestler_name
  from stage
  where draw_participant_2 is not null and trim(draw_participant_2) != ''
),

cleaned_names as (
  select
    event_name,
    event_year,
    match_number,
    participant_role,
    -- *** Only keep up to first parenthesis group (team + members), drop all trailing parentheticals/notes ***
    trim(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  wrestler_name,
                  r'^([^(]*\([^)]*\)).*',  -- NEW: keep only team (members), drop extra notes
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
  from all_participants
  where wrestler_name is not null and wrestler_name != ''
)

select
  ev.dim_event_key,
  c.match_number,
  c.participant_role,
  w.dim_wrestler_key,
  w.wrestler_name
from cleaned_names c
join {{ ref('dim_event') }} ev
  on c.event_name = ev.event_name
  and cast(c.event_year as string) = ev.event_year
join {{ ref('dim_wrestler') }} w
  on c.wrestler_name_cleaned = w.wrestler_name
order by ev.dim_event_key, c.match_number, c.participant_role, w.wrestler_name

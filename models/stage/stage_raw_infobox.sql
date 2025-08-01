with source as (
    select * from {{ source('analytics_prod', 'raw_infobox') }}
),

city_parts as (
    select
        *,
        -- Remove bracketed refs, split on comma
        ARRAY(
          SELECT trim(x) FROM UNNEST(SPLIT(regexp_replace(city, r'\[.*?\]', ''), ',')) as x
          WHERE trim(x) != ''
        ) as city_array
    from source
),

us_states as (
    select [
      'Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida','Georgia',
      'Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine','Maryland','Massachusetts',
      'Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire','New Jersey',
      'New Mexico','New York','North Carolina','North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island',
      'South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont','Virginia','Washington','West Virginia',
      'Wisconsin','Wyoming','D.C.','DC'
    ] as states
),

cleaned as (
    select
        -- Parse city
        city_array[SAFE_OFFSET(0)] as event_city,
        -- Logic for state/country:
        case
            when array_length(city_array) >= 3 then city_array[SAFE_OFFSET(1)]
            when array_length(city_array) = 2 and UPPER(REGEXP_REPLACE(city_array[SAFE_OFFSET(1)], r'[.]', '')) in (select UPPER(REGEXP_REPLACE(state, r'[.]', '')) from unnest(u.states) as state)
                then city_array[SAFE_OFFSET(1)]
            else null
        end as event_state,
        case
            when array_length(city_array) >= 3 then city_array[SAFE_OFFSET(2)]
            when array_length(city_array) = 2 and UPPER(REGEXP_REPLACE(city_array[SAFE_OFFSET(1)], r'[.]', '')) in (select UPPER(REGEXP_REPLACE(state, r'[.]', '')) from unnest(u.states) as state)
                then 'United States'
            when array_length(city_array) = 2 then city_array[SAFE_OFFSET(1)]
            else 'United States'
        end as event_country,
        
        -- Event name/year
        regexp_extract(event, r'^(.*?)\s*\(') as event_name,
        regexp_extract(event, r'\((\d{4})\)') as event_year,

        -- buy_rate: remove [brackets] and commas, cast to int
        safe_cast(
            regexp_replace(
                regexp_replace(buy_rate, r'\[.*?\]', ''), 
                r',', ''
            ) as int64
        ) as event_buy_rate,

        -- attendance: extract first numeric chunk, remove commas, cast to int
        safe_cast(
            regexp_replace(
                regexp_extract(regexp_replace(attendance, r'\[.*?\]', ''), r'(\d[\d,]*)'),
                r',', ''
            ) as int64
        ) as event_attendance,

        -- event_date: parse cleaned string to DATE (SAFE for robustness)
        SAFE.PARSE_DATE('%B %d, %Y', regexp_extract(date, r'([A-Za-z]+ \d{1,2}, \d{4})')) as event_date,

        -- venue_clean: remove brackets
        trim(
            regexp_replace(venue, r'\[.*?\]', '')
        ) as event_venue,

        -- promotion_clean: normalize names
        case
            when promotion = 'WWE' or promotion = 'World Wrestling Entertainment' then 'WWE'
            when promotion = 'World Wrestling Federation' then 'WWF'
            else promotion
        end as promotion

    from city_parts cp
    cross join us_states u
)

select
    event_name,
    event_year,
    event_date,
    event_city,
    event_state,
    event_country,
    event_venue,
    event_buy_rate,
    event_attendance,
    promotion
from cleaned
where
    event_year is not null
    and SAFE_CAST(event_year AS int64) between 1980 and 2100
    and event_name is not null
    and trim(event_name) != ''

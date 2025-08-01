SELECT
  f.match_number,
  e.event_name,
  e.event_year,
  mt.match_type,
  t.title,
  f.is_championship_match,
  f.match_duration_seconds,   -- <-- Added match duration here
  bw.participant_role,
  w.wrestler_name
FROM {{ ref('fact_match_metrics') }} f
JOIN {{ ref('dim_event') }} e
  ON e.dim_event_key = f.dim_event_key
JOIN {{ ref('dim_match_type') }} mt
  ON mt.dim_match_type_key = f.dim_match_type_key
LEFT JOIN {{ ref('dim_title') }} t
  ON t.dim_title_key = f.dim_title_key
JOIN {{ ref('bridge_match_wrestler') }} bw
  ON bw.dim_event_key = f.dim_event_key
  AND bw.match_number = f.match_number
JOIN {{ ref('dim_wrestler') }} w
  ON w.dim_wrestler_key = bw.dim_wrestler_key
ORDER BY
  f.dim_event_key, f.match_number, bw.participant_role

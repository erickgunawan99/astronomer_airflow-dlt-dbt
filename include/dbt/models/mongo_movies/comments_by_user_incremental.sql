{{ config(
  materialized='incremental',
  unique_key=['name', 'email'],
) }}



WITH max_existing AS (
  {% if is_incremental() %}
      SELECT max(load_date AT TIME ZONE 'UTC') AS max_load_date
      FROM {{ this }}
  {% else %}
      SELECT null::timestamp AS max_load_date
  {% endif %}
),

filtered_new AS (
  SELECT *
  FROM {{ ref('stg_comments') }} c
  JOIN max_existing m ON true
  WHERE (c.load_date AT TIME ZONE 'UTC') > coalesce(m.max_load_date, '1900-01-01'::timestamp)
),

new_agg AS (
  SELECT
    name,
    email,
    count(*) AS comment_count,
    array_agg(distinct movie_id) AS movie_ids,
    max(load_date) AS load_date
  FROM filtered_new
  GROUP BY name, email
),

final AS (
  {% if is_incremental() %}
    SELECT * FROM {{ this }}
    UNION ALL
    SELECT * FROM new_agg
  {% else %}
    SELECT * FROM new_agg
  {% endif %}
),

flat AS (
  {% if is_incremental() %}
   SELECT
      name,
      email,
      total_comment_count,
      unnest(movie_ids) AS movie_id,
      load_date
    FROM (
        SELECT
            name,
            email,
            SUM(comment_count) OVER (PARTITION BY name, email) AS total_comment_count,
            movie_ids,
            load_date
        FROM final
    )
  {% else %}
    SELECT * FROM new_agg
  {% endif %}
)

{% if is_incremental() %}
SELECT
  name,
  email,
  MAX(total_comment_count) as comment_count,
  array_agg(DISTINCT movie_id ORDER BY movie_id) AS movie_ids,
  max(load_date) AS load_date
FROM flat
GROUP BY name, email
ORDER BY comment_count DESC
{% else %}
SELECT * FROM new_agg
{% endif %}

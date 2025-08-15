



WITH max_existing AS (
  
      SELECT max(load_date AT TIME ZONE 'UTC') AS max_load_date
      FROM "test_db"."public_marts"."comments_by_user_incremental"
  
),

filtered_new AS (
  SELECT *
  FROM "test_db"."public_staging"."stg_comments" c
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
  
    SELECT * FROM "test_db"."public_marts"."comments_by_user_incremental"
    UNION ALL
    SELECT * FROM new_agg
  
),

flat AS (
  
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
  
)


SELECT
  name,
  email,
  MAX(total_comment_count) as comment_count,
  array_agg(DISTINCT movie_id ORDER BY movie_id) AS movie_ids,
  max(load_date) AS load_date
FROM flat
GROUP BY name, email
ORDER BY comment_count DESC

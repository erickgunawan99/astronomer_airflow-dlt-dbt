-- depends_on: {{ ref('stg_comments') }}

SELECT name, email, count(text) as total_comment_count, 
count(distinct movie_id) as total_movies_count
FROM {{ ref('stg_comments') }} 
GROUP BY name, email
ORDER BY email
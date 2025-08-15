SELECT 
    c.email,
    COALESCE(array_agg(DISTINCT gd.director) FILTER (
      WHERE c.text ~* '\mgood\M' AND c.text !~* 'not\s+good'
    ), '{}') AS fav_directors,

    COALESCE(array_agg(DISTINCT gd.genre) FILTER (
      WHERE c.text ~* '\mgood\M' AND c.text !~* 'not\s+good'
    ), '{}') AS fav_genres,

    COALESCE(array_agg(DISTINCT gd.title) FILTER (
      WHERE c.text ~* '\mgood\M' AND c.text !~* 'not\s+good'
    ), '{}') AS fav_movies
FROM {{ ref('stg_comments') }} c
JOIN {{ ref('movies_genres_directors') }} gd ON c.movie_id = gd.movie_id
GROUP BY c.email

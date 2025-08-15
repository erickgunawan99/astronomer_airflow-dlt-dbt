
  
    

  create  table "test_db"."public_marts"."genres_and_directors_with_good_comment_each_user__dbt_tmp"
  
  
    as
  
  (
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
FROM "test_db"."public_staging"."stg_comments" c
JOIN "test_db"."public_marts"."movies_genres_directors" gd ON c.movie_id = gd.movie_id
GROUP BY c.email
  );
  
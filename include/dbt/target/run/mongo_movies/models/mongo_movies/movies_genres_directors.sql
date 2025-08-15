
  
    

  create  table "test_db"."public_marts"."movies_genres_directors__dbt_tmp"
  
  
    as
  
  (
    SELECT m.title, m._id AS movie_id, g.genre, d.director FROM "test_db"."public_staging"."stg_movies" m 
JOIN "test_db"."public_staging"."stg_movies_genres" g ON m._dlt_id = g.movie_id
JOIN "test_db"."public_staging"."stg_movies_directors" d ON m._dlt_id = d.movie_id
  );
  
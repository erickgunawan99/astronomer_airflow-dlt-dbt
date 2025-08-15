
  create view "test_db"."public_staging"."stg_movies_genres__dbt_tmp"
    
    
  as (
    SELECT value AS genre, _dlt_parent_id AS movie_id 
FROM "test_db"."mongo_data"."movies__genres"
  );
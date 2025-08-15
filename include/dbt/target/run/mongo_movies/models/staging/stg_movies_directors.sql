
  create view "test_db"."public_staging"."stg_movies_directors__dbt_tmp"
    
    
  as (
    SELECT value AS director, _dlt_parent_id AS movie_id 
FROM "test_db"."mongo_data"."movies__directors"
  );
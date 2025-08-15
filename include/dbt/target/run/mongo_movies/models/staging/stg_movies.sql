
  create view "test_db"."public_staging"."stg_movies__dbt_tmp"
    
    
  as (
    SELECT * FROM "test_db"."mongo_data"."movies"
  );
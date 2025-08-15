
  create view "test_db"."public_staging"."stg_comments__dbt_tmp"
    
    
  as (
    select c.*, d.inserted_at as load_date from "test_db"."mongo_data"."comments" c
join "test_db"."mongo_data"."_dlt_loads" d
on c._dlt_load_id = d.load_id
  );
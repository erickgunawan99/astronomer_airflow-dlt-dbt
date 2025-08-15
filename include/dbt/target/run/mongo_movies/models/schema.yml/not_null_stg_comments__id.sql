
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _id
from "test_db"."public_staging"."stg_comments"
where _id is null



  
  
      
    ) dbt_internal_test
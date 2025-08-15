
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from "test_db"."public_marts"."comments_by_user"
where email is null



  
  
      
    ) dbt_internal_test
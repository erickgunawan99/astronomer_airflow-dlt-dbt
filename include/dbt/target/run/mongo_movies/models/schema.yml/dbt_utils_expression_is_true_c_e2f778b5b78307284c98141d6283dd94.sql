
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "test_db"."public_marts"."comments_by_user"

where not(total_comment_count comments_by_user.total_comment_count > 0)


  
  
      
    ) dbt_internal_test
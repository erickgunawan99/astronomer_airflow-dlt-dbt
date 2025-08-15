
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from "test_db"."public_marts"."genres_and_directors_with_good_comment_each_user"
where email is null



  
  
      
    ) dbt_internal_test
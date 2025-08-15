
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    (name || '_' || email) as unique_field,
    count(*) as n_records

from "test_db"."public_marts"."comments_by_user"
where (name || '_' || email) is not null
group by (name || '_' || email)
having count(*) > 1



  
  
      
    ) dbt_internal_test
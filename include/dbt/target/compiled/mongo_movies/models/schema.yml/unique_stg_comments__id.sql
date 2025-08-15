
    
    

select
    _id as unique_field,
    count(*) as n_records

from "test_db"."public_staging"."stg_comments"
where _id is not null
group by _id
having count(*) > 1



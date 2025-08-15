
    
    

select
    email as unique_field,
    count(*) as n_records

from "test_db"."public_marts"."genres_and_directors_with_good_comment_each_user"
where email is not null
group by email
having count(*) > 1



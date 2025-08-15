
    
    

with child as (
    select movie_id as from_field
    from "test_db"."public_staging"."stg_comments"
    where movie_id is not null
),

parent as (
    select _id as to_field
    from "test_db"."public_staging"."stg_movies"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null







select
    1
from "test_db"."public_marts"."comments_by_user"

where not(total_comment_count total_comment_count > 0)


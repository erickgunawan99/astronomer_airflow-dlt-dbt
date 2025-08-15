
  
    

  create  table "test_db"."public_marts"."comments_by_user__dbt_tmp"
  
  
    as
  
  (
    -- depends_on: "test_db"."public_staging"."stg_comments"

SELECT name, email, count(text) as total_comment_count, 
count(distinct movie_id) as total_movies_count
FROM "test_db"."public_staging"."stg_comments" 
GROUP BY name, email
ORDER BY email
  );
  
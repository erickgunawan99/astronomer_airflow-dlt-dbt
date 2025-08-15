
      
        delete from "test_db"."public_marts"."comments_by_user_incremental" as DBT_INTERNAL_DEST
        where (name, email) in (
            select distinct name, email
            from "comments_by_user_incremental__dbt_tmp222357455975" as DBT_INTERNAL_SOURCE
        );

    

    insert into "test_db"."public_marts"."comments_by_user_incremental" ("name", "email", "comment_count", "movie_ids", "load_date")
    (
        select "name", "email", "comment_count", "movie_ids", "load_date"
        from "comments_by_user_incremental__dbt_tmp222357455975"
    )
  
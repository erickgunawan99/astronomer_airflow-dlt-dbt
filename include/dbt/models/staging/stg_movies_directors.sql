SELECT value AS director, _dlt_parent_id AS movie_id 
FROM {{ source('mongo_data', 'movies__directors') }}
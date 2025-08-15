SELECT m.title, m._id AS movie_id, g.genre, d.director FROM {{ ref('stg_movies') }} m 
JOIN {{ ref('stg_movies_genres') }} g ON m._dlt_id = g.movie_id
JOIN {{ ref('stg_movies_directors') }} d ON m._dlt_id = d.movie_id
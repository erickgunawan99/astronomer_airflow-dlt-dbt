select c.*, d.inserted_at as load_date from {{ source('mongo_data', 'comments') }} c
join {{ source('mongo_data', '_dlt_loads') }} d
on c._dlt_load_id = d.load_id 
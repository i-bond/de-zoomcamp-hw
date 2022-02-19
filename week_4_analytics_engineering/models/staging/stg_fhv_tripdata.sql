{{ config(materialized='view') }}

-- select * from {{ source('hw','table_fhv_tripdata') }}
-- limit 100

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,     
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(PULocationID as integer) as  pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    cast(SR_Flag as integer) as sr_flag
from {{ source('hw','table_fhv_tripdata') }}


-- dbt build --m <model.sql> --var 'is_test_run: false' switch to true
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
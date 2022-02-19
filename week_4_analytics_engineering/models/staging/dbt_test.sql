{{ config(materialized='view') }}


select * from {{ source('staging','green_tripdata_csv') }}
limit 100
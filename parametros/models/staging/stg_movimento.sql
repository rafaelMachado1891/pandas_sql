{{ config(materialized='view') }}

with source_data as (

    select * from {{ source('db', 'MOVIMENTO') }}

)

select *
from source_data

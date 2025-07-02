

{{ config(materialized='view') }}

with source_data as (

    select * from {{ source('parametros_db', 'MOVIMENTO') }}
),

tabela_transformada as(
select 
    cast(codigo as text) as codigo,
    cast(descricao as text),
    cast(data_baixa as date),
    cast(total_movimento as float) as quantidade,
    cast(grupo  as text) 
from source_data

)

select *
from tabela_transformada


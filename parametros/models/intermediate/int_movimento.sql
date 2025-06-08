with int_movimento as (
    SELECT 
        *
    FROM 
        {{ ref('stg_movimento') }}
)

select * from int_movimento
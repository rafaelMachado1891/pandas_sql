with estaticas_dia as (
    select
        *

    from {{ ref('stg_estatisticas_dia') }}

),
estaticas_mensal as (
    select
        *

    from {{ ref('stg_estatisticas_mes') }}
),
agrupamento as (
    select
        a.codigo
    from estaticas_dia a 
    join estaticas_mensal b
    on a.codigo = b.codigo
)
select * from agrupamento



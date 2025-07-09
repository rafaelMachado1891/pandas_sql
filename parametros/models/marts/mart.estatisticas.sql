with mart_produtos as (
    select
        *
    from {{ ref('int_produtos') }}
),
estatisticas as (
    select
        *
    from {{ ref('int_estatisticas')}}
)
select 
    a.codigo,
    a.descricao,
    b.soma,
    b.soma_mensal,
    b.media_mensal,
    b.media_dia

from mart_produtos a join estatisticas b on a.codigo = b.codigo 
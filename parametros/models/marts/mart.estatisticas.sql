with mart_produtos as (
    select
        *
    from {{ ref('int_produtos') }}
),
estatisticas as (
    select
        *
    from {{ ref('int_estatisticas')}}
),
resultado as (
    SELECT 
        a.codigo,
        a.descricao,
        a.grupo,
        b.*
    FROM mart_produtos a
    JOIN estatisticas b ON a.codigo b.codigo
)
select * from resultado



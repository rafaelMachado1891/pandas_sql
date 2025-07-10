with mart_produtos as (
    select
        *
    from public.int_produtos
),
estatisticas as (
    select
        *
    from public.int_estatisticas
),
resultado as (
    SELECT 
        a.codigo as codigo_item,
        a.descricao,
        a.grupo,
        b.*
    FROM mart_produtos a
    JOIN estatisticas b ON a.codigo = b.codigo
)
select 
    codigo_item,
    descricao,
    grupo,
    soma,
    maximo,
    minimo,
    media_dia,
    desvio_padrao,
    mediana,
    contagem,
    primeiro_quartil,
    terceiro_quartil,
    maximo_mensal,
    minimo_mensal,
    media_mensal,
    desvio_padrao_mensal,
    mediana_mensal,
    contagem_mensal,
    primeiro_quartil_mensal,
    terceiro_quartil_mensal
from resultado




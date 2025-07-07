with produtos as (
    select
        codigo,
        MIN(descricao) as descricao
    from {{ ref('int_movimento') }}
    group by codigo
)
select * from produtos
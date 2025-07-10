with produtos as (
    select 
        codigo,
        min(descricao) as descricao,
        grupo
    from {{ ref('stg_movimento') }}
    group by codigo,grupo
)

select * from produtos order by codigo
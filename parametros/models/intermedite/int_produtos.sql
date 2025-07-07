with produtos as (
    select 
        codigo,
        min(descricao) as descricao
    from stg_movimento
    group by codigo
)

select * from produtos order by codigo
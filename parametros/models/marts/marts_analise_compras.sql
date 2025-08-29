WITH compras AS (
    SELECT 
        *
    FROM {{ ref("int_compras") }}
),
estatitsticas_produtos AS (
    SELECT
        *
    FROM {{ ref("int_estaticas_produtos") }}
),
resultado AS (
      SELECT 
        a.data_entrega,
        a.fornecedor,
        a.numero_oc,
        a.codigo_produto,
        a.descricao,
		a.quantidade,
        a.valor_total,
        b.calculo_estoque,
        CASE WHEN
            quantidade > B.calculo_estoque * 1.2 THEN 'VERIFICAR ORDEM DE COMPRA'
             ELSE 'OK'
        END AS check_oc
    FROM compras a  
    JOIN estatitsticas_produtos b
    ON a.codigo_produto = b.codigo
)

SELECT * FROM resultado WHERE 2, 1, 4
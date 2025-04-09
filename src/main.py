from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

username = os.getenv("DB_USERNAME")
password = quote_plus(os.getenv("DB_PASSWORD"))
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

# URL de conexão com o banco de dados
DATABASE_URL = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATABASE_URL)

query = """
    WITH VENDAS AS (	
	SELECT 
	 A.Data_EM
	,A.Numero
	,B.Codigo
	,C.Descricao
	,C.Referencia
	,C.Produtos_Volume          AS QUANTIDADE_POR_EMBALAGEM
	,SUM(B.Quantidade )           AS QUANTIDADE
	,SUM(B.Quantidade * b.Preco)  AS TOTAL_VENDA
	,B.P_Desconto
	,C.Complemento
	,C.Grupo 
	,D.descricao        AS MARCA
	,MONTH(A.DATA_EM)   AS MES
	,YEAR(A.DATA_EM)    AS ANO
	FROM NotaS1 A 
	JOIN
	NOTAS2 B
	ON A.Numero = B. Numero
	JOIN
	Produtos C
	ON B.Codigo = C.Codigo
	JOIN 
	marcas D
	ON 
	C.Marca = D.codigo
	WHERE 
	A.DATA_EM  >= '2024-01-01'

	Group by
	 A.Data_EM
	,A.Numero
	,B.Codigo
	,C.Descricao
	,C.Referencia
	,C.Complemento
	,D.descricao        
	,MONTH(A.DATA_EM)   
	,YEAR(A.DATA_EM)  
	,C.Produtos_Volume 
	,C.Grupo
	,B.P_Desconto
	
	) 

	SELECT
	MARCA   AS familia,
	MES     AS mes,
	Codigo         AS codigo,
	REFERENCIA   AS referencia,	
	SUM(QUANTIDADE)  AS quantidade,
	ROUND(SUM(TOTAL_VENDA) - (SUM(TOTAL_VENDA * P_Desconto / 100)),2) AS faturamento
		
	FROM VENDAS
	WHERE Data_EM >= '2024-01-01' AND Data_EM <= '2024-12-31' AND MARCA = 'INFINITY' AND GRUPO = '5009'
	
	GROUP BY MARCA, 	Codigo ,	REFERENCIA, MES,Grupo
	ORDER BY 4,3 DESC

"""

with engine.connect() as connection:
    result = connection.execute(text(query))
    
    # Carregar os resultados em um DataFrame
    df = pd.DataFrame(result.fetchall(), columns=result.keys())

df
df['quantidade'] = df['quantidade'].astype(int)
df[['codigo','familia','referencia',]] = df[['codigo','familia','referencia',]].astype('str')

df['codigo'].sort_values(ascending=True)

df['codigo'] = df['codigo'].sort_values(ascending=True)


agregacoes = df.groupby(by=['codigo'],as_index=False).agg({'quantidade':['min','max','sum','mean','var', 'std']})

agregacoes.columns = ['codigo', 'minimo','maximo','soma','media','variancia', 'desvio_padrao']

agregacoes['amplitude'] = agregacoes['maximo'] - agregacoes['minimo']


produtos = df[['codigo','referencia']].drop_duplicates()

df = produtos.merge(agregacoes,left_on=['codigo'],right_on=['codigo'],how='inner')


df['media'] = df['media'].astype(str).str.replace('.',',')
df['variancia'] = df['variancia'].astype(str).str.replace('.',',')
df['desvio_padrao'] = df['desvio_padrao'].astype(str).str.replace('.',',')


df.to_csv('dados.csv',sep='|',index=False)
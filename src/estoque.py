from urllib.parse import quote_plus
import pandas as pd
import os
from sqlalchemy import create_engine, text
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
    WITH movimento_estoque AS (
        SELECT 
            A.Numero,
            A.Codigo,
            D.Descricao,
            d.Custo_Medio,
            A.Quantidade,
            r.DATA_BAIXA,
        
            YEAR(r.DATA_BAIXA)  AS ano,
            MONTH(r.DATA_BAIXA) AS mes
        FROM 
            Ords3 A 
        JOIN 
            (SELECT * FROM Ords2) r ON A.Numero = r.Numero
        LEFT JOIN
            (   
                SELECT 
                    X.CodProduto AS CODIGO,
                    X.NumeroRequisicao AS NUMERO,
                    X.Quantidade,
                    Y.Data AS DATA_BAIXA
                FROM 
                    RequisicaoItens X
                JOIN
                    (SELECT Numero, Data FROM Requisicao) Y ON X.NumeroRequisicao = Y.Numero
                WHERE
                    situacao = 2 
            ) C ON A.NUMERO = C.NUMERO 
            LEFT JOIN
            Produtos D ON A.Codigo = D.Codigo
        WHERE 
            YEAR(r.DATA_BAIXA) >= 2023
    ),
    tabela_produtos AS (
        SELECT 
            a.codigo,
            a.descricao,
            b.Descricao AS grupo
        FROM Produtos a 
        JOIN Grupos b
        ON a.Grupo = b.Codigo
        WHERE Situacao = 0 and
        a.grupo NOT IN(5000, 3068)
    ),
    tabela_resultado AS (
        SELECT 
            a.codigo,
            a.descricao,
            a.grupo,
            b.Quantidade AS total_movimento,
            b.Data_Baixa AS data_baixa,
            b.ano,
            b.mes

        FROM tabela_produtos a 
        JOIN movimento_estoque b
        ON a.codigo = b.Codigo
    )
    select * from tabela_resultado
"""

with engine.connect() as connection:
    result = connection.execute(text(query))
    
    # Carregar os resultados em um DataFrame
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
tipo_dados = {
    'codigo': str,
    'descricao': str,
    'data_baixa':'datetime64[ns]',
    'mes': int,
    'ano': int,
    'total_movimento': int,
    'grupo': str
}

df = df.astype(tipo_dados)

selecao_colunas = ["codigo", "descricao", "data_baixa", "total_movimento", "grupo"]

df = df[selecao_colunas]

print(df)
# conexao com o banco de dados de destino

USERNAME_POSTGRE = os.getenv("USER_POSTGRE")
PASSWORD_POSTGRE = quote_plus(os.getenv("PASSWORD_POSTGRE"))
HOST_POSTGRE = os.getenv("HOST_POSTGRE")
DB_POSTGRE = os.getenv("DB_POSTGRE")
PORT_POSTGRE = os.getenv("PORT_POSTGRE")

connection_string = f"postgresql://{USERNAME_POSTGRE}:{PASSWORD_POSTGRE}@{HOST_POSTGRE}:{PORT_POSTGRE}/{DB_POSTGRE}"

target_engine = create_engine(connection_string)

with target_engine.connect() as connection:
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(
        text("DROP TABLE IF EXISTS MOVIMENTO CASCADE")
    )

df.to_sql(
        name='MOVIMENTO',         
        con=target_engine,           
        schema=os.getenv('SCHEMA'),  
        if_exists='replace',          
        index=False                  
)
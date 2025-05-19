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
    WITH movimento_estoque as (
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
            YEAR(r.DATA_BAIXA) >= 2024 and
            D.Codigo = '00621'
    )

    SELECT * FROM movimento_estoque

"""

with engine.connect() as connection:
    result = connection.execute(text(query))
    
    # Carregar os resultados em um DataFrame
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
print(df)

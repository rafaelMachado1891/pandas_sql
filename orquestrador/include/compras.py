from urllib.parse import quote_plus
import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

def coletar_dados_compras():
       
    
    load_dotenv()

    username = os.getenv("DB_USERNAME")
    password = quote_plus(os.getenv("DB_PASSWORD"))
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_NAME")


    DATABASE_URL = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

    engine = create_engine(DATABASE_URL)

    query = """
        with ordem_compras as(
            select 
                Numero,
                Codigo,
                Descr,
                CASE 
                    WHEN UN = 'CT' THEN  Saldo * 100 
                    ELSE Saldo
                END AS saldo,
                preco,
                saldo * preco as valor_total,
                Data_ET,
                UN
            FROM Ordc2 where Situacao = 'Pendente'  and (Situacao_MRP = 2 or Situacao_MRP = 0)
        ),
        ordem_compra_terceiro AS (
            SELECT 
                a.Numero,
                b.Codigo,
                b.Razao,
                A.Situacao
            FROM Ordc1 a
            LEFT JOIN Terceiros b
            ON a.Codigo_For = b.codigo
        ),
        resultado AS (
            SELECT
                a.Numero as numero,
                a.Codigo as codigo,
                a.Descr as descricao,
                a.UN as un,
                a.saldo as saldo,
                a.preco as preco,
                a.valor_total as valor_total,
                a.Data_ET as data_entrega,
                b.Razao as razao,
                b.codigo as cod_terceiro

            FROM ordem_compras a
            JOIN ordem_compra_terceiro b
            ON a.Numero = b.numero
        )
        SELECT * FROM resultado
    """

    with engine.connect() as connection:
        result = connection.execute(text(query))
        
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
    tipo_de_dados = {
        'numero': int,
        'codigo': str,
        'descricao': str,
        'saldo': float,
        'preco': float,
        'valor_total': float      
    }

    df['data_entrega'] = pd.to_datetime(df['data_entrega'], errors='coerce')

    df=df.astype(tipo_de_dados)

    USERNAME_POSTGRE = os.getenv("USER_POSTGRES")
    PASSWORD_POSTGRE = quote_plus(os.getenv("PASSWORD_POSTGRES"))
    HOST_POSTGRE = os.getenv("HOST_POSTGRES")
    DB_POSTGRE = os.getenv("DB_POSTGRES")
    PORT_POSTGRE = os.getenv("PORT_POSTGRES")

    connection_string = f"postgresql://{USERNAME_POSTGRE}:{PASSWORD_POSTGRE}@{HOST_POSTGRE}:{PORT_POSTGRE}/{DB_POSTGRE}"

    target_engine = create_engine(connection_string)

    with target_engine.execution_options(isolation_level="AUTOCOMMIT").connect() as connection:
        
        connection.execute(text('DROP TABLE IF EXISTS "COMPRAS" CASCADE'))  
            

    df.to_sql(
        name='COMPRAS',           
        con=target_engine,            
        schema=os.getenv('SCHEMA'), 
        if_exists='append',         
        index=False                 
    )

        
    print('Dados carregados no banco com sucesso! ')
    
coletar_dados_compras()
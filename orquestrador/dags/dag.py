from airflow.decorators import dag, task
from include.controller import coletar_dados_compras, coletar_dados_estoque
from datetime import datetime


@dag(
    dag_id="rodar_scripts_python",
    description="pipeline que extrai os dados de movimento de estoque e compra de material",
    start_date=datetime(2025, 8, 24),
    schedule="* * * * *",
    catchup=False
)

def rodar_scripts_python():
    
    @task(task_id="coletar_dados_estoque")
    def task_coletar_dados_estoque():
        return coletar_dados_estoque()
    
    
    @task(task_id="coletar_dados_compras")
    def task_coletar_dados_compras():
        return coletar_dados_compras()
    
    
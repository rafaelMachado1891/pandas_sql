from airflow.models import Variable
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
from pendulum import datetime
from airflow.datasets import Dataset

def create_cosmos_dag():
    # Lê a variável apenas dentro da função para evitar timeout
    dbt_env = Variable.get("dbt_env", default_var="dev").lower()
    if dbt_env not in ("dev", "prod"):
        raise ValueError(f"dbt_env inválido: {dbt_env!r}, use 'dev' ou 'prod'")

    # Configurações para ambiente de desenvolvimento
    profile_config_dev = ProfileConfig(
        profile_name="parametros",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="docker_postgres_db",
            profile_args={
                "schema": "public" #"public"
            },
        ),
    )

    # Configurações para ambiente de produção
    profile_config_prod = ProfileConfig(
        profile_name="parametros",
        target_name="prod",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="render_postgres_db",
            profile_args={
                "schema": "public"
            }
        ),
    )

    profile_config = profile_config_dev if dbt_env == "dev" else profile_config_prod

    return DbtDag(
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dbt/parametros",  # cuidado com o caminho conforme override.yml
            project_name="parametros"
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        ),
        operator_args={
            "target": profile_config.target_name,
        },
        
        schedule="@daily",
        start_date=datetime(2025, 5, 30),
        catchup=False,
        dag_id=f"dag_dw_dbt_{dbt_env}",
        default_args={"retries": 2},
    )


# Criação da DAG
my_cosmos_dag = create_cosmos_dag()

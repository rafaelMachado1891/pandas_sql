from airflow.models import Variable
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
from pendulum import datetime

# Configurações para ambiente de desenvolvimento
profile_config_dev = ProfileConfig(
    profile_name="parametros",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="docker_postgres_db",
        profile_args={
            "schema": "public"
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

# Lê a variável do ambiente para saber qual profile usar
dbt_env = Variable.get("dbt_env", default_var="dev").lower()
if dbt_env not in ("dev", "prod"):
    raise ValueError(f"dbt_env inválido: {dbt_env!r}, use 'dev' ou 'prod'")

# Aponta para o profile correto
profile_config = profile_config_dev if dbt_env == "dev" else profile_config_prod

# Define a DAG principal do Cosmos com dbt
my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/parametros",
        project_name="parametros",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        "target": profile_config.target_name,
        "enable_asset_metadata": False  # Tentar desabilitar via parâmetro
    },
    schedule="@daily",
    start_date=datetime(2025, 5, 30),
    catchup=False,
    dag_id=f"dag_dw_dbt_{dbt_env}",
    default_args={"retries": 2},
)

# ✅ Solução complementar (necessária em Cosmos 1.10.1)
# Força a remoção dos datasets automáticos
my_cosmos_dag.operator_args["input_dataset"] = None
my_cosmos_dag.operator_args["output_dataset"] = None

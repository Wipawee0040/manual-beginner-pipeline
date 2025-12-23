from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

import sys
from pathlib import Path

# --- path setup ---
PYTHON_EXEC_PATH = "/home/wipawee/Roadmap_DE_python/airflow_dbt_env/bin/python"
PYTHON_SCRIPT = Path("/home/wipawee/Roadmap_DE_python/load_raw.py")
DBT_EXECUTABLE_PATH = "/home/wipawee/Roadmap_DE_dbt_prosgres/postgres_env/bin/dbt"
DBT_PROJECT_DIR = Path("/home/wipawee/Roadmap_DE_dbt_prosgres/mini_dbt_pg")


# --- DAG definition ---
with DAG(
    dag_id="order_items_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # manual run
    catchup=False,
    tags=["portfolio", "dbt"],
) as dag:

    load_raw = BashOperator(
        task_id="load_raw_order_items",
        bash_command=f"{PYTHON_EXEC_PATH} {PYTHON_SCRIPT}",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE_PATH} run --select staging",
    )

    dbt_run_fact = BashOperator(
        task_id="dbt_run_fact",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE_PATH} run --select fact_table",
    )

  
    load_raw >> dbt_run_staging >> dbt_run_fact 
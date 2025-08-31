import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/home/airflow/gcs/dags/dbt/olist"  # adjust if different
DEFAULT_ARGS = {"retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="dbt_build_olist",
    start_date=datetime(2025, 8, 25),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt","analytics","abdelrahmanem"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .."
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .."
    )

    dbt_deps >> dbt_run >> dbt_test

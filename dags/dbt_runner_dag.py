from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# -------------------- Paths (match your Composer bucket layout) --------------------
DBT_DIR = "/home/airflow/gcs/dags/abdelrahman/dbt/olist"   # contains dbt_project.yml
PROFILES_DIR = "/home/airflow/gcs/dags/abdelrahman/dbt"    # contains profiles.yml

# -------------------- Defaults --------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dbt_build_olist",
    description="Run dbt (deps/run/test) for the Olist project on BigQuery",
    start_date=datetime(2025, 8, 25),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "analytics", "abdelrahman"],
) as dag:

    # Quick sanity check: list dbt folder so we see it in logs if path is wrong
    ls_dbt_dir = BashOperator(
        task_id="ls_dbt_dir",
        bash_command=(
            "echo 'Listing dags root:' && ls -la /home/airflow/gcs/dags || true; "
            f"echo '---\\nListing {DBT_DIR}:' && ls -la {DBT_DIR} || true; "
            f"echo '---\\nListing {PROFILES_DIR}:' && ls -la {PROFILES_DIR} || true"
        ),
    )

    # Optional: print dbt version to logs (helps when debugging)
    dbt_version = BashOperator(
        task_id="dbt_version",
        bash_command="dbt --version",
    )

    # Install packages referenced in packages.yml (if any)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {PROFILES_DIR}",
    )

    # Build all models (staging -> intermediate -> marts)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {PROFILES_DIR} --no-use-colors",
    )

    # Run tests (schema + data tests)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {PROFILES_DIR} --no-use-colors",
    )

    ls_dbt_dir >> dbt_version >> dbt_deps >> dbt_run >> dbt_test

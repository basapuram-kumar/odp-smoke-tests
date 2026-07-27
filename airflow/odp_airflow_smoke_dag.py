from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="odp_airflow_smoke",
    default_args=default_args,
    description="ODP Airflow sample smoke DAG",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["odp", "smoke"],
) as dag:
    BashOperator(
        task_id="echo_ok",
        bash_command="echo OK_AIRFLOW_SMOKE && date",
    )

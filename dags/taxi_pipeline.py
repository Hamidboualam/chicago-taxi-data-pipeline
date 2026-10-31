from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SRC_DIR = "/opt/airflow/src"

# Lancement manuel uniquement (schedule=None) : la période de données est fixe
with DAG(
    dag_id="taxi_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"python {SRC_DIR}/ingest_bronze.py",
    )
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"python {SRC_DIR}/bronze_to_silver.py",
    )
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"python {SRC_DIR}/silver_to_gold.py",
    )
    check_gold = BashOperator(
        task_id="check_gold",
        bash_command=f"python {SRC_DIR}/check_gold.py",
    )

    ingest_bronze >> bronze_to_silver >> silver_to_gold >> check_gold

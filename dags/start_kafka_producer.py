from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="start_kafka_producer",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["kafka", "producer", "streaming"],
) as dag:

    start_producer = BashOperator(
        task_id="start_producer",
        bash_command="python /opt/airflow/producer_raw.py",
        env={
            "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
            "POLL_INTERVAL_SECONDS": "1.0",
            "API_URL": "https://randomuser.me/api/",
        },
    )

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os


default_args = {
    'owner': 'Rishi',
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}


with DAG(
    dag_id="delta_snapshot_daily",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    s3_env = os.getenv('S3_BUCKET_NAME', '')
    if s3_env:
        s3_export = f"S3_BUCKET_NAME={s3_env} "
    else:
        s3_export = ""

    # Snapshot the gold layer to a daily folder under data/exports/gold
    snapshot_gold = BashOperator(
        task_id='snapshot_gold_to_parquet',
        bash_command=(
            s3_export +
            "spark-submit \"/opt/airflow/src/snapshot_delta.py\" gold /opt/airflow/data/exports/gold --format parquet"
        )
    )

    snapshot_gold

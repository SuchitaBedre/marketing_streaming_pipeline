from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os


default_args = {
    'owner': 'Rishi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def _resolve_paths():
    # Prefer importing project config if available, else fallback to env vars
    try:
        # try to import config from repo (Airflow should have src/ on PYTHONPATH if mounted)
        import config as cfg  # type: ignore
        s3_bucket = getattr(cfg, 'S3_BUCKET', os.getenv('S3_BUCKET_NAME', 's3a://nasa-asteroid-pipeline'))
    except Exception:
        s3_bucket = os.getenv('S3_BUCKET_NAME', 's3a://nasa-asteroid-pipeline')

    bronze = f"{s3_bucket}/delta/asteroid_bronze"
    silver = f"{s3_bucket}/delta/asteroid_silver"
    gold = f"{s3_bucket}/delta/asteroid_gold"
    return bronze, silver, gold


BRONZE_TABLE_PATH, SILVER_TABLE_PATH, GOLD_TABLE_PATH = _resolve_paths()


with DAG(
    dag_id="nasa_asteroid_streaming_orchestration",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Build spark-submit commands and export S3_BUCKET_NAME into the execution environment.
    s3_env = os.getenv('S3_BUCKET_NAME', '')
    if s3_env:
        s3_export = f"S3_BUCKET_NAME={s3_env} "
    else:
        s3_export = ""

    # Run scripts using Spark via the CLI environment inside the Docker container
    run_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=s3_export + "spark-submit --packages io.delta:delta-spark_2.12:3.1.0 /opt/airflow/src/bronze_layer.py"
    )

    run_silver = BashOperator(
        task_id="clean_to_silver",
        bash_command=s3_export + "spark-submit --packages io.delta:delta-spark_2.12:3.1.0 /opt/airflow/src/silver_layer.py"
    )

    run_gold = BashOperator(
        task_id="features_to_gold",
        bash_command=s3_export + "spark-submit --packages io.delta:delta-spark_2.12:3.1.0 /opt/airflow/src/gold_layer.py"
    )

    run_bronze >> run_silver >> run_gold
# NASA Asteroid Pipeline

This repository implements a medallion-style streaming/batch pipeline for NASA Near-Earth Objects (NEO) data.

Quick contents:
- `src/bronze_layer.py`, `src/silver_layer.py`, `src/gold_layer.py` — streaming stages
- `src/api_ingest.py` — batch ingest from NASA API into Bronze
- `src/export_delta_to_csv.py` — helper to export Delta tables to CSV
- `src/generate_test_data.py` — sample CSV generator
- `src/config.py` — configuration (S3_BUCKET, paths, checkpoints, etc.)

## Environment
- Python 3.10+ recommended
- Spark with Delta support required for `spark-submit` runs (Delta Spark package used in commands below)

## Local testing (filesystem-backed Delta)

1. Install Python dependencies (skip Spark; install via your Spark distribution):
```bash
python -m pip install -r requirements.txt
```

2. Set a local `S3_BUCKET_NAME` so paths resolve under `data/` in this repo (PowerShell example):
```powershell
$env:S3_BUCKET_NAME='file:///C:/Users/rishi/nasa_asteroid_pipeline/data'
$env:NASA_API_KEY=''  # if using API ingest
```

3. Generate sample CSV and move into autoloader input folder:
```bash
python src/generate_test_data.py
mkdir -p data/raw_csv_landing
mv data/sample_asteroids.csv data/raw_csv_landing/
```

4. Run Bronze/Silver/Gold (requires `spark-submit` on PATH):
```bash
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 src/bronze_layer.py
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 src/silver_layer.py
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 src/gold_layer.py
```

5. Ingest from NASA API (optional):
```bash
$env:NASA_API_KEY='atRlamU8h97Mj5wYvtjsDkkNua16mjAJcFC8JUch'
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 src/api_ingest.py --start_date 2026-06-30 --end_date 2026-06-30
```

6. Export any layer to CSV for quick inspection:
```bash
spark-submit src/export_delta_to_csv.py bronze data/exports/bronze_csv
```

## Where files are stored
- Input CSVs: `data/raw_csv_landing/`
- Bronze Delta: `data/delta/asteroid_bronze/`
- Silver Delta: `data/delta/asteroid_silver/`
- Gold Delta: `data/delta/asteroid_gold/`

## Docker Compose (example)

See `docker-compose.yml` for an example that includes Postgres, Redis, Airflow webserver and scheduler, and Spark master/worker. Notes:

- The compose is illustrative for local development; you may need to adapt images, versions, and mount points for your environment.
- Ensure the Airflow containers have access to a Spark installation or are able to reach a remote Spark cluster to run `spark-submit`.

To initialize Airflow metadata DB and start services (quick workflow):

```bash
docker-compose up -d postgres redis
docker-compose up -d airflow-webserver airflow-scheduler
```

Then open the Airflow UI at http://localhost:8080. Mounts in the compose ensure `dags/` and `src/` are available inside the webserver.

### Snapshot scheduling

The repository contains `dags/snapshot_dag.py` which runs `snapshot_delta.py` daily to write a parquet snapshot of the `gold` layer to `data/exports/gold/`.

You can run a manual snapshot locally with:

```bash
spark-submit src/snapshot_delta.py gold data/exports/gold --format parquet
```


## Next steps
- Convert `from config import *` to explicit `config.` access (done).
- Add CI or unit tests for small functions.
- Provide a production-ready `docker-compose` or Helm chart tailored to your environment.

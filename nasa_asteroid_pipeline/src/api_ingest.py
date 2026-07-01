from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import requests
import config
import sys
import datetime


def main(start_date: str | None = None, end_date: str | None = None):
    api_key = config.NASA_API_KEY
    if not api_key:
        print("NASA_API_KEY not set; set env var NASA_API_KEY and retry.")
        sys.exit(1)

    today = datetime.date.today()
    sd = start_date or today.isoformat()
    ed = end_date or today.isoformat()

    params = {"start_date": sd, "end_date": ed, "api_key": api_key}
    resp = requests.get(NASA_API_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    neo_by_date = payload.get("near_earth_objects", {})

    records = []
    for date_key, neos in neo_by_date.items():
        for obj in neos:
            rec = {
                "id": obj.get("id"),
                "name": obj.get("name"),
                "absolute_magnitude_h": obj.get("absolute_magnitude_h"),
                "estimated_diameter_km": (
                    obj.get("estimated_diameter", {}).get("kilometers", {}).get("estimated_diameter_max")
                ),
                "is_potentially_hazardous_asteroid": obj.get("is_potentially_hazardous_asteroid"),
                "close_approach_date": None,
            }
            cad = obj.get("close_approach_data")
            if cad and isinstance(cad, list) and len(cad) > 0:
                rec["close_approach_date"] = cad[0].get("close_approach_date")
            records.append(rec)

    if not records:
        print("No records returned from NASA API for the given date range.")
        return

    spark = SparkSession.builder \
        .appName("NASA-API-Ingest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    df = spark.createDataFrame(records)
    df = df.withColumn("ingestion_timestamp", current_timestamp()).withColumn("source", lit("nasa_api"))

    df.write.format("delta").mode("append").option("mergeSchema", "true").save(config.BRONZE_TABLE_PATH)

    print(f"Wrote {len(records)} records to {config.BRONZE_TABLE_PATH}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest NASA NEO feed into Bronze Delta table")
    parser.add_argument("--start_date", help="YYYY-MM-DD start date", default=None)
    parser.add_argument("--end_date", help="YYYY-MM-DD end date", default=None)
    args = parser.parse_args()
    main(args.start_date, args.end_date)

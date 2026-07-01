from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import config
import sys
import os


def snapshot_layer(layer: str, out_dir: str, fmt: str = "parquet"):
    mapping = {
        "bronze": config.BRONZE_TABLE_PATH,
        "silver": config.SILVER_TABLE_PATH,
        "gold": config.GOLD_TABLE_PATH,
    }
    if layer not in mapping:
        raise ValueError("layer must be one of: bronze, silver, gold")

    delta_path = mapping[layer]

    spark = SparkSession.builder.appName("Snapshot-Delta-Layer").getOrCreate()
    df = spark.read.format("delta").load(delta_path)

    # add a snapshot timestamp column for traceability
    df = df.withColumn("snapshot_ts", current_timestamp())

    ts = int(__import__("time").time())
    out_path = os.path.join(out_dir, f"{layer}_snapshot_{ts}")

    if fmt == "parquet":
        df.write.mode("overwrite").parquet(out_path)
    elif fmt == "csv":
        df.coalesce(1).write.option("header", "true").mode("overwrite").csv(out_path)
    else:
        raise ValueError("Unsupported format: use 'parquet' or 'csv'")

    print(f"Wrote snapshot to {out_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Snapshot a Delta layer to Parquet or CSV")
    parser.add_argument("layer", help="Layer to snapshot: bronze|silver|gold")
    parser.add_argument("out_dir", help="Output directory to write snapshots to")
    parser.add_argument("--format", default="parquet", help="parquet or csv")
    args = parser.parse_args()
    snapshot_layer(args.layer, args.out_dir, args.format)

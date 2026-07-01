from pyspark.sql import SparkSession
import config
import sys


def main(layer: str, out_path: str):
    mapping = {
        "bronze": config.BRONZE_TABLE_PATH,
        "silver": config.SILVER_TABLE_PATH,
        "gold": config.GOLD_TABLE_PATH,
    }
    if layer not in mapping:
        print("Layer must be one of: bronze, silver, gold")
        sys.exit(1)

    delta_path = mapping[layer]

    spark = SparkSession.builder.appName("Export-Delta-To-CSV").getOrCreate()
    df = spark.read.format("delta").load(delta_path)

    # Coalesce to 1 file for easy inspection (may be large)
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(out_path)

    print(f"Exported {layer} data from {delta_path} to {out_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export Delta layer to CSV folder")
    parser.add_argument("layer", help="Layer to export: bronze|silver|gold")
    parser.add_argument("out_path", help="Output folder for CSV files")
    args = parser.parse_args()
    main(args.layer, args.out_path)

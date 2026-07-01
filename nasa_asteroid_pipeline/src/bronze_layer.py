from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import config


def main():
    # Initialize Spark with Delta & S3 Cloud Extension capabilities
    spark = SparkSession.builder \
        .appName("NASA-Bronze-Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("🌊 Starting Bronze Layer Stream Ingestion...")

    # --- SOURCE 1: Standard Spark Stream from Directory ---
    df_csv_stream = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(config.LOCAL_CSV_PATH) \
        .withColumn("source", lit("s3_csv")) \
        .withColumn("ingestion_timestamp", current_timestamp())

    # --- OR SOURCE 1b: Stream directly from a Kafka Topic ---
    # df_kafka_stream = spark.readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
    #     .option("subscribe", config.KAFKA_TOPIC) \
    #     .load()

    # Write Streaming Pipeline to Bronze Delta Table on S3
    bronze_query = df_csv_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", config.BRONZE_CHECKPOINT) \
        .option("mergeSchema", "true") \
        .trigger(processingTime="30 seconds") \
        .start(config.BRONZE_TABLE_PATH)

    bronze_query.awaitTermination()


if __name__ == "__main__":
    main()
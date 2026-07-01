from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import config


def main():
    spark = SparkSession.builder.appName("NASA-Silver-Processing").getOrCreate()

    print("⏰ Connecting to Bronze Delta Stream and applying Watermarks...")

    # Read stream from Bronze Delta Path
    df_bronze = spark.readStream.format("delta").load(config.BRONZE_TABLE_PATH)

    # Apply Watermark & Clean
    df_cleaned = df_bronze \
        .withWatermark("ingestion_timestamp", config.WATERMARK_DELAY) \
        .select(
            col("id").cast("string").alias("asteroid_id"),
            col("name"),
            col("absolute_magnitude_h").cast("double").alias("absolute_magnitude"),
            when(col("is_potentially_hazardous_asteroid") == True, 1).otherwise(0).alias("is_hazardous"),
            col("ingestion_timestamp"),
            col("source")
        ).dropna(subset=["asteroid_id", "name", "absolute_magnitude"])

    # Deduplicate stream based on key
    df_silver_stream = df_cleaned.dropDuplicates(["asteroid_id"])

    # Write to Silver Location
    silver_query = df_silver_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", config.SILVER_CHECKPOINT) \
        .trigger(processingTime="30 seconds") \
        .start(config.SILVER_TABLE_PATH)

    silver_query.awaitTermination()


if __name__ == "__main__":
    main()
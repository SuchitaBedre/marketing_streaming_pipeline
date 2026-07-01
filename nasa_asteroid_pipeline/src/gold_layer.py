from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import config


def main():
    spark = SparkSession.builder.appName("NASA-Gold-Features").getOrCreate()

    df_silver = spark.readStream.format("delta").load(config.SILVER_TABLE_PATH)

    # Feature engineering logic
    df_gold_stream = df_silver.withColumn(
        "magnitude_category",
        when(col("absolute_magnitude") < 18, "very_bright")
        .when(col("absolute_magnitude") < 22, "bright")
        .when(col("absolute_magnitude") < 25, "moderate")
        .otherwise("dim")
    ).withColumn(
        "risk_score",
        when(col("is_hazardous") == 1, 100 - col("absolute_magnitude") * 3).otherwise(0)
    )

    # Write out to Gold Target Storage
    gold_query = df_gold_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", config.GOLD_CHECKPOINT) \
        .trigger(processingTime="30 seconds") \
        .start(config.GOLD_TABLE_PATH)

    gold_query.awaitTermination()


if __name__ == "__main__":
    main()
import os

# AWS S3 Configuration (Point to LocalStack or real AWS S3)
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "s3a://nasa-asteroid-pipeline")

# Data Sources
LOCAL_CSV_PATH = f"{S3_BUCKET}/raw_csv_landing"
NASA_API_URL = "https://api.nasa.gov/neo/rest/v1/feed"
NASA_API_KEY = os.getenv("atRlamU8h97Mj5wYvtjsDkkNua16mjAJcFC8JUch", "")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "nasa-asteroids-raw"

# Delta Table Paths (Replacing Unity Catalog with S3-backed paths)
BRONZE_TABLE_PATH = f"{S3_BUCKET}/delta/asteroid_bronze"
SILVER_TABLE_PATH = f"{S3_BUCKET}/delta/asteroid_silver"
GOLD_TABLE_PATH = f"{S3_BUCKET}/delta/asteroid_gold"

# Processing Configuration
WATERMARK_DELAY = "2 hours"
BRONZE_CHECKPOINT = f"{S3_BUCKET}/checkpoints/bronze/"
SILVER_CHECKPOINT = f"{S3_BUCKET}/checkpoints/silver/"
GOLD_CHECKPOINT = f"{S3_BUCKET}/checkpoints/gold/"
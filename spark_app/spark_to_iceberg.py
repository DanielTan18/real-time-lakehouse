from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *


## spark streaming: kafka + iceberg
## consumes real-time transactions from kafka, parses them using Spark Structured Streaming
## then writes to an Iceberg table stored on MinIO (S3)

# Spark session with Iceberg pointing to MinIO
spark = SparkSession.builder \
    .appName("KafkaToIceberg") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("txn_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant", StringType()),
    StructField("txn_type", StringType()),
    StructField("timestamp", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write to Iceberg in MinIO
query = parsed_df.writeStream \
    .format("iceberg") \
    .option("checkpointLocation", "s3a://iceberg-warehouse/checkpoints/transactions") \
    .outputMode("append") \
    .toTable("local.default.transactions")

query.awaitTermination()
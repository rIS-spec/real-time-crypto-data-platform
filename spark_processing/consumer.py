import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# PostgreSQL connection details from environment variables
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'arish')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'Arish200502')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'data_platform')

# Kafka bootstrap servers
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

# Create Spark session
spark = SparkSession.builder \
    .appName("CryptoSparkConsumer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Define the schema for crypto data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("last_updated", StringType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("last_updated", to_timestamp(col("last_updated")))

# Write to console (for debugging) and to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .mode("append") \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("dbtable", "crypto_prices") \
            .save()

# Write to console for debugging
console_query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Write to PostgreSQL
postgres_query = df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

print("Spark Consumer started. Listening for Kafka messages...")

# Wait for termination
postgres_query.awaitTermination()
# The goal: Kafka is producing crypto prices every few seconds. Spark should read them live and print/save them continuously.
# spark_stream.py = working, tested, don't touch it. Ever.

# Step 1 — Create SparkSession with Kafka connector
# Start the Spark engine with Kafka support enabled.
# Step 2 — Define Schema
# Tell Spark the shape of Kafka messages — coin_id, price_usd, fetched_at etc.
# Step 3 — readStream from Kafka
# Connect Spark to your crypto-events topic and start listening continuously.
# Step 4 — Parse the JSON
# Kafka sends raw text. Convert it into proper columns using the schema.
# Step 5 — Transform
# Do simple processing — select columns, filter, add new columns if needed.
# Step 6 — writeStream to console
# Output results to terminal first (for testing). Later we change this to PostgreSQL.


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType



# step 1 — Create SparkSession - becoz Spark needs to be started with Kafka support enabled to read from Kafka topic live and write to console live or to PostgreSQL.
spark = SparkSession.builder \
    .appName("CryptoStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
    .config("spark.sql.streaming.checkpointLocation", "C:/tmp/spark-checkpoint") \
    .getOrCreate()



# step 2 — Define Schema
schema = StructType([      # Defines the full shape of one Kafka message — like CREATE TABLE in SQL
    StructField("coin_id", StringType(), True),   # coin_id column, data type is String, can be NULL (True)
    StructField("coin_name", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price_usd", FloatType(), True),
    StructField("fetched_at", StringType(), True)
])



#  step 3 — readStream
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-events") \
    .option("startingOffsets", "latest") \
    .load()



# step 4 — Parse the JSON
parsed_stream = raw_stream.select(
    F.from_json(
        F.col("value").cast("string"), schema
    ).alias("data")
).select("data.*")



# step 5 — Transform
transformed_stream = parsed_stream.select(
    F.col("coin_id"),
    F.col("coin_name"),
    F.col("symbol"),
    F.col("price_usd"),
    F.col("fetched_at"),
    F.current_timestamp().alias("spark_processed_at")
)



# step 6 — writeStream to Console
query = transformed_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("checkpointLocation", "file:///C:/tmp/spark-checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()     # Keep running forever until I press Ctrl+C — without this, Spark starts and immediately exits

query.awaitTermination()




# transformations.py — batch processing every hour via Airflow
# spark_stream_pg.py — live Kafka → PostgreSQL
# spark_stream.py — testing only (but still part of project)

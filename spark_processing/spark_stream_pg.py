# Kafka → Spark → PostgreSQL (SAVED PERMANENTLY)
# spark_stream_pg.py = new version that saves to PostgreSQL.

# spark_stream_pg.py — Phase 4 Part 2 Same as spark_stream.py but instead of printing to console, saves each micro-batch permanently to PostgreSQL streaming_results table using foreachBatch().
# Example use: "Every 10 seconds, 5 new coin prices from Kafka get saved to database automatically"

# STEP 1 — Create SparkSession
#          (start the Spark engine, load JDBC + Kafka drivers)

# STEP 2 — Define Schema
#          (tell Spark what shape the JSON from Kafka has)

# STEP 3 — Read from Kafka (readStream)
#          (same as yesterday — tap into live Kafka topic)

# STEP 4 — Parse JSON
#          (convert raw Kafka bytes → proper columns)

# STEP 5 — Define foreachBatch function
#          (THIS IS NEW — write a function that saves
#           each micro-batch to PostgreSQL)

# STEP 6 — writeStream with foreachBatch
#          (instead of .format("console"),
#           use .foreachBatch(our_function))

# STEP 7 — awaitTermination
#          (keep running forever, listening to Kafka)


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from api_service.config import get_settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# step 1 — Create SparkSession
spark = SparkSession.builder \
    .appName("CryptoStreamToPostgres") \
    .config("spark.jars", "spark_processing/postgresql-42.7.3.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
    .getOrCreate()   # Create new SparkSession or reuse existing one, if it exists.

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark Session created — CryptoStreamToPostgres")



settings = get_settings()


# step 2 — Define Schema
schema = StructType([     # Define the shape of JSON coming from Kafka — like CREATE TABLE in SQL.
    StructField("coin_id", StringType(), True),
    StructField("coin_name", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price_usd", FloatType(), True),
    StructField("fetched_at", TimestampType(), True)
])



# step 3 — Read from Kafka (readStream) 
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-events") \
    .option("startingOffsets", "latest") \
    .load()




# step 4 — Parse JSON   WHY: Kafka sends raw bytes → Spark needs shape to parse
parsed_stream = raw_stream.select(
    F.from_json(
        F.col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")




# step 5 — Define foreachBatch function -> this is the main function of this script
def write_to_postgres(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id} — rows: {batch_df.count()}")

    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id} is empty — skipping")
        return

    batch_df = batch_df.withColumn(
        "spark_processed_at",
        F.current_timestamp()
    )

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/data_platform") \
        .option("dbtable", "streaming_results") \
        .option("user", settings.POSTGRES_USER) \
        .option("password", settings.POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    logger.info(f"Batch {batch_id} saved to PostgreSQL successfully")




# step 6 — attach the function to writeStream with foreachBatch
query = parsed_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "file:///C:/tmp/spark-checkpoint-pg") \
    .trigger(processingTime="10 seconds") \
    .start()


# step 7 — awaitTermination
query.awaitTermination()





# What it does:
# 1. Creates SparkSession with Kafka + JDBC
# 2. Uses readStream to read from Kafka continuously
# 3. Parses JSON → DataFrame
# 4. Uses foreachBatch() to process each micro-batch
# 5. Each batch written to streaming_results table in PostgreSQL
# 6. Runs forever until manually stopped

# Flow:
# Kafka → Spark Streaming → foreachBatch() → PostgreSQL
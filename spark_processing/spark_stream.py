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



# step 1 — Create SparkSession
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


# ═══════════════════════════════════════════════════════════════
# REAL-TIME CRYPTO DATA PLATFORM — SPARK PROCESSING OVERVIEW
# ═══════════════════════════════════════════════════════════════
#
# 3 PRODUCTION FILES:
# 1. transformations.py  → batch processing  → every hour via Airflow
# 2. spark_stream_pg.py  → live streaming    → always running
# 3. spark_stream.py     → testing only      → manual use
#
# ═══════════════════════════════════════════════════════════════
# FULL PLATFORM FLOW (all files together)
# ═══════════════════════════════════════════════════════════════
#
# CoinGecko API
#    ↓ every 60 seconds
# producer.py → sends JSON to Kafka
#    ↓
# Kafka broker (crypto-events, 3 partitions)
#    ↓                        ↓
# consumer.py          spark_stream_pg.py
#    ↓                        ↓
# crypto_events          streaming_results
# (PostgreSQL)           (PostgreSQL)
#    ↓
# transformations.py (every hour)
#    ↓
# crypto_aggregations (PostgreSQL)
#    ↓
# ML + Dashboard (Month 3)
#
# ═══════════════════════════════════════════════════════════════
# FILE 1 — transformations.py (BATCH — every hour via Airflow)
# ═══════════════════════════════════════════════════════════════
#
# python spark_processing/transformations.py
#    ↓
# SparkSession created
#    master = local[*] → all CPU cores
#    postgresql jar loaded → needed for JDBC read/write
#    ↓
# read_crypto_from_postgres()
#    spark.read.jdbc() → reads ALL crypto_events rows
#    40 rows loaded into DataFrame
#    ↓
# filter_high_value_coins()
#    TRANSFORMATION: df.filter(price_usd > 1.0)
#    ACTION: .count() → triggers execution
#    Dogecoin removed → 32 rows remain
#    ↓
# average_price_per_coin()
#    TRANSFORMATION: groupBy("coin_id").agg(avg, max, min, count)
#    ACTION: .show() → triggers execution
#    40 rows → 5 rows (one per coin)
#    ↓
# add_price_category()
#    TRANSFORMATION: withColumn + F.when()
#       price > 1000 → HIGH (Bitcoin, Ethereum)
#       price > 1    → MID  (Solana, Ripple)
#       otherwise    → LOW  (Dogecoin)
#    ACTION: .show() → triggers execution
#    ↓
# write_aggregations_to_postgres()
#    ACTION: avg_df.write.jdbc()
#    mode = overwrite → always latest aggregation only
#    5 rows saved to crypto_aggregations table
#    ↓
# window_analysis()
#    TRANSFORMATION: rank() → price rank within each coin
#    TRANSFORMATION: lag()  → previous row price
#                             first row = NULL (no previous)
#    TRANSFORMATION: row_number() → unique number per row
#    TRANSFORMATION: price_change = current - prev_price
#    ACTION: .show() → all 4 window ops execute together
#    ↓
# spark.stop() → RAM freed, JVM terminated
#    ↓
# DONE — results in crypto_aggregations table
#
# ═══════════════════════════════════════════════════════════════
# FILE 2 — spark_stream_pg.py (LIVE STREAMING — always running)
# ═══════════════════════════════════════════════════════════════
#
# python spark_processing/spark_stream_pg.py
#    ↓
# SparkSession created
#    postgresql jar → needed for writing to PostgreSQL
#    kafka jar      → needed for readStream from Kafka
#    ↓
# Schema defined (StructType)
#    coin_id, coin_name, symbol → StringType
#    price_usd                  → FloatType
#    fetched_at                 → TimestampType
#    WHY: Kafka sends raw bytes → Spark needs shape to parse
#    ↓
# readStream from Kafka
#    format("kafka")
#    subscribe = "crypto-events"
#    startingOffsets = "latest" → only new messages
#    raw_stream → has key, value, topic, partition columns
#    value column = raw JSON bytes from producer
#    ↓
# Parse JSON
#    value.cast("string") → bytes to readable string
#    F.from_json(..., schema) → string to structured columns
#    .select("data.*") → flatten nested columns
#    parsed_stream → coin_id | coin_name | price_usd | fetched_at
#    ↓
# write_to_postgres() function defined
#    called every 10 seconds with a new mini DataFrame
#    if batch empty → skip
#    adds spark_processed_at timestamp column
#    writes to streaming_results table (mode=append)
#    WHY append: keep ALL historical streaming data
#    ↓
# writeStream with foreachBatch
#    .foreachBatch(write_to_postgres) → call function every batch
#    .checkpointLocation → saves Kafka offset to disk
#       if crash → restart from last offset, no duplicates
#    .trigger(10 seconds) → new batch every 10 seconds
#    .start() → streaming begins
#    ↓
# awaitTermination()
#    infinite loop — every 10 seconds:
#       collect new Kafka messages
#       → call write_to_postgres()
#       → rows saved to streaming_results
#       → wait 10 seconds → repeat
#    ↓
# Until Ctrl+C → streaming stops
#
# ═══════════════════════════════════════════════════════════════
# FILE 3 — spark_stream.py (TESTING ONLY — verify Kafka works)
# ═══════════════════════════════════════════════════════════════
#
# python spark_processing/spark_stream.py
#    ↓
# SparkSession created
#    kafka jar only → NO postgresql jar needed because no write to PG   
#    ↓
# Schema defined
#    same 5 fields BUT fetched_at → StringType
#    WHY: console does not need exact timestamp type
#         spark_stream_pg.py uses TimestampType for PostgreSQL storage
#    ↓
# readStream from Kafka → IDENTICAL to spark_stream_pg.py
#    ↓
# Parse JSON → IDENTICAL to spark_stream_pg.py
#    ↓
# Transform (EXTRA step — not in spark_stream_pg.py)
#    select all columns + add spark_processed_at timestamp
#    ↓
# writeStream to Console (NOT PostgreSQL)
#    .format("console") → prints to terminal
#    .outputMode("append") → show only new rows
#    .option("truncate", False) → show full values
#    .trigger(10 seconds) → new batch every 10 seconds
#    Every 10 seconds in terminal:
#    | bitcoin | Bitcoin | BTC | 66737.00 | 2026-03-29 |
#    | ethereum| Ethereum| ETH |  2004.67 | 2026-03-29 |
#    ↓
# awaitTermination() → runs until Ctrl+C
#
# ═══════════════════════════════════════════════════════════════
# KEY DIFFERENCES — 3 FILES SIDE BY SIDE
# ═══════════════════════════════════════════════════════════════
#
# Feature              transformations.py   spark_stream_pg.py   spark_stream.py
# ─────────────────    ──────────────────   ──────────────────   ───────────────
# Type                 Batch                Stream               Stream (test)
# Input                PostgreSQL (JDBC)    Kafka (readStream)   Kafka (readStream)
# Output               PostgreSQL (JDBC)    PostgreSQL (batch)   Console
# Runs                 Once/hour (Airflow)  Forever              Manual only
# PostgreSQL jar       Yes (read+write)     Yes (write only)     No
# Kafka jar            No                   Yes                  Yes
# Schema defined       No (inferred JDBC)   Yes (StructType)     Yes (StructType)
# foreachBatch         No                   Yes                  No
# Checkpoint           No                   Yes                  Yes
# Write mode           overwrite            append               N/A
# Window Functions     Yes (rank,lag,rn)    No                   No
# Stops automatically  Yes (spark.stop)     No (Ctrl+C)          No (Ctrl+C)
#
# ═══════════════════════════════════════════════════════════════
# HOW TO RUN
# ═══════════════════════════════════════════════════════════════
#
# Terminal 1: python kafka_service/producer.py
# Terminal 2: python spark_processing/spark_stream.py     ← test first
# Terminal 3: python spark_processing/spark_stream_pg.py  ← then production
# Terminal 4: python spark_processing/transformations.py  ← or let Airflow do it
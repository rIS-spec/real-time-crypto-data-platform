# PURPOSE: PySpark batch transformations on crypto price data
# also Phase 2 + 3 work
# Reads crypto_events from PostgreSQL, applies filter, groupBy, aggregations, window functions (rank, lag, row_number). Saves results to crypto_aggregations table.
# Example use: "Find average price per coin, rank coins by price, calculate price change from previous row"

# BEFORE RUNNING — set these in PowerShell

# $env:JAVA_TOOL_OPTIONS = "-Duser.timezone=UTC"
# $env:HADOOP_HOME = "C:\hadoop"
# $env:PATH = "$env:PATH;C:\hadoop\bin"
# $env:PYTHONPATH = "C:\Users\arish\Desktop\real-time-data-platform"

# Then run: python spark_processing/transformations.py


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from api_service.config import get_settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()



# Execution Order:
#     Start Spark
#     Load data
#     Filter
#     Aggregate
#     Categorize
#     Save
#     Stop Spark


# Start Spark
# Analogy: turning the car key before driving
# Without this — nothing works
def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("CryptoTransformations") \
        .master("local[*]") \
        .config("spark.jars", "spark_processing/postgresql-42.7.3.jar") \
        .getOrCreate()
    logger.info("SparkSession created successfully")
    return spark



# Load data
# Read from PostgreSQL
# SQL equivalent: SELECT * FROM crypto_events
# Analogy: getting all ingredients from the fridge
def read_crypto_from_postgres(spark: SparkSession):
    # JDBC address of PostgreSQL database
    url = "jdbc:postgresql://localhost:5432/data_platform"

    # Credentials — same as config.py and docker-compose.yml
    properties = {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "options": "-c TimeZone=UTC"
    }

    # Read entire crypto_events table into Spark DataFrame
    df = spark.read.jdbc(
        url=url,
        table="crypto_events",
        properties=properties
    )

    logger.info(f"Loaded {df.count()} rows from crypto_events")
    df.show(5)
    return df



# Transformations - Filter
#  Filter High Value Coins
# SQL equivalent: SELECT * FROM crypto_events WHERE price_usd > 1.0
# Removes Dogecoin ($0.08) — keeps Bitcoin, Ethereum, Solana, Ripple
# filter() is a TRANSFORMATION — lazy, does not run yet
# .count() is an ACTION — triggers Spark to execute
def filter_high_value_coins(df):
    filtered = df.filter(df.price_usd > 1.0)
    logger.info(f"High value coins: {filtered.count()}")
    filtered.show()
    return filtered



# Aggregate
# SQL equivalent: SELECT coin_id, AVG(price_usd), MAX(price_usd), MIN(price_usd), COUNT(price_usd) FROM crypto_events GROUP BY coin_id
def average_price_per_coin(df):
    avg_df = df.groupBy("coin_id").agg(
        F.avg("price_usd").alias("avg_price"),
        F.max("price_usd").alias("max_price"),
        F.min("price_usd").alias("min_price"),
        F.count("price_usd").alias("total_records")
    )
    logger.info("Average price per coin calculated")
    avg_df.orderBy("avg_price", ascending=False).show()
    return avg_df


# Categorize
def add_price_category(df):
    categorized = df.withColumn(
        "price_category",
        F.when(df.price_usd > 1000, "HIGH")
         .when(df.price_usd > 1, "MID")
         .otherwise("LOW")
    )
    logger.info("Price category column added")
    categorized.select(
        "coin_id", "price_usd", "price_category"
    ).show()
    return categorized


# Save
# Write Results to PostgreSQL
# Saves aggregated results permanently to crypto_aggregations table
# mode=overwrite — replaces old aggregation with fresh one every run because it's LATEST aggregation
# Why overwrite not append? Dashboard only needs LATEST aggregation
# Analogy: updating a scoreboard — replace old scores, not stack them
def write_aggregations_to_postgres(avg_df):
    url = "jdbc:postgresql://localhost:5432/data_platform"
    properties = {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    avg_df.write.jdbc(
        url=url,
        table="crypto_aggregations",
        mode="overwrite",
        properties=properties
    )
    logger.info("Aggregations written to crypto_aggregations table")



def window_analysis(df):
    
    window_by_price = Window.partitionBy("coin_id") \
                            .orderBy(F.desc("price_usd"))
    
    df = df.withColumn("price_rank", F.rank().over(window_by_price))
    
    window_by_time = Window.partitionBy("coin_id") \
                           .orderBy("fetched_at")
    
    df = df.withColumn("prev_price", F.lag("price_usd", 1).over(window_by_time))
    
    df = df.withColumn("row_num", F.row_number().over(window_by_time))

    df = df.withColumn("price_change",
                       F.round(df["price_usd"] - df["prev_price"], 8))
    
    return df




# MAIN: Run all transformations in order 
if __name__ == "__main__":

    # Step 1 — Start Spark engine
    spark = create_spark_session()

    # Step 2 — Pull all rows from PostgreSQL into DataFrame
    df = read_crypto_from_postgres(spark)

    # Step 3 — Filter: keep only coins with price > $1
    filter_high_value_coins(df)

    # Step 4 — Aggregate: avg, max, min, count per coin
    avg_df = average_price_per_coin(df)

    # Step 5 — Categorize: label each coin HIGH / MID / LOW
    add_price_category(df)

    # Step 6 — Write aggregation results back to PostgreSQL
    write_aggregations_to_postgres(avg_df)

    print("\n--- Window Analysis ---")
    window_df = window_analysis(df)
    window_df.select("coin_id", "price_usd", "prev_price", 
                     "price_change", "row_num").show(10)

    # Step 7 — Shut down Spark cleanly
    spark.stop()




# CoinGecko → producer.py → Kafka topic (crypto-events)
#                               │
#                 ┌─────────────┴─────────────┐
#                 ↓                           ↓
#           consumer.py              spark_stream_pg.py
#                 ↓                           ↓
#         crypto_events table        streaming_results table
#                 ↓
#         transformations.py (reads crypto_events)
#                 ↓
#         crypto_aggregations table







# CoinGecko API sends live crypto prices
#    ↓
# FastAPI (api_service/main.py) receives request
#    ↓
# fetch_crypto() called → prices fetched from CoinGecko
#    ↓
# Kafka Producer (kafka_service/producer.py) runs
#    ↓
# Each coin price sent as message to crypto-events topic
#    ↓
# Kafka Topic (crypto-events) stores messages in order
#    ↓
# Kafka Consumer (kafka_service/consumer.py) reads messages
#    ↓
# Each message saved as a row in PostgreSQL → crypto_events table
#    ↓
# THIS FILE runs — spark_processing/transformations.py
#    ↓
# PySpark reads crypto_events → runs transformations
#    ↓
# Results saved to PostgreSQL → crypto_aggregations table
#    ↓
# Streamlit Dashboard (Month 3) reads crypto_aggregations
#    ↓
# Live charts, price categories, anomaly alerts shown
#
 
# ─── THIS FILE WORKFLOW (transformations.py) 
#
# python transformations.py
#    ↓
# create_spark_session() runs
#    ↓
# Spark starts on local[*] — use all CPU cores
#    ↓
# read_crypto_from_postgres() runs
#    ↓
# Spark connects to PostgreSQL via JDBC
#    ↓
# 40 rows loaded into DataFrame
#    ↓
# filter_high_value_coins() runs
#    ↓
# Removes coins where price_usd < 1.0 (Dogecoin filtered out)
#    ↓
# average_price_per_coin() runs
#    ↓
# Groups by coin_id → AVG, MAX, MIN, COUNT calculated
#    ↓
# add_price_category() runs
#    ↓
# New column added → HIGH / MID / LOW label per coin
#    ↓
# write_aggregations_to_postgres() runs
#    ↓
# avg_df saved to crypto_aggregations table (mode=overwrite)
#    ↓
# spark.stop() — Spark shuts down cleanly
#    ↓
# Done!

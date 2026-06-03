from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


#  STEP 1 — Create SparkSession
spark = SparkSession.builder \
    .appName("CryptoRevision26") \
    .config("spark.jars", "file:///D:/Desktop/real-time-data-platform/spark_processing/postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "D:/Desktop/real-time-data-platform/spark_processing/postgresql-42.7.3.jar") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


#  STEP 2 — Read crypto_events from PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/data_platform") \
    .option("dbtable", "crypto_events") \
    .option("user", "arish") \
    .option("password", "Arish200502") \
    .load()



#  STEP 3 — Write to CSV
print("\n--- Writing CSV ---")
df.select("coin_id", "coin_name", "price_usd", "fetched_at") \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv("spark_output/crypto_csv")

print("CSV written successfully")


#  STEP 4 — Write to Parquet
print("\n--- Writing Parquet ---")
df.select("coin_id", "coin_name", "price_usd", "fetched_at") \
  .write \
  .mode("overwrite") \
  .parquet("spark_output/crypto_parquet")

print("Parquet written successfully")


#  STEP 5 — Write to JSON
print("\n--- Writing JSON ---")
df.select("coin_id", "coin_name", "price_usd", "fetched_at") \
  .write \
  .mode("overwrite") \
  .json("spark_output/crypto_json")

print("JSON written successfully")



#  STEP 6 — Read CSV, Parquet, and JSON
print("\n--- Reading CSV ---")
csv_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("spark_output/crypto_csv")
csv_df.show(3)

print("\n--- Reading Parquet ---")
parquet_df = spark.read.parquet("spark_output/crypto_parquet")
parquet_df.show(3)

print("\n--- Reading JSON ---")
json_df = spark.read.json("spark_output/crypto_json")
json_df.show(3)






#  STEP 7 — Null Handling in Spark DataFrames with PySpark DataFrames API 
print("\n--- Null Handling ---")
print("Total rows:", df.count())

print("\nRows with NULL price_change_24h:", 
      df.filter(F.col("price_change_24h").isNull()).count())

print("\nAfter dropna() on price_change_24h:")
df.dropna(subset=["price_change_24h"]).count()
print(df.dropna(subset=["price_change_24h"]).count(), "rows remaining")


#  STEP 8 — Null Handling in Spark DataFrames with PySpark DataFrames API
print("\n--- fillna() --- replace NULL with default value ---")
df_filled = df.fillna({
    "price_change_24h": 0.0,
    "market_cap": 0.0,
    "volume_24h": 0.0
})

print("Rows with NULL price_change_24h after fillna():",
      df_filled.filter(F.col("price_change_24h").isNull()).count())

df_filled.select("coin_id", "price_usd", "price_change_24h") \
         .show(5)




#  STEP 9 — Debugging Tools in PySpark 
print("\n--- Debugging Tools ---")
print("\nSchema:")
df.printSchema()

print("\nPartition count:", df.rdd.getNumPartitions())

print("\nSample data:")
df.select("coin_id", "price_usd", "price_change_24h") \
  .show(3)

print("\nNull counts per column:")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) 
           for c in ["coin_id", "price_usd", "price_change_24h", "market_cap"]]) \
  .show()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
import logging
import random

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

# STEP 3 — Check Partitions
print("Default parallelism:", spark.sparkContext.defaultParallelism)
print("Current partitions:", df.rdd.getNumPartitions())

# STEP 4 — Repartition
print("\n--- repartition() --- increase partitions ---")
df_repartitioned = df.repartition(4)
print("After repartition(4):", df_repartitioned.rdd.getNumPartitions())

# STEP 5 — Coalesce
print("\n--- coalesce() --- decrease partitions ---")
df_coalesced = df_repartitioned.coalesce(2)
print("After coalesce(2):", df_coalesced.rdd.getNumPartitions())



# STEP 6 — Caching in PySpark
from pyspark import StorageLevel

print("\n--- Caching --- store in RAM for reuse ---")
df.cache()
print("DataFrame cached in RAM")
print("Row count:", df.count())  
print("Row count again:", df.count())  
df.unpersist()      # free RAM immediately when done
print("Cache cleared")


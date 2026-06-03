# performance_demo.py — Phase 5
# Demonstrates all 4 performance techniques on real crypto data: repartition(4), cache(), groupBy shuffle, broadcast join.
# Example use: "Shows broadcast join takes 0.52s vs regular join which would shuffle all data"


# STEP 1 — Create SparkSession
#          (same as always)
#          ↓
# STEP 2 — Read crypto_events from PostgreSQL
#          (load real data we already have)
#          ↓
# STEP 3 — Check Partitions
#          (how many partitions does Spark create by default?)
#          ↓
# STEP 4 — Repartition
#          (manually control how data is divided)
#          ↓
# STEP 5 — Caching
#          (save DataFrame in memory to avoid re-reading)
#          ↓
# STEP 6 — Shuffle demonstration
#          (see how groupBy causes shuffle)
#          ↓
# STEP 7 — Broadcast Join
#          (smart way to join large + small tables)
#          ↓
# STEP 8 — Compare performance
#          (with cache vs without cache)



from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# STEP 1 — Create SparkSession
spark = SparkSession.builder \
    .appName("CryptoPerformanceDemo") \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .config("spark.jars", "spark_processing/postgresql-42.7.3.jar") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark Session created — Performance Demo")



# STEP 2 — Read crypto_events from PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/data_platform") \
    .option("dbtable", "crypto_events") \
    .option("user", "arish") \
    .option("password", "Arish200502") \
    .option("driver", "org.postgresql.Driver") \
    .load()

logger.info(f"Loaded {df.count()} rows from crypto_events")



# STEP 3 — Check Partitions
num_partitions = df.rdd.getNumPartitions()    # count the chunks → returns 1 by default
logger.info(f"Default partitions: {num_partitions}")



# STEP 4 — Repartition
df_repartitioned = df.repartition(4)
logger.info(f"After repartition: {df_repartitioned.rdd.getNumPartitions()}")   # count the chunks → returns 4 now




# STEP 5 — Caching

# Step 1 — Run WITHOUT cache and measure time(Run WITHOUT cache → measure time → SLOW)
logger.info("Without cache:")
start = time.time()
count1 = df_repartitioned.count()
count2 = df_repartitioned.filter(F.col("price_usd") > 100).count()
end = time.time()
logger.info(f"Without cache time: {round(end - start, 2)} seconds")

# Step 2 — applying cache
df_repartitioned.cache()
logger.info("Cache applied — data now in memory")

# Step 3 — Run WITH cache and measure time
logger.info("With cache:")
start = time.time()
count3 = df_repartitioned.count()
count4 = df_repartitioned.filter(F.col("price_usd") > 100).count()
end = time.time()
logger.info(f"With cache time: {round(end - start, 2)} seconds")

# Step 4 — Compare and unpersist.
logger.info("Removing cache from memory")
df_repartitioned.unpersist()




# STEP 6 — Shuffle demonstration
logger.info("Shuffle demonstration — groupBy causes shuffle")
start = time.time()
shuffle_df = df_repartitioned.groupBy("coin_id") \
    .agg(
        F.avg("price_usd").alias("avg_price"),
        F.count("*").alias("total_records")
    )
shuffle_df.show()
end = time.time()
logger.info(f"GroupBy with shuffle time: {round(end - start, 2)} seconds")



# STEP 7 — Broadcast Join
coin_info = spark.sql("""
    SELECT * FROM VALUES
    ('bitcoin','Layer 1'),
    ('ethereum','Layer 1'),
    ('solana','Layer 1'),
    ('dogecoin','Meme'),
    ('cardano','Layer 1')
    AS coin_info(coin_id, category)
""")

logger.info(f"Small table rows: {coin_info.count()}")

logger.info("Broadcast Join demonstration")
start = time.time()
joined_df = df_repartitioned.join(
    F.broadcast(coin_info),
    on="coin_id",
    how="left"
)
joined_df.select("coin_id", "price_usd", "category").show(5)
end = time.time()
logger.info(f"Broadcast join time: {round(end - start, 2)} seconds")


# STEP 8 — Compare performance
spark.stop()
logger.info("Spark Session stopped")












# What it does:
# 1. Demonstrates all Phase 5 optimizations
# 2. Shows partition count before/after repartition
# 3. Demonstrates cache() speedup
# 4. Shows shuffle happening in groupBy
# 5. Demonstrates broadcast join vs normal join
# 6. All on real crypto_events data
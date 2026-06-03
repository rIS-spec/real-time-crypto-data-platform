from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



#  STEP 1 — Create SparkSession
spark = SparkSession.builder \
    .appName("CryptoRevision24") \
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

print("Total rows:", df.count())
print("Columns:", df.columns)



print("\n--- rank() --- price rank per coin ---")
window_spec = Window.partitionBy("coin_id")\
                    .orderBy(F.desc("price_usd"))


df.withColumn("price_rank", F.rank().over(window_spec))\
  .select("coin_id", "price_usd", "price_rank")\
  .orderBy("coin_id", "price_rank")\
  .show(20)




print("\n--- dense_rank() --- no gaps in ranking ---")
df.withColumn("dense_rank", F.dense_rank().over(window_spec))\
  .select("coin_id", "price_usd", "dense_rank")\
  .orderBy("coin_id", "dense_rank")\
  .show(10)



print("\n--- row_number() --- unique number every row ---")
window_rn = Window.partitionBy("coin_id") \
                  .orderBy(F.desc("fetched_at"))

df.withColumn("rn", F.row_number().over(window_rn))\
  .filter(F.col("rn") == 1)\
  .select("coin_id", "price_usd", "fetched_at")\
  .show()



print("\n----- lag() --- previous row value -----")
window_lag = Window.partitionBy("coin_id") \
                   .orderBy("fetched_at")

df.withColumn("prev_price", F.lag("price_usd", 1).over(window_lag))\
  .withColumn("price_change", F.round(F.col("price_usd") - F.col("prev_price"), 2)) \
  .select("coin_id", "price_usd", "prev_price", "price_change")\
  .orderBy("coin_id", "fetched_at")\
  .show(10)

print("\n----- lead() --- next row value -----")
df.withColumn("next_price", F.lead("price_usd", 1).over(window_lag))\
  .select("coin_id", "price_usd", "next_price")\
  .orderBy("coin_id", "fetched_at")\
  .show(10)


print("\n-----for each coin - calculate the price change percentage between current and previous price -----")
window_percentage = Window.partitionBy("coin_id") \
                           .orderBy("fetched_at") 

df.withColumn("prev_price", F.lag("price_usd", 1).over(window_percentage))\
  .withColumn("price_change_percentage", F.round((F.col("price_usd") - F.col("prev_price")) / F.col("prev_price") * 100, 2))\
  .select("coin_id", "price_usd", "prev_price", "price_change_percentage")\
  .orderBy("coin_id", "fetched_at")\
  .show(10)

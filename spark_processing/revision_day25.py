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
    .appName("CryptoRevision25") \
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


coin_categories = spark.sql("""
    SELECT * FROM VALUES
        ('bitcoin',  'Store of Value', 'High'),
        ('ethereum', 'Smart Contract', 'High'),
        ('solana',   'Fast Payments',  'Very High'),
        ('dogecoin', 'Meme Coin',      'Extreme')
    AS t(coin_id, category, risk)
""")

print("\n--- Coin Categories Lookup Table ---")
coin_categories.show()



print("\n--- Inner Join --- only matching rows ---")
df.join(coin_categories, on="coin_id", how="inner") \
  .select("coin_id", "price_usd", "category", "risk") \
  .orderBy("coin_id") \
  .show(5)



print("\n--- Left Join --- all left rows + matched right ---")
df.join(coin_categories, on="coin_id", how="left") \
  .select("coin_id", "price_usd", "category", "risk") \
  .orderBy(F.desc("coin_id")) \
  .show(10)



print("\n--- Right Join --- all right rows + matched left ---")
df.join(coin_categories, on="coin_id", how="right") \
  .select("coin_id", "price_usd", "category", "risk") \
  .orderBy("coin_id") \
  .show(5)



print("\n--- Full Outer Join --- all rows from both tables ---")
df.join(coin_categories, on="coin_id", how="full") \
  .select("coin_id", "price_usd", "category", "risk") \
  .orderBy(F.desc("coin_id")) \
  .show(10)




print("\n--- Broadcast Join --- small table copied to every core ---")
df.join(broadcast(coin_categories), on="coin_id", how="inner") \
  .select("coin_id", "price_usd", "category", "risk") \
  .orderBy("coin_id") \
  .show(5)




print("\n---- Skew Handling ---- salting technique ----")

salted_df = df.withColumn("salt", (F.rand() * 4).cast("int")) \
              .withColumn("salted_coin_id", F.concat(F.col("coin_id"), F.lit("_"), F.col("salt")))

print("Salted coin_id examples:")
salted_df.select("coin_id", "salted_coin_id").show(8)
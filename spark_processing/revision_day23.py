from pyspark.sql import SparkSession
from pyspark.sql import functions as F


#  STEP 1 — Create SparkSession (same as always) 
spark = SparkSession.builder \
    .appName("CryptoRevision23") \
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


print("\n--- select() --- pick only columns you need ---")
df.select("coin_id", "coin_name", "price_usd").show(5)


print("\n------ MY OWN QUERY --- Solana filter + select + orderBy -----")
df.filter(F.col("coin_id") == "solana") \
  .select("coin_id", "price_usd") \
  .orderBy(F.desc("price_usd")) \
  .show(5)



print("\n--- filter() --- keep only rows you need ---")
df.filter(F.col("coin_id") == "bitcoin") \
  .select("coin_id", "price_usd", "fetched_at") \
  .show(5)




print("\n--- groupBy() + agg() --- one result per group ---")
df.groupBy("coin_id") \
  .agg(F.round(F.avg("price_usd"), 2).alias("avg_price"),
       F.round(F.max("price_usd"), 2).alias("max_price"),
       F.round(F.min("price_usd"), 2).alias("min_price")) \
  .show()





print("\n--- withColumn() --- add a new column ---")
df.select("coin_id", "price_usd") \
  .withColumn("price_category",
              F.when(F.col("price_usd") > 10000, "HIGH")
               .when(F.col("price_usd") > 100, "MEDIUM")
               .otherwise("LOW")) \
  .show(10)



print("\n--- orderBy() --- sort your data ---")
df.select("coin_id", "price_usd") \
  .orderBy(F.desc("price_usd")) \
  .show(5)


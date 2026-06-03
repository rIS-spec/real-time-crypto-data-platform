


# Step 1 — Create SparkSession (same as always, with your 2 critical configs)
# Step 2 — Read crypto_events from PostgreSQL via JDBC (same as transformations.py — real data)
# Step 3 — Register as a Temp View — give the DataFrame a SQL name: createOrReplaceTempView("crypto_events")
# Step 4 — Run 5 SQL queries using spark.sql(), each mapping to something you already built in previous phases:



from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



# step 1 — Create SparkSession - WHY: Every Spark program needs a SparkSession first. It is the entry point — without it, nothing works. Think of it as - starting the engine before driving.
spark = SparkSession.builder \
    .appName("CryptoSparkSQL") \
    .config("spark.jars", "file:///D:/Desktop/real-time-data-platform/spark_processing/postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "D:/Desktop/real-time-data-platform/spark_processing/postgresql-42.7.3.jar") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")



# step 2 — Read crypto_events from PostgreSQL via JDBC - WHY: We need real crypto data to run SQL on. We read from crypto_events table using JDBC — same as transformations.py. This gives us a Spark DataFrame with all your Bitcoin, Ethereum, Solana, Dogecoin, Cardano rows.
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/data_platform") \
    .option("dbtable", "crypto_events") \
    .option("user", "arish") \
    .option("password", "Arish200502") \
    .load()




# step 3 — Register as a Temp View - WHY: Right now df is a Spark DataFrame. Spark SQL cannot run SQL on a DataFrame directly — it needs a named table to query against. createOrReplaceTempView() gives your DataFrame a temporary name that SQL can reference.
df.createOrReplaceTempView("crypto_events")


# step 4 — Run 5 SQL queries using spark.sql(), each mapping to something you already built in previous phases:
print("\n--- Query 1: Average Price Per Coin ---")
spark.sql("""
    SELECT coin_id, 
           ROUND(AVG(price_usd), 2) as avg_price
    FROM crypto_events
    GROUP BY coin_id
""").show()

print("\n--- Query 2: Price Rank Per Coin ---")
spark.sql("""
    SELECT coin_id,
           price_usd,
           RANK() OVER (PARTITION BY coin_id ORDER BY price_usd DESC) as price_rank
    FROM crypto_events
""").show()

print("\n--- Query 3: Previous Price Per Coin ---")
spark.sql("""
    SELECT coin_id,
           price_usd,
           fetched_at,
           LAG(price_usd, 1) OVER (PARTITION BY coin_id ORDER BY fetched_at) as prev_price
    FROM crypto_events
""").show()

print("\n--- Query 4: Price Change Per Coin ---")
spark.sql("""
    SELECT coin_id,
           price_usd,
           fetched_at,
           ROUND(price_usd - LAG(price_usd, 1) OVER (PARTITION BY coin_id ORDER BY fetched_at), 2) as price_change
    FROM crypto_events
""").show()

print("\n--- Query 5: Combined Filter + Group + Rank ---")
spark.sql("""
    SELECT coin_id,
           ROUND(AVG(price_usd), 2) as avg_price,
           RANK() OVER (ORDER BY AVG(price_usd) DESC) as overall_rank
    FROM crypto_events
    WHERE price_usd > 0
    GROUP BY coin_id
""").show()

spark.stop()











# What it does:
# 1. Reads crypto_events from PostgreSQL
# 2. Registers as temp view: createOrReplaceTempView("crypto_events")
# 3. Runs 5 SQL queries:
#    Q1: AVG price per coin (GROUP BY)
#    Q2: RANK() price within each coin
#    Q3: LAG() previous price per coin
#    Q4: Price change calculation
#    Q5: Combined filter + group + global rank
# 4. Shows results to terminal
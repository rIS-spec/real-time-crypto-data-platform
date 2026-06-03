from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
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



df.createOrReplaceTempView("crypto_events")
print("Temp view registered")



print("\n--- Query 1: Average Price Per Coin ---")
spark.sql("""
    SELECT coin_id,
           ROUND(AVG(price_usd), 2) as avg_price
    FROM crypto_events
    GROUP BY coin_id
    ORDER BY avg_price DESC
""").show()




print("\n--- Query 2: Price Rank Per Coin ---")
spark.sql("""
    SELECT coin_id,
           price_usd,
           RANK() OVER (PARTITION BY coin_id ORDER BY price_usd DESC) as price_rank
    FROM crypto_events
""").show(10)




print("\n--- Query 3: Previous Price Per Coin ---")
spark.sql("""
    SELECT coin_id,
           price_usd,
           LAG(price_usd, 1) OVER (PARTITION BY coin_id ORDER BY fetched_at) as prev_price
    FROM crypto_events
""").show(10)





print("\n--- Query 4: Price Change Per Coin ---")
spark.sql("""
    SELECT coin_id,
           price_usd,
           ROUND(price_usd - LAG(price_usd, 1) OVER (PARTITION BY coin_id ORDER BY fetched_at), 2) as price_change
    FROM crypto_events
""").show(10)








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
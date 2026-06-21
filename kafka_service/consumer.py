
# Producer configs → sending logic to Kafka topic
# Consumer configs → reading logic,
#            and saves to PostgreSQL

# FastAPI → Producer → Kafka → Consumer → PostgreSQL

# dumps = dump to string
# loads = load from string


# Serializer sends, Deserializer reads

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2            # for connect with postgresql.
import json
import logging
import time
from api_service.config import get_settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

settings = get_settings()

# Connect → Listen → Decode → Insert → Retry → Commit
# 1. Create consumer
# 2. Connect DB
# 3. Read message
# 4. Insert with retry
# 5. Commit and log
def create_consumer():
    return KafkaConsumer(
        settings.KAFKA_TOPIC_CRYPTO,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,

        # Convert bytes → JSON → Python dict
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
          # Deserializer = convert bytes → Python data (after receiving from Kafka)

        # Consumer group (scalability)
        group_id="crypto-group-3",    #  Name of consumer group. All consumers with same group_id share the work. Kafka assigns partitions among them.

        # bookmark → never lose your place
        auto_offset_reset="earliest",    # start reading from the beginning of the topic if no offset is found in the consumer group

        # Auto save progress
        enable_auto_commit=False,  # it false becoz we want to commit manually 
        consumer_timeout_ms=-1    # Never stop waiting. Consumer runs forever, always listening for new messages. 
    )


def create_db_connection():   # connect with database.
    try:
        conn = psycopg2.connect(settings.POSTGRES_URL)
        logger.info("Connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        raise


# Create consumer and connect to DB and start listening 
def consume_crypto_from_kafka():
    try:
        consumer = create_consumer()
        conn = create_db_connection()
        cursor = conn.cursor()     # cursor is like a pen, the tool that actually runs SQL commands, on the database(executor)

        for message in consumer:
            data = message.value        # NEVER stops — waits for new messages forever.

            for attempt in range(3):   # retry DB insert
                try:
                    cursor.execute("""
                            INSERT INTO crypto_events 
                            (coin_id, coin_name, symbol, price_usd, 
                            price_change_24h, market_cap, volume_24h)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            data["coin_id"],
                            data["coin_name"],
                            data["symbol"],
                            data["price_usd"],
                            data["price_change_24h"],
                            data["market_cap"],
                            data["volume_24h"]
                        )
                    )      # writing on whiteboard

                    conn.commit()       # taking a photo of whiteboard, data saved permanently into postgresql, without it → data is lost on crash.
                    logger.info(f"Saved {data['coin_id']} to DB")
                    break

                except Exception as e:
                    logger.warning(f"DB Retry {attempt+1} failed: {e}")
                    conn.rollback()
                    time.sleep(2)

            else:
                logger.error(f"Failed to save {data['coin_id']} after 3 attempts")

    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        raise

    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise

    finally:                              
        cursor.close()
        conn.close()
        consumer.close()
        logger.info("Connections closed cleanly")




if __name__ == "__main__":
    consume_crypto_from_kafka()






# Run file
#    ↓
# Create Kafka consumer (connect to Kafka)
#    ↓
# Connect to PostgreSQL
#    ↓
# Start listening to Kafka topic
#    ↓
# Message arrives
#    ↓
# Convert bytes → JSON → dict
#    ↓
# Try inserting into DB
#    ↓
# If fail → retry 3 times
#    ↓
# If success → commit
#    ↓
# Repeat forever






# CoinGecko API
#       ↓
# FastAPI (fetch)
#       ↓
# Kafka Producer (send)
#       ↓
# Kafka Topic (crypto-events)
#       ↓
# Kafka Consumer (read)
#       ↓
# PostgreSQL (store)
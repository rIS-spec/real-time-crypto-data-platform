# The producer is the file that:
# Fetches live crypto prices from CoinGecko,
# Sends them INTO Kafka topic "crypto-events".

# kafka only understand bytes
# json.dumps() = converts Python dict to JSON
# json.loads() = converts JSON to Python dict


from kafka import KafkaProducer
from kafka.errors import KafkaError
from api_service.fetchers.crypto import fetch_crypto
from api_service.config import get_settings
import psycopg2
from datetime import datetime, timezone

import json  # json.dumps() = converts Python dict to JSON
import logging 
import time

logger = logging.getLogger(__name__)
settings = get_settings()




# 1. Create producer (connect Kafka)
# 2. Fetch crypto data
# 3. Loop each coin
# 4. Send with retry logic
# 5. Flush and log success

def create_producer():
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,     # Address of Kafka broker. In this case, localhost:9092 is the broker
        # Convert Python dict → JSON → bytes (Kafka required format)
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),  # Serializer = convert Python data → bytes (to send to Kafka)
        key_serializer=lambda x: x.encode('utf-8') if x else None,
        # Fault tolerance → retry if send fails
        retries=3,
        # Delivery guarantee → wait until message is safely stored
        acks='all',
        # Performance optimization → batch messages slightly
        linger_ms=5
    )


def log_pipeline_run(status, rows_processed=0, error_message=None, duration_seconds=0):
    try:
        conn = psycopg2.connect(settings.POSTGRES_URL)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_logs
            (pipeline_name, task_name, status, started_at, finished_at,
             duration_seconds, rows_processed, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "crypto_producer",
            "fetch_and_send",
            status,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            duration_seconds,
            rows_processed,
            error_message
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to log pipeline run: {e}")


def send_crypto_to_kafka():
    start_time = datetime.now(timezone.utc)
    rows_sent = 0
    try:
        producer = create_producer()
        prices = fetch_crypto()

        logger.info(f"Fetched {len(prices)} coins")

        for price in prices:
            for attempt in range(3):
                try:
                    producer.send(
                        topic=settings.KAFKA_TOPIC_CRYPTO,
                        value=price.model_dump(mode='json')
                    )
                    logger.info(f"Sent {price.coin_id}")
                    rows_sent += 1
                    break

                except KafkaError as e:
                    logger.warning(f"Retry {attempt+1} failed: {e}")
                    time.sleep(2)

        producer.flush()
        logger.info("All messages sent successfully")

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        log_pipeline_run("success", rows_processed=rows_sent, duration_seconds=duration)

    except Exception as e:
        logger.error(f"Producer failed: {e}")
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        log_pipeline_run("failed", rows_processed=rows_sent, error_message=str(e), duration_seconds=duration)
        raise


if __name__ == "__main__":
    send_crypto_to_kafka()

# producer.flush() = Empty your bag completely before leaving post office.

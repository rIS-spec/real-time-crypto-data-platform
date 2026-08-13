import json
import logging
import time

import psycopg2
from kafka import KafkaConsumer, TopicPartition

from api_service.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()

TARGET_MESSAGES = 5
START_OFFSET = 25


def main():

    consumer = KafkaConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=False,
    )

    partition = TopicPartition(
        settings.KAFKA_TOPIC_CRYPTO,
        0
    )

    consumer.assign([partition])
    consumer.seek(partition, START_OFFSET)

    conn = psycopg2.connect(settings.POSTGRES_URL)
    cursor = conn.cursor()

    logger.info(
        f"Starting benchmark from Kafka offset {START_OFFSET}"
    )

    count = 0
    start = time.perf_counter()

    try:

        for message in consumer:

            data = message.value

            cursor.execute(
                """
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
                    data["volume_24h"],
                ),
            )

            conn.commit()

            count += 1

            logger.info(
                f"Offset {message.offset} → "
                f"Saved {data['coin_id']} to PostgreSQL "
                f"({count}/{TARGET_MESSAGES})"
            )

            if count == TARGET_MESSAGES:
                elapsed = time.perf_counter() - start

                print("\n========== BENCHMARK ==========")
                print(f"Kafka offsets     : 25 - 29")
                print(f"Messages processed: {count}")
                print(f"Processing time   : {elapsed:.3f} sec")
                print(
                    f"Throughput        : "
                    f"{count / elapsed:.2f} events/sec"
                )
                print("================================")

                break

    finally:
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
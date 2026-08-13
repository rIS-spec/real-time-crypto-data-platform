import json
import logging
import time

import psycopg2
from kafka import KafkaConsumer, TopicPartition

from api_service.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()

TOTAL_MESSAGES = 1000
START_OFFSET = 30


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
        f"Waiting for {TOTAL_MESSAGES} messages "
        f"starting at offset {START_OFFSET}"
    )

    count = 0
    start = None

    try:

        for message in consumer:

            if count == 0:
                start = time.perf_counter()

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

            if count % 100 == 0:
                logger.info(
                    f"Processed {count}/{TOTAL_MESSAGES} "
                    f"(latest offset={message.offset})"
                )

            if count == TOTAL_MESSAGES:

                elapsed = time.perf_counter() - start

                print("\n========== CONSUMER LOAD TEST ==========")
                print(f"Kafka offsets : {START_OFFSET} - {START_OFFSET + TOTAL_MESSAGES - 1}")
                print(f"Messages     : {count}")
                print(f"Time         : {elapsed:.3f} sec")
                print(
                    f"Throughput   : "
                    f"{count / elapsed:.2f} events/sec"
                )
                print("Failures     : 0")
                print("=========================================")

                break

    finally:
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
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


def send_crypto_to_kafka():
    try:
        producer = create_producer()
        prices = fetch_crypto()

        logger.info(f"Fetched {len(prices)} coins")

        for price in prices:
            for attempt in range(3):   # Try sending message → if fails → try again (max 3 times)
                try:    #Try to send message — if error happens, go to except
                    producer.send(
                        topic=settings.KAFKA_TOPIC_CRYPTO,
                        value=price.model_dump(mode='json')
                    )           # Send this coin data to Kafka
                    logger.info(f"Sent {price.coin_id}")
                    break       # If success, break out of loop

                except KafkaError as e:
                    logger.warning(f"Retry {attempt+1} failed: {e}")
                    time.sleep(2)        # Wait 2 seconds before retry

        producer.flush()    # Ensure all messages sent, no loss.
        logger.info("All messages sent successfully")

    except Exception as e:
        logger.error(f"Producer failed: {e}")
        raise


if __name__ == "__main__":
    send_crypto_to_kafka()




# producer.flush() = Empty your bag completely before leaving post office.



# File runs
#    ↓
# Required tools imported (KafkaProducer, fetch_crypto, json, logging, settings)
#    ↓
# Settings loaded from config.py (.env values)
#    ↓
# Logger initialized (for tracking execution)
#    ↓
# Main function send_crypto_to_kafka() called
#    ↓
# Try block starts (safe execution)
#    ↓
# create_producer() called
#    ↓
# Connect to Kafka broker (localhost:9092)
#    ↓
# Producer configured with:
#       - retries (failure handling)
#       - acks='all' (message durability)
#       - linger_ms (batch optimization)
#    ↓
# Fetch crypto data from CoinGecko API (crypto.py)
#    ↓
# Receive list of coins (Bitcoin, Ethereum, etc.)
#    ↓
# Loop through each coin
#    ↓
# For each coin:
#       Try sending message to Kafka topic "crypto-events"
#       ↓
#       If success → log message sent
#       ↓
#       If failure → retry up to 3 times
#           - wait 2 seconds between retries
#    ↓
# After all messages processed:
#       Call producer.flush()
#       → ensure all messages sent immediately
#    ↓
# Log: "All messages sent successfully"
#    ↓
# Handle errors:
#       - KafkaError → retry handled
#       - General Exception → log error and raise
#    ↓
# Execution completes safely

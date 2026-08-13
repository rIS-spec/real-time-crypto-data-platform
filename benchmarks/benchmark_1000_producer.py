import json
import time

from kafka import KafkaProducer

from api_service.config import get_settings

settings = get_settings()

TOTAL_MESSAGES = 1000

coins = [
    ("bitcoin", "Bitcoin", "BTC"),
    ("ethereum", "Ethereum", "ETH"),
    ("ripple", "XRP", "XRP"),
    ("solana", "Solana", "SOL"),
    ("dogecoin", "Dogecoin", "DOGE"),
]


def create_event(i):
    coin_id, coin_name, symbol = coins[i % len(coins)]

    return {
        "coin_id": coin_id,
        "coin_name": coin_name,
        "symbol": symbol,
        "price_usd": 50000.0 + (i % 1000),
        "price_change_24h": 100.0,
        "price_change_pct_24h": 0.5,
        "market_cap": 1000000000.0,
        "volume_24h": 50000000.0,
        "high_24h": 51000.0,
        "low_24h": 49000.0,
        "fetched_at": "2026-08-13T07:00:00+00:00",
    }


producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

start = time.perf_counter()

for i in range(TOTAL_MESSAGES):
    producer.send(
        settings.KAFKA_TOPIC_CRYPTO,
        value=create_event(i),
    )

producer.flush()

elapsed = time.perf_counter() - start

print("\n========== PRODUCER LOAD TEST ==========")
print(f"Messages sent : {TOTAL_MESSAGES}")
print(f"Time          : {elapsed:.3f} sec")
print(f"Throughput    : {TOTAL_MESSAGES / elapsed:.2f} events/sec")
print("=========================================")

producer.close()
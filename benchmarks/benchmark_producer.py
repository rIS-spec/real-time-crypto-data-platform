import time
from kafka_service.producer import send_crypto_to_kafka

start = time.perf_counter()

send_crypto_to_kafka()

elapsed = time.perf_counter() - start

print(f"\nTotal producer time: {elapsed:.3f} seconds")
print("Events: 5")
print(f"Throughput: {5 / elapsed:.2f} events/sec")
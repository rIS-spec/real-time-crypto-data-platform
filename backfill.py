# PURPOSE: Fetches crypto prices repeatedly and saves to Neon's crypto_events table
# Run this once to build up enough history for the ML model to train on.

import time
from sqlalchemy import create_engine, text
from api_service.fetchers.crypto import fetch_crypto
from api_service.config import settings

NUM_FETCHES = 20      # how many times to fetch
DELAY_SECONDS = 15    # pause between fetches


def insert_prices(engine, prices):
    with engine.begin() as conn:
        for coin in prices:
            conn.execute(text("""
                INSERT INTO crypto_events
                (coin_id, coin_name, symbol, price_usd, price_change_24h,
                 price_change_pct_24h, market_cap, volume_24h,
                 high_24h, low_24h, fetched_at)
                VALUES
                (:coin_id, :coin_name, :symbol, :price_usd, :price_change_24h,
                 :price_change_pct_24h, :market_cap, :volume_24h,
                 :high_24h, :low_24h, :fetched_at)
            """), {
                "coin_id": coin.coin_id,
                "coin_name": coin.coin_name,
                "symbol": coin.symbol,
                "price_usd": coin.price_usd,
                "price_change_24h": coin.price_change_24h,
                "price_change_pct_24h": coin.price_change_pct_24h,
                "market_cap": coin.market_cap,
                "volume_24h": coin.volume_24h,
                "high_24h": coin.high_24h,
                "low_24h": coin.low_24h,
                "fetched_at": coin.fetched_at,
            })


def backfill():
    engine = create_engine(settings.POSTGRES_URL)
    for i in range(1, NUM_FETCHES + 1):
        print(f"\nFetch {i}/{NUM_FETCHES}...")
        prices = fetch_crypto()
        insert_prices(engine, prices)
        print(f"Inserted {len(prices)} rows.")
        if i < NUM_FETCHES:
            time.sleep(DELAY_SECONDS)
    print("\nBackfill complete!")


if __name__ == "__main__":
    backfill()
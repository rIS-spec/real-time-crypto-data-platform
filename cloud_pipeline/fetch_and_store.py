# cloud_pipeline/fetch_and_store.py
# PURPOSE: Lightweight version of producer.py + consumer.py combined,
#          WITHOUT Kafka. Used only for the live cloud demo, where
#          running Kafka isn't practical on free hosting.
#
# CoinGecko -> (fetch) -> Neon PostgreSQL (direct insert)

import os
import logging
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

# Reuse your existing fetch logic and schema — no duplication
from api_service.fetchers.crypto import fetch_crypto

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load variables from .env.cloud specifically (not the local .env)
load_dotenv(".env.cloud")

NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")


def get_cloud_connection():
    if not NEON_DATABASE_URL:
        raise ValueError(
            "NEON_DATABASE_URL not found. Check that .env.cloud exists "
            "and contains NEON_DATABASE_URL=..."
        )
    return psycopg2.connect(NEON_DATABASE_URL)


def log_pipeline_run(conn, status, rows_processed=0, error_message=None, duration_seconds=0):
    # Reuses the SAME connection passed in, instead of opening a new one.
    # Opening a fresh connection just for logging was hanging/unreliable.
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_logs
            (pipeline_name, task_name, status, started_at, finished_at,
             duration_seconds, rows_processed, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "cloud_demo_pipeline",
            "fetch_and_store",
            status,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            duration_seconds,
            rows_processed,
            error_message
        ))
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Failed to log pipeline run: {e}")


def fetch_and_store():
    start_time = datetime.now(timezone.utc)
    rows_saved = 0

    try:
        prices = fetch_crypto()
        logger.info(f"Fetched {len(prices)} coins")

        conn = get_cloud_connection()
        cursor = conn.cursor()

        for price in prices:
            cursor.execute("""
                INSERT INTO crypto_events
                (coin_id, coin_name, symbol, price_usd,
                 price_change_24h, price_change_pct_24h, market_cap, volume_24h,
                 high_24h, low_24h)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                price.coin_id,
                price.coin_name,
                price.symbol,
                price.price_usd,
                price.price_change_24h,
                price.price_change_pct_24h,
                price.market_cap,
                price.volume_24h,
                price.high_24h,
                price.low_24h
            ))
            logger.info(f"Saved {price.coin_id} to Neon")
            rows_saved += 1

        conn.commit()
        cursor.close()

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        log_pipeline_run(conn, "success", rows_processed=rows_saved, duration_seconds=duration)
        conn.close()
        logger.info(f"Done. Saved {rows_saved} rows in {duration:.2f}s")

    except Exception as e:
        logger.error(f"Cloud pipeline failed: {e}")
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        try:
            log_pipeline_run(conn, "failed", rows_processed=rows_saved, error_message=str(e), duration_seconds=duration)
            conn.close()
        except Exception:
            pass
        raise


if __name__ == "__main__":
    fetch_and_store()
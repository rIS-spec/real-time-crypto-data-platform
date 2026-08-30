# PURPOSE: Defines all API endpoints for crypto data
# File: api_service/routes.py

from fastapi import APIRouter, HTTPException
from api_service.schemas import CryptoPriceResponse, CryptoPrice
from api_service.fetchers.crypto import fetch_crypto
from api_service.config import get_settings
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()

router = APIRouter(prefix="/crypto", tags=["Crypto"])   # /crypto/prices = endpoint for live prices


@router.get("/health")
def health_check():
    try:
        conn = psycopg2.connect(settings.POSTGRES_URL)
        conn.close()
        return {
            "status": "ok",
            "service": "crypto-api",
            "database": "connected",
            "kafka": settings.KAFKA_BOOTSTRAP_SERVERS
        }
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return {
            "status": "ok",
            "service": "crypto-api",
            "database": "disconnected",
            "error": str(e),
            "kafka": settings.KAFKA_BOOTSTRAP_SERVERS
        }



@router.get("/prices", response_model=CryptoPriceResponse)
def get_live_prices():
    # NOTE: This used to call CoinGecko directly on every request (fetch_crypto()).
    # On Render's free tier, many apps share the same outbound IP range, so
    # CoinGecko's rate limiter blocked us even though our own traffic was low.
    # Instead, we now read the latest row per coin from crypto_events, which
    # is kept fresh every 20 minutes by the GitHub Actions cloud pipeline.
    # This also decouples the read API from an external dependency, which is
    # generally better practice for a read endpoint like this.
    try:
        logger.info("Fetching latest prices from database")
        conn = psycopg2.connect(settings.POSTGRES_URL)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ON (coin_id)
                coin_id, coin_name, symbol, price_usd,
                price_change_24h, price_change_pct_24h,
                market_cap, volume_24h, high_24h, low_24h, fetched_at
            FROM crypto_events
            ORDER BY coin_id, fetched_at DESC
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        prices = []
        for row in rows:
            prices.append(CryptoPrice(
                coin_id=row[0],
                coin_name=row[1],
                symbol=row[2],
                price_usd=float(row[3]),
                price_change_24h=float(row[4]) if row[4] is not None else None,
                price_change_pct_24h=float(row[5]) if row[5] is not None else None,
                market_cap=float(row[6]) if row[6] is not None else None,
                volume_24h=float(row[7]) if row[7] is not None else None,
                high_24h=float(row[8]) if row[8] is not None else None,
                low_24h=float(row[9]) if row[9] is not None else None,
                fetched_at=row[10]
            ))

        return CryptoPriceResponse(
            success=True,
            message="Prices fetched from database successfully",
            data=prices,
            total_coins=len(prices)
        )
    except Exception as e:
        logger.error(f"Failed to fetch prices from database: {e}")
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/prices/history")
def get_price_history(coin: str = "bitcoin", limit: int = 10):
    try:
        conn = psycopg2.connect(settings.POSTGRES_URL)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT coin_id, coin_name, price_usd, fetched_at
            FROM crypto_events
            WHERE coin_id = %s
            ORDER BY fetched_at DESC
            LIMIT %s
        """, (coin, limit))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        results = []
        for row in rows:
            results.append({
                "coin_id": row[0],
                "coin_name": row[1],
                "price_usd": float(row[2]),
                "fetched_at": str(row[3])
            })

        return {
            "success": True,
            "coin": coin,
            "total_records": len(results),
            "data": results
        }
    except Exception as e:
        logger.error(f"Failed to fetch history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prices/{coin_id}")
def get_coin_price(coin_id: str):
    try:
        conn = psycopg2.connect(settings.POSTGRES_URL)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT coin_id, coin_name, symbol, price_usd, fetched_at
            FROM crypto_events
            WHERE coin_id = %s
            ORDER BY fetched_at DESC
            LIMIT 1
        """, (coin_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Coin '{coin_id}' not found in database"
            )

        return {
            "coin_id": row[0],
            "coin_name": row[1],
            "symbol": row[2],
            "price_usd": float(row[3]),
            "fetched_at": str(row[4])
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch coin {coin_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
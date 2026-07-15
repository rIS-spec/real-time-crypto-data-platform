# PURPOSE: Defines all API endpoints for crypto data
# File: api_service/routes.py

from fastapi import APIRouter, HTTPException
from api_service.schemas import CryptoPriceResponse
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
        db_status = "connected"
    except Exception:
        db_status = "disconnected"

    return {
        "status": "ok",
        "service": "crypto-api",
        "database": db_status,
        "kafka": settings.KAFKA_BOOTSTRAP_SERVERS
    }


@router.get("/prices", response_model=CryptoPriceResponse)
def get_live_prices():
    try:
        logger.info("Fetching live prices from CoinGecko")
        prices = fetch_crypto()
        return CryptoPriceResponse(
            success=True,
            message="Live prices fetched successfully",
            data=prices,
            total_coins=len(prices)
        )
    except Exception as e:
        logger.error(f"Failed to fetch live prices: {e}")
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
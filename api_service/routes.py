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



# File runs
#    ↓
# Router created with prefix /crypto
#    ↓
# Settings loaded → POSTGRES_URL available
#    ↓
# 4 endpoints registered
#    ↓
# Request comes in → matched to correct endpoint
#    ↓
# Endpoint runs its logic
#    ↓
# Returns JSON response
#    ↓
# Done!



# **Dry run — user visits `/crypto/prices/dogecoin`:**
#
# GET /crypto/prices/dogecoin
#     ↓
# coin_id = "dogecoin" (from URL path)
#     ↓
# psycopg2.connect() → connect to PostgreSQL
#     ↓
# cursor.execute(SELECT ... WHERE coin_id = 'dogecoin' LIMIT 1)
#     ↓
# row = cursor.fetchone()
#     ↓
# row = ("dogecoin", "Dogecoin", "DOGE", 0.09, "2026-03-28...")
#     ↓
# row is not None → skip 404
#     ↓
# cursor.close(), conn.close()
#     ↓
# return {
#   "coin_id": "dogecoin",
#   "coin_name": "Dogecoin",
#   "symbol": "DOGE",
#   "price_usd": 0.09,
#   "fetched_at": "2026-03-28..."
# }
#     ↓
# Status 200 OK 





# **Dry run — user visits `/crypto/prices/cardano` (not in DB):**
#
# GET /crypto/prices/cardano
#     ↓
# coin_id = "cardano"
#     ↓
# cursor.execute(SELECT ... WHERE coin_id = 'cardano' LIMIT 1)
#     ↓
# row = cursor.fetchone() → None (not found)
#     ↓
# if not row → True
#     ↓
# raise HTTPException(status_code=404,
#   detail="Coin 'cardano' not found in database")
#     ↓
# except HTTPException → raise (passes through)
#     ↓
# User sees:
# {
#   "detail": "Coin 'cardano' not found in database"
# }
# Status 404 Not Found 








# LINE: router = APIRouter(prefix="/crypto", tags=["Crypto"])
# MEANS: Creates a group of related endpoints
#        prefix="/crypto" → every URL starts with /crypto
#        So /health becomes /crypto/health automatically
#        tags=["Crypto"] → groups them in /docs page

# ─────────────────────────────────────────
# ENDPOINT 1: GET /crypto/health
# ─────────────────────────────────────────

# LINE: @router.get("/health")
# MEANS: When someone visits GET /crypto/health
#        run health_check() function below
#        No response_model → returns raw dict

# LINE: conn = psycopg2.connect(settings.POSTGRES_URL)
# MEANS: Actually tries to connect to PostgreSQL
#        If connection works → db_status = "connected"
#        If connection fails → db_status = "disconnected"
#        This is a REAL check, not just "I am alive"

# LINE: conn.close()
# MEANS: Close the connection immediately after checking
#        Health check should not hold connections open
#        Open connections waste PostgreSQL resources

# LINE: "kafka": settings.KAFKA_BOOTSTRAP_SERVERS
# MEANS: Shows which Kafka server is configured
#        Does not actually check Kafka — just shows the address
#        Full Kafka health check would need KafkaAdminClient

# ─────────────────────────────────────────
# ENDPOINT 2: GET /crypto/prices
# ─────────────────────────────────────────

# LINE: @router.get("/prices", response_model=CryptoPriceResponse)
# MEANS: response_model = FastAPI validates the return value
#        against CryptoPriceResponse shape
#        If return value does not match → FastAPI raises error
#        Ensures consistent response format always

# LINE: raise HTTPException(status_code=503, detail=str(e))
# MEANS: 503 = Service Unavailable
#        Used specifically when CoinGecko is down
#        NOT 500 because our code is fine
#        The EXTERNAL dependency failed

# ─────────────────────────────────────────
# ENDPOINT 3: GET /crypto/prices/history
# ─────────────────────────────────────────

# LINE: def get_price_history(coin: str = "bitcoin", limit: int = 10)
# MEANS: coin and limit are QUERY PARAMETERS
#        They come after ? in the URL
#        /prices/history?coin=ethereum&limit=5
#        = "bitcoin" and = 10 are DEFAULT VALUES
#        Used when user does not specify anything

# LINE: cursor.execute(""" SELECT ... WHERE coin_id = %s """, (coin, limit))
# MEANS: %s = placeholder for values
#        (coin, limit) = actual values inserted safely
#        NEVER use f-strings in SQL → SQL injection risk
#        %s with tuple = safe parameterized query

# LINE: rows = cursor.fetchall()
# MEANS: Gets ALL matching rows from PostgreSQL
#        Returns a list of tuples
#        Each tuple = one row from the table

# LINE: for row in rows: results.append({"coin_id": row[0]...})
# MEANS: row[0] = first column = coin_id
#        row[1] = second column = coin_name
#        row[2] = third column = price_usd
#        row[3] = fourth column = fetched_at
#        We convert tuple to dictionary for JSON response

# ─────────────────────────────────────────
# ENDPOINT 4: GET /crypto/prices/{coin_id}
# ─────────────────────────────────────────

# LINE: @router.get("/prices/{coin_id}")
# MEANS: {coin_id} is a PATH PARAMETER
#        It comes directly in the URL — not after ?
#        /crypto/prices/bitcoin → coin_id = "bitcoin"
#        /crypto/prices/ethereum → coin_id = "ethereum"

# LINE: row = cursor.fetchone()
# MEANS: Gets only ONE row — the most recent price
#        fetchone() vs fetchall():
#        fetchone = one row (for single coin lookup)
#        fetchall = all matching rows (for history)

# LINE: if not row:
# MEANS: If fetchone() returns None → coin not in database
#        Raise 404 Not Found
#        404 = resource does not exist
#        NOT 500 — this is expected, not a crash

# LINE: except HTTPException: raise
# MEANS: If HTTPException was already raised (like 404)
#        do NOT catch it here — let it pass through
#        Without this line, the 404 would be caught by
#        the general except and turned into a 500 error

# LINE: cursor.close() / conn.close()
# MEANS: Always close cursor and connection after use
#        Prevents connection leaks in PostgreSQL
#        Each unclosed connection wastes server resources



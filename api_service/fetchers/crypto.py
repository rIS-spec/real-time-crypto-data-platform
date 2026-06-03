# PURPOSE: Fetches live crypto prices from CoinGecko API

import requests
import logging
import time
from datetime import datetime, timezone
from api_service.config import settings
from api_service.schemas import CryptoPrice
from typing import List

# Set up logging — shows messages in terminal
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3   # maximum number of times to retry
RETRY_BACKOFF = 2  # seconds that we wait before retrying


def fetch_crypto() -> List[CryptoPrice]:

    url = f"{settings.CRYPTO_API_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(settings.COINS),
        "order": "market_cap_desc",
        "per_page": 5,
        "page": 1,
        "price_change_percentage": "24h"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Attempt {attempt} — fetching crypto prices")
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 429:
                wait_time = RETRY_BACKOFF * attempt * 2
                logger.warning(f"Rate limited by CoinGecko. Waiting {wait_time}s")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            raw_data = response.json()
            logger.info(f"Successfully fetched {len(raw_data)} coins")

            crypto_prices = []
            for coin in raw_data:
                crypto_price = CryptoPrice(
                    coin_id=coin["id"],
                    coin_name=coin["name"],
                    symbol=coin["symbol"].upper(),
                    price_usd=coin["current_price"],
                    price_change_24h=coin.get("price_change_24h"),
                    price_change_pct_24h=coin.get("price_change_percentage_24h"),
                    market_cap=coin.get("market_cap"),
                    volume_24h=coin.get("total_volume"),
                    high_24h=coin.get("high_24h"),
                    low_24h=coin.get("low_24h"),
                    fetched_at=datetime.now(timezone.utc)
                )
                crypto_prices.append(crypto_price)

            return crypto_prices
        
        except requests.exceptions.Timeout:
            logger.warning(f"Attempt {attempt} timed out")
            if attempt == MAX_RETRIES:
                logger.error("All retries exhausted — CoinGecko timeout")
                raise
            time.sleep(RETRY_BACKOFF * attempt)

        except requests.exceptions.ConnectionError:
            logger.warning(f"Attempt {attempt} connection failed")
            if attempt == MAX_RETRIES:
                logger.error("All retries exhausted — cannot connect")
                raise
            time.sleep(RETRY_BACKOFF * attempt)

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error on attempt {attempt}: {e}")
            raise

        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt}: {e}")
            raise

    logger.error("All retries exhausted")
    raise Exception("Failed to fetch crypto prices after all retries")


if __name__ == "__main__":
    prices = fetch_crypto()
    for coin in prices:
        print(f"\n{coin.coin_name} ({coin.symbol})")
        print(f"  Price:     ${coin.price_usd:,.2f}")
        print(f"  Fetched at: {coin.fetched_at}")


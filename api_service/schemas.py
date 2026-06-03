# PURPOSE: Defines the shape of crypto data using Pydantic

from pydantic import BaseModel, Field, field_validator
from typing import Optional, Literal, List
from datetime import datetime, timezone

# Input schema 
# This is what we RECEIVE from CoinGecko API
class CryptoPrice(BaseModel):
    coin_id: str = Field(min_length=1, max_length=50)
    coin_name: str = Field(min_length=1, max_length=100)
    symbol: str = Field(min_length=1, max_length=10)
    price_usd: float = Field(gt=0)
    price_change_24h: Optional[float] = None
    price_change_pct_24h: Optional[float] = None
    market_cap: Optional[float] = Field(default=None, ge=0)
    volume_24h: Optional[float] = Field(default=None, ge=0)
    high_24h: Optional[float] = Field(default=None, gt=0)
    low_24h: Optional[float] = Field(default=None, gt=0)
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    class Config:
        from_attributes = True

    @field_validator('symbol')
    @classmethod
    def symbol_must_be_uppercase(cls, v):
        return v.upper()

    @field_validator('coin_id')
    @classmethod
    def coin_id_must_be_lowercase(cls, v):
        return v.lower().strip()


# Output schema 
# This is what we RETURN from our API endpoints
class CryptoPriceResponse(BaseModel):
    success: bool = True
    message: str = "Data fetched successfully"
    data: List[CryptoPrice]
    total_coins: int


# Pipeline log schema 
# Used when recording pipeline runs in pipeline_logs table
class PipelineLog(BaseModel):
    pipeline_name: str = Field(min_length=1)
    task_name: str = Field(min_length=1)
    status: Literal["running", "success", "failed"]
    rows_processed: int = Field(default=0, ge=0)
    error_message: Optional[str] = None
    dag_run_id: Optional[str] = None




# File runs
#    ↓
# Constants loaded (MAX_RETRIES = 3, RETRY_BACKOFF = 2)
#    ↓
# fetch_crypto() called
#    ↓
# Build CoinGecko URL + params
#    ↓
# Start retry loop (attempt 1, 2, 3)
#    ↓
# Call CoinGecko API
#    ↓
# Got 429? → wait and continue to next attempt
#    ↓
# Got response → raise_for_status()
#    ↓
# Parse JSON → loop each coin
#    ↓
# Convert to CryptoPrice object
#    ↓
# Return list of 5 coins
#    ↓
# If Timeout/ConnectionError → wait → retry
#    ↓
# If all retries exhausted → raise Exception
#    ↓
# Done!

# LIKE 

# **Full dry run — what happens when CoinGecko times out twice then succeeds:**
# ```
# attempt = 1
#   → requests.get() → Timeout after 10s
#   → except Timeout caught
#   → attempt 1 != MAX_RETRIES (3)
#   → time.sleep(2 * 1) = sleep 2 seconds
#       ↓
# attempt = 2
#   → requests.get() → Timeout again
#   → except Timeout caught
#   → attempt 2 != MAX_RETRIES (3)
#   → time.sleep(2 * 2) = sleep 4 seconds
#       ↓
# attempt = 3
#   → requests.get() → SUCCESS
#   → status 200
#   → raise_for_status() passes
#   → parse JSON → build CryptoPrice objects
#   → return list of 5 coins ✅





# LINE: import time
# MEANS: Python's built-in library for time operations
#        We use time.sleep(seconds) to pause between retries
#        Without this we cannot add wait time

# LINE: MAX_RETRIES = 3
# MEANS: Maximum number of retry attempts
#        Capital letters = constant, never changes
#        If CoinGecko fails 3 times → give up and raise error

# LINE: RETRY_BACKOFF = 2
# MEANS: Base wait time in seconds between retries
#        Used to calculate: wait = RETRY_BACKOFF * attempt
#        Attempt 1 → wait 2s, Attempt 2 → wait 4s, Attempt 3 → wait 6s

# LINE: for attempt in range(1, MAX_RETRIES + 1):
# MEANS: Loop runs 3 times: attempt = 1, 2, 3
#        range(1, 4) = [1, 2, 3]
#        We start from 1 (not 0) so log messages say
#        "Attempt 1" not "Attempt 0" — more readable

# LINE: logger.info(f"Attempt {attempt} — fetching crypto prices")
# MEANS: Logs which attempt number we are on
#        In terminal you see: "Attempt 1 — fetching crypto prices"
#        Helps you know if retries are happening

# LINE: if response.status_code == 429:
# MEANS: 429 = Too Many Requests = CoinGecko rate limited us
#        We sent too many requests too fast
#        CoinGecko says "slow down"
#        We must wait before trying again

# LINE: wait_time = RETRY_BACKOFF * attempt * 2
# MEANS: Calculates how long to wait for rate limit
#        Attempt 1 → 2 * 1 * 2 = 4 seconds
#        Attempt 2 → 2 * 2 * 2 = 8 seconds
#        Attempt 3 → 2 * 3 * 2 = 12 seconds
#        Longer waits each time = exponential backoff

# LINE: time.sleep(wait_time)
# MEANS: Pauses the program for wait_time seconds
#        Program is completely frozen during this time
#        This gives CoinGecko time to recover

# LINE: continue
# MEANS: Skip rest of this loop iteration
#        Go back to top of for loop
#        Try the API call again with next attempt number

# LINE: response.raise_for_status()
# MEANS: Checks HTTP status code
#        200 = success → do nothing, continue
#        4xx/5xx = raise HTTPError immediately
#        We put this AFTER the 429 check because
#        429 needs special handling (wait + retry)

# LINE: except requests.exceptions.Timeout:
# MEANS: Catches when API takes more than 10 seconds
#        Worth retrying — CoinGecko might have been busy
#        if attempt == MAX_RETRIES → all retries used up → raise
#        Otherwise → wait and try again

# LINE: if attempt == MAX_RETRIES:
# MEANS: Check if this was the last allowed attempt
#        If yes → no more retries left → raise the error
#        If no → wait and let the loop try again

# LINE: time.sleep(RETRY_BACKOFF * attempt)
# MEANS: Wait before next retry
#        Attempt 1 fails → wait 2 * 1 = 2 seconds
#        Attempt 2 fails → wait 2 * 2 = 4 seconds
#        Each retry waits longer = exponential backoff

# LINE: except requests.exceptions.ConnectionError:
# MEANS: Catches when cannot reach CoinGecko at all
#        No internet, DNS failure, CoinGecko completely down
#        Worth retrying — temporary network issues fix themselves
#        Same retry logic as Timeout

# LINE: except requests.exceptions.HTTPError as e:
# MEANS: Catches when CoinGecko responds with error code
#        401 = wrong API key, 404 = URL wrong, 500 = their server crashed
#        NOT worth retrying — server gave a clear answer
#        raise immediately without waiting

# LINE: except Exception as e:
# MEANS: Catches ANY other unexpected error
#        Something we did not anticipate
#        raise immediately — we don't know if it's safe to retry

# LINE: raise Exception("Failed to fetch crypto prices after all retries")
# MEANS: This line runs ONLY if the for loop finishes
#        without returning — meaning all 3 attempts failed
#        Raises a clear error message to the caller
#        Caller = routes.py → returns 503 to the browser
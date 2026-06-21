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



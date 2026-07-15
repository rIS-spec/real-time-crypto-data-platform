# PURPOSE: Reads .env file and shares settings with all files

from pydantic import field_validator, model_validator
from typing import Literal
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):    # basesettings means we can use env vars as settings

    # PostgreSQL
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = "data_platform"
    POSTGRES_URL: str = ""

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_CRYPTO: str = "crypto-events"

    # CoinGecko API
    CRYPTO_API_URL: str = "https://api.coingecko.com/api/v3"

    # Coins to track
    COINS: list = ["bitcoin", "ethereum", "solana", "dogecoin", "ripple"]
    
    # App settings
    LOG_LEVEL: str = "INFO"
    ENVIRONMENT: Literal["development", "staging", "production"] = "development"


    @model_validator(mode="after")
    def validate_postgres_url(self):
        if self.POSTGRES_DB not in self.POSTGRES_URL:
            raise ValueError(f"POSTGRES_URL must contain database: {self.POSTGRES_DB}")
        return self

    # Read .env file automatically if it exists
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


# Single instance used everywhere
settings = get_settings()

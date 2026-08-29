# PURPOSE: Reads .env file and shares settings with all files

from pydantic import field_validator, model_validator, computed_field
from typing import Literal
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    # PostgreSQL
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "data_platform"
    # POSTGRES_URL is now computed dynamically

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

    @computed_field
    @property
    def POSTGRES_URL(self) -> str:
        """Build PostgreSQL URL from individual components."""
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

@lru_cache()
def get_settings() -> Settings:
    return Settings()

# Single instance used everywhere
settings = get_settings()
# PURPOSE: Reads .env file and shares settings with all files

from pydantic import field_validator, model_validator
from typing import Literal
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):

    # PostgreSQL
    POSTGRES_USER: str = "arish"
    POSTGRES_PASSWORD: str = "Arish200502"
    POSTGRES_DB: str = "data_platform"
    POSTGRES_URL: str = "postgresql://arish:Arish200502@localhost:5432/data_platform"

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


    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


# Single instance used everywhere
settings = get_settings()




# File runs
#    ↓
# Python reads this file
#    ↓
# Settings class defined with all fields
#    ↓
# get_settings() called at bottom of file
#    ↓
# lru_cache checks → first time? create Settings object
#    ↓
# Settings() reads .env file automatically
#    ↓
# .env values override default values
#    ↓
# Settings object saved in cache
#    ↓
# settings variable ready to use
#    ↓
# Any file that does:
# from api_service.config import settings
# gets the SAME cached object instantly
#    ↓
# Done!




# **Dry run — what happens when producer.py imports settings:**
# ```
# producer.py runs
#     ↓
# from api_service.config import get_settings
#     ↓
# Python imports config.py
#     ↓
# settings = get_settings() runs at bottom of config.py
#     ↓
# lru_cache checks → first time → run function
#     ↓
# Settings() created
#     ↓
# Reads .env file:
#   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
#   KAFKA_TOPIC_CRYPTO=crypto-events
#     ↓
# Fields populated:
#   settings.KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
#   settings.KAFKA_TOPIC_CRYPTO = "crypto-events"
#     ↓
# Object cached by lru_cache
#     ↓
# producer.py uses settings.KAFKA_BOOTSTRAP_SERVERS
#     ↓
# consumer.py imports settings
#     ↓
# lru_cache → already cached → return same object instantly
#     ↓
# No .env reading again 

# **Dry run — what happens if .env has a value:**
# ```
# .env file contains:
# ENVIRONMENT=production

# Settings class has:
# ENVIRONMENT: str = "development"  ← default

# When Settings() is created:
#     ↓
# BaseSettings reads .env
#     ↓
# Finds ENVIRONMENT=production
#     ↓
# Overrides default "development" with "production"
#     ↓
# settings.ENVIRONMENT = "production" 






# LINE: from pydantic_settings import BaseSettings
# MEANS: BaseSettings is a special Pydantic class
#        that knows how to read .env files automatically
#        Regular BaseModel only validates data shapes
#        BaseSettings validates AND reads from environment

# LINE: from functools import lru_cache
# MEANS: lru = Least Recently Used
#        A caching tool built into Python
#        Saves the result of a function call
#        Next time the same function is called
#        returns saved result instead of running again

# LINE: class Settings(BaseSettings):
# MEANS: Our Settings class inherits from BaseSettings
#        This gives it the power to:
#        1. Read values from .env file
#        2. Read values from system environment variables
#        3. Validate all values using Pydantic rules
#        4. Use default values if .env has nothing

# LINE: POSTGRES_USER: str = "127.0.0.1"
# MEANS: Type hint says this must be a string
#        "127.0.0.1" is the DEFAULT value
#        If .env file has POSTGRES_USER=arish
#        then "arish" overrides "127.0.0.1"
#        If .env has nothing → "127.0.0.1" is used

# LINE: POSTGRES_URL: str = "postgresql://arish:..."
# MEANS: Full database connection string
#        Format: postgresql://user:password@host:port/dbname
#        psycopg2 and SQLAlchemy both understand this format
#        This is what consumer.py uses to connect to DB

# LINE: KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
# MEANS: Address of your Kafka broker
#        producer.py and consumer.py both use this
#        In production this would be a real server address
#        like "kafka.mycompany.com:9092"

# LINE: KAFKA_TOPIC_CRYPTO: str = "crypto-events"
# MEANS: Name of the Kafka topic we use
#        producer.py sends TO this topic
#        consumer.py reads FROM this topic
#        topics.py creates this topic
#        All three files use the same value from here

# LINE: CRYPTO_API_URL: str = "https://api.coingecko.com/api/v3"
# MEANS: Base URL of CoinGecko API
#        crypto.py adds "/coins/markets" to this
#        Result: "https://api.coingecko.com/api/v3/coins/markets"
#        Keeping base URL here means if CoinGecko changes
#        their URL, we update in ONE place only

# LINE: COINS: list = ["bitcoin", "ethereum", "solana", "dogecoin", "ripple"]
# MEANS: List of coins we track
#        crypto.py joins this list into "bitcoin,ethereum,..."
#        and sends to CoinGecko as query parameter
#        Add or remove coins here — everything updates

# LINE: LOG_LEVEL: str = "INFO"
# MEANS: Controls how much logging you see
#        INFO = normal messages + warnings + errors
#        DEBUG = everything including very detailed messages
#        WARNING = only warnings and errors
#        ERROR = only errors

# LINE: ENVIRONMENT: str = "development"
# MEANS: Tells the app which environment it is running in
#        "development" = local machine
#        "production" = live server
#        Can be used to change behaviour:
#        if settings.ENVIRONMENT == "production": use real secrets

# LINE: class Config:
# MEANS: Inner class that configures how BaseSettings behaves
#        Not a data class — a configuration class for Pydantic

# LINE: env_file = ".env"
# MEANS: "Look for a file called .env in the project root"
#        Read all KEY=VALUE pairs from it
#        Override any matching field in Settings class

# LINE: env_file_encoding = "utf-8"
# MEANS: Read the .env file using UTF-8 encoding
#        Prevents issues with special characters
#        in passwords or values

# LINE: @lru_cache()
# MEANS: Decorator that caches the return value
#        First call → runs get_settings() → creates Settings object
#        Second call → returns saved object, skips creation
#        Without this → .env file read 100 times per request
#        With this → .env file read ONCE, forever reused

# LINE: def get_settings() -> Settings:
# MEANS: Function that creates and returns Settings object
#        -> Settings = return type hint
#        Called at bottom of file AND by other files
#        via: from api_service.config import get_settings

# LINE: return Settings()
# MEANS: Creates one Settings object
#        BaseSettings reads .env automatically here
#        Pydantic validates all field types here
#        If .env has wrong type → error raised here

# LINE: settings = get_settings()
# MEANS: Creates the settings object when this file is imported
#        All other files just do:
#        from api_service.config import settings
#        and get the same cached object instantly
#        No need to call get_settings() themselves

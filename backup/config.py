from typing import Dict, Any
import os
from dataclasses import dataclass
from datetime import timedelta

@dataclass
class APIConfig:
    api_key: str
    base_url: str
    requests_per_minute: int
    delay_between_calls: float

class Config:
    # API Configuration
    API_CONFIGS = {
        'alpha_vantage': APIConfig(
            api_key=os.getenv('ALPHA_VANTAGE_KEY'),
            base_url='https://www.alphavantage.co/query',
            requests_per_minute=5,  # Conservative limit for free tier
            delay_between_calls=12.0
        ),
        'finnhub': APIConfig(
            api_key=os.getenv('FINNHUB_KEY', ''),
            base_url='https://finnhub.io/api/v1',
            requests_per_minute=30,
            delay_between_calls=2.0
        ),
        'fmp': APIConfig(
            api_key=os.getenv('FMP_KEY', ''),
            base_url='https://financialmodelingprep.com/api/v3',
            requests_per_minute=10,
            delay_between_calls=6.0
        )
    }

    # Cache Configuration
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    
    # Cache durations
    MARKET_CAP_CACHE_DURATION = timedelta(hours=24)
    PRICE_CACHE_DURATION = timedelta(hours=24)
    UNIVERSE_CACHE_DURATION = timedelta(days=7)
    
    # DuckDB Configuration
    DUCKDB_PATH = os.getenv('DUCKDB_PATH', 'market_data.db')
    
    # Data Quality Settings
    MAX_MARKET_CAP_AGE = timedelta(days=7)  # Maximum age for using historical market cap
    MAX_RETRIES = 3  # Maximum retry attempts per API
    BATCH_SIZE = 1  # Process one symbol at a time for better error handling
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'market_data.log')

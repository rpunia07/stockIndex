from dataclasses import dataclass
from typing import Optional

@dataclass
class APIConfig:
    """Configuration for an API provider."""
    api_key: str
    base_url: str
    rate_limit: int  # requests per minute
    retry_count: int = 3
    retry_delay: int = 20  # seconds

    def __post_init__(self):
        if not self.api_key and self.api_key != "":  # Allow empty string for providers that don't need API key
            raise ValueError("API key is required")
        if not self.base_url:
            raise ValueError("Base URL is required")
        if self.rate_limit < 1:
            raise ValueError("Rate limit must be at least 1 request per minute")
        if self.retry_count < 0:
            raise ValueError("Retry count must be non-negative")
        if self.retry_delay < 0:
            raise ValueError("Retry delay must be non-negative")

# Default configurations for each provider
class DefaultConfigs:
    ALPHA_VANTAGE = {
        "base_url": "https://www.alphavantage.co/query",
        "rate_limit": 5,  # requests per minute
        "retry_count": 3,
        "retry_delay": 20
    }
    
    FINNHUB = {
        "base_url": "https://finnhub.io/api/v1",
        "rate_limit": 30,  # requests per minute
        "retry_count": 3,
        "retry_delay": 20
    }
    
    YAHOO_FINANCE = {
        "base_url": "https://query1.finance.yahoo.com/v8/finance",
        "rate_limit": 100,  # requests per minute
        "retry_count": 3,
        "retry_delay": 20
    }

def create_alpha_vantage_config(api_key: str) -> APIConfig:
    """Create Alpha Vantage API configuration."""
    return APIConfig(
        api_key=api_key,
        **DefaultConfigs.ALPHA_VANTAGE
    )

def create_finnhub_config(api_key: str) -> APIConfig:
    """Create Finnhub API configuration."""
    return APIConfig(
        api_key=api_key,
        **DefaultConfigs.FINNHUB
    )

def create_yahoo_finance_config() -> APIConfig:
    """Create Yahoo Finance API configuration (no API key needed)."""
    return APIConfig(
        api_key="",  # Yahoo Finance doesn't need an API key
        **DefaultConfigs.YAHOO_FINANCE
    )

from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
import asyncio
from abc import ABC, abstractmethod
from aiohttp import ClientSession, ClientTimeout
import logging

from .config import APIConfig

logger = logging.getLogger(__name__)

class MarketDataProvider(ABC):
    def __init__(self, config: APIConfig):
        self.config = config
        self.last_request_time = {}
        self.request_semaphore = asyncio.Semaphore(1)

    async def _wait_for_rate_limit(self, endpoint: str):
        """Implements rate limiting for API requests."""
        now = datetime.now()
        if endpoint in self.last_request_time:
            last_request = self.last_request_time[endpoint]
            time_since_last = (now - last_request).total_seconds()
            min_interval = 60 / self.config.requests_per_minute
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)
        self.last_request_time[endpoint] = now

    @abstractmethod
    async def get_market_cap(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Get market cap for a symbol."""
        pass

    @abstractmethod
    async def get_daily_data(self, symbol: str, start_date: datetime, end_date: datetime, 
                            session: ClientSession) -> List[Dict[str, Any]]:
        """Get daily price and volume data."""
        pass

    async def _make_request(self, session: ClientSession, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Make a rate-limited API request with retries."""
        try:
            async with self.request_semaphore:
                await self._wait_for_rate_limit(url)
                async with session.get(url, params=params, timeout=ClientTimeout(total=30)) as response:
                    if response.status == 429:
                        logger.warning(f"Rate limit hit for {url}")
                        return None
                    return await response.json()
        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            return None

from typing import List, Dict, Any, Optional
from datetime import datetime
import aiohttp
from .base import BaseDataProvider
from ..config import APIConfig

class FinnhubProvider(BaseDataProvider):
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            rate_limit=APIConfig.FINNHUB["rate_limit"],
            base_delay=APIConfig.FINNHUB["base_delay"]
        )
        self.base_url = APIConfig.FINNHUB["base_url"]

    async def get_market_cap(self, symbol: str, session: aiohttp.ClientSession) -> Optional[float]:
        """Get market cap for a symbol from Finnhub."""
        try:
            params = {
                "symbol": symbol,
                "token": self.api_key
            }
            data = await self.make_request(
                session,
                f"{self.base_url}/stock/metric",
                params,
                "finnhub_metrics"
            )
            
            if data and "metric" in data:
                return float(data["metric"].get("marketCapitalization", 0)) * 1_000_000  # Convert to actual value
            return None
        except Exception as e:
            print(f"Error getting market cap from Finnhub for {symbol}: {str(e)}")
            return None

    async def get_stock_data(self, symbol: str, start_date: str, end_date: str, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        """Get historical stock data from Finnhub."""
        try:
            start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
            end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
            
            params = {
                "symbol": symbol,
                "resolution": "D",  # Daily candles
                "from": start_timestamp,
                "to": end_timestamp,
                "token": self.api_key
            }
            
            data = await self.make_request(
                session,
                f"{self.base_url}/stock/candle",
                params,
                "finnhub_candles"
            )
            
            if data and data.get("s") == "ok":
                result = []
                for i in range(len(data["t"])):
                    date = datetime.fromtimestamp(data["t"][i]).strftime("%Y-%m-%d")
                    result.append({
                        "date": date,
                        "symbol": symbol,
                        "price": float(data["c"][i]),  # Close price
                        "volume": int(data["v"][i]),
                        "market_cap": 0  # Will be filled later
                    })
                return result
            return []
        except Exception as e:
            print(f"Error getting stock data from Finnhub for {symbol}: {str(e)}")
            return []

    async def get_company_info(self, symbol: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """Get company information from Finnhub."""
        try:
            params = {
                "symbol": symbol,
                "token": self.api_key
            }
            data = await self.make_request(
                session,
                f"{self.base_url}/stock/profile2",
                params,
                "finnhub_profile"
            )
            
            if data:
                return {
                    "symbol": symbol,
                    "name": data.get("name", ""),
                    "industry": data.get("finnhubIndustry", ""),
                    "market_cap": float(data.get("marketCapitalization", 0)) * 1_000_000
                }
            return {}
        except Exception as e:
            print(f"Error getting company info from Finnhub for {symbol}: {str(e)}")
            return {}

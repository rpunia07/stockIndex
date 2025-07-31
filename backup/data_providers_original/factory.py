from typing import List, Dict, Any, Optional
import asyncio
from aiohttp import ClientSession
from .base import MarketDataProvider
from .alpha_vantage import AlphaVantageProvider
from .config import APIConfig, create_alpha_vantage_config, create_yahoo_finance_config

class DataProviderFactory:
    def __init__(self, alpha_vantage_key: str = None, finnhub_key: str = None):
        self.providers: List[BaseDataProvider] = []
        
        if alpha_vantage_key:
            self.providers.append(AlphaVantageProvider(alpha_vantage_key))
        if finnhub_key:
            self.providers.append(FinnhubProvider(finnhub_key))
        # Yahoo Finance doesn't need an API key
        self.providers.append(YahooFinanceProvider())

    async def get_market_cap(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Try getting market cap from all providers until successful."""
        for provider in self.providers:
            try:
                market_cap = await provider.get_market_cap(symbol, session)
                if market_cap and market_cap > 0:
                    return market_cap
            except Exception as e:
                print(f"Error with provider {provider.__class__.__name__}: {str(e)}")
                continue
        return None

    async def get_stock_data(self, symbol: str, start_date: str, end_date: str, session: ClientSession) -> List[Dict[str, Any]]:
        """Try getting stock data from all providers until successful."""
        for provider in self.providers:
            try:
                data = await provider.get_stock_data(symbol, start_date, end_date, session)
                if data:
                    # If we got data but no market cap, try to get market cap
                    if all(item.get('market_cap', 0) == 0 for item in data):
                        market_cap = await self.get_market_cap(symbol, session)
                        if market_cap:
                            for item in data:
                                item['market_cap'] = market_cap
                    return data
            except Exception as e:
                print(f"Error with provider {provider.__class__.__name__}: {str(e)}")
                continue
        return []

    async def get_company_info(self, symbol: str, session: ClientSession) -> Dict[str, Any]:
        """Try getting company info from all providers until successful."""
        for provider in self.providers:
            try:
                info = await provider.get_company_info(symbol, session)
                if info and info.get('market_cap', 0) > 0:
                    return info
            except Exception as e:
                print(f"Error with provider {provider.__class__.__name__}: {str(e)}")
                continue
        return {}

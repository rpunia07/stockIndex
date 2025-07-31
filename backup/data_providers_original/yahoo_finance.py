from typing import List, Dict, Any, Optional
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup
import json
from .base import BaseDataProvider
from ..config import APIConfig

class YahooFinanceProvider(BaseDataProvider):
    def __init__(self):
        super().__init__(
            api_key="",  # Yahoo Finance doesn't require an API key
            rate_limit=APIConfig.YAHOO_FINANCE["rate_limit"],
            base_delay=APIConfig.YAHOO_FINANCE["base_delay"]
        )
        self.base_url = APIConfig.YAHOO_FINANCE["base_url"]
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    async def get_market_cap(self, symbol: str, session: aiohttp.ClientSession) -> Optional[float]:
        """Get market cap for a symbol from Yahoo Finance."""
        try:
            async with session.get(f"https://finance.yahoo.com/quote/{symbol}", 
                                 headers=self.headers,
                                 timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    return None
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Try to find market cap in the page
                market_cap_td = soup.find('td', {'data-test': 'MARKET_CAP-value'})
                if market_cap_td:
                    mc_text = market_cap_td.text.strip()
                    # Convert text like "1.23T" to numeric
                    value = float(''.join(filter(str.isdigit, mc_text[:-1])))
                    unit = mc_text[-1].upper()
                    multiplier = {'B': 1e9, 'M': 1e6, 'T': 1e12}.get(unit, 1)
                    return value * multiplier
                
                # Backup: look in the JSON-LD data
                script = soup.find('script', {'type': 'application/ld+json'})
                if script:
                    try:
                        data = json.loads(script.string)
                        market_cap = float(data.get('marketCap', 0))
                        if market_cap > 0:
                            return market_cap
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass
                
                return None
        except Exception as e:
            print(f"Error getting market cap from Yahoo Finance for {symbol}: {str(e)}")
            return None

    async def get_stock_data(self, symbol: str, start_date: str, end_date: str, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        """Get historical stock data from Yahoo Finance."""
        try:
            start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
            end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
            
            params = {
                "symbol": symbol,
                "period1": start_timestamp,
                "period2": end_timestamp,
                "interval": "1d",
                "includeAdjustedClose": True
            }
            
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            data = await self.make_request(session, url, params, "yahoo_chart")
            
            if data and "chart" in data and "result" in data["chart"]:
                result = data["chart"]["result"][0]
                timestamps = result["timestamp"]
                quotes = result["indicators"]["quote"][0]
                
                stock_data = []
                for i in range(len(timestamps)):
                    if (quotes["close"][i] is not None and 
                        quotes["volume"][i] is not None):
                        date = datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%d")
                        stock_data.append({
                            "date": date,
                            "symbol": symbol,
                            "price": float(quotes["close"][i]),
                            "volume": int(quotes["volume"][i]),
                            "market_cap": 0  # Will be filled later
                        })
                return stock_data
            return []
        except Exception as e:
            print(f"Error getting stock data from Yahoo Finance for {symbol}: {str(e)}")
            return []

    async def get_company_info(self, symbol: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """Get company information from Yahoo Finance."""
        try:
            async with session.get(f"https://finance.yahoo.com/quote/{symbol}/profile",
                                 headers=self.headers,
                                 timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    return {}
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                name = ""
                industry = ""
                
                # Try to get company name
                name_h1 = soup.find('h1', {'class': 'D(ib)'})
                if name_h1:
                    name = name_h1.text.strip()
                
                # Try to get industry
                spans = soup.find_all('span', {'class': 'Fw(600)'})
                for span in spans:
                    if 'Industry' in span.text:
                        industry = span.find_next_sibling('span').text.strip()
                        break
                
                market_cap = await self.get_market_cap(symbol, session)
                
                return {
                    "symbol": symbol,
                    "name": name,
                    "industry": industry,
                    "market_cap": market_cap or 0
                }
        except Exception as e:
            print(f"Error getting company info from Yahoo Finance for {symbol}: {str(e)}")
            return {}

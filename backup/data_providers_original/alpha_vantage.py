from typing import Optional, Dict, Any, List
from datetime import datetime
import logging
import json
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from .base import MarketDataProvider

logger = logging.getLogger(__name__)

class AlphaVantageProvider(MarketDataProvider):
    async def _get_market_cap_from_yahoo(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Fallback method to get market cap by scraping Yahoo Finance."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            url = f"https://finance.yahoo.com/quote/{symbol}"
            
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch Yahoo Finance data for {symbol}: Status {response.status}")
                    return None
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Try to find market cap in the page
                market_cap_td = soup.find('td', {'data-test': 'MARKET_CAP-value'})
                if market_cap_td:
                    mc_text = market_cap_td.text.strip()
                    # Convert text like "1.23T" to numeric
                    value = float(''.join(filter(str.isdigit, mc_text[:-1])))
                    if '.' in mc_text[:-1]:  # Handle decimal points
                        value = float(mc_text[:-1].replace('T', '').replace('B', '').replace('M', ''))
                    unit = mc_text[-1].upper()
                    multiplier = {
                        'T': 1e12,  # Trillion
                        'B': 1e9,   # Billion
                        'M': 1e6    # Million
                    }.get(unit, 1)
                    market_cap = value * multiplier
                    logger.info(f"Fetched market cap for {symbol} from Yahoo Finance: ${market_cap:,.2f}")
                    return market_cap
                
                # Backup method: look in the JSON-LD data
                script = soup.find('script', {'type': 'application/ld+json'})
                if script:
                    try:
                        data = json.loads(script.string)
                        market_cap = float(data.get('marketCap', 0))
                        if market_cap > 0:
                            logger.info(f"Fetched market cap for {symbol} from Yahoo Finance metadata: ${market_cap:,.2f}")
                            return market_cap
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass
                
                return None
                
        except Exception as e:
            logger.error(f"Error fetching from Yahoo Finance for {symbol}: {str(e)}")
            return None

    async def get_market_cap(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Get market cap from Alpha Vantage OVERVIEW endpoint with Yahoo Finance fallback."""
        # Try Alpha Vantage first
        params = {
            "function": "OVERVIEW",
            "symbol": symbol,
            "apikey": self.config.api_key
        }
        
        data = await self._make_request(session, self.config.base_url, params)
        if data:
            try:
                market_cap = float(data.get("MarketCapitalization", 0))
                if market_cap > 0:
                    logger.info(f"Fetched market cap for {symbol} from Alpha Vantage: ${market_cap:,.2f}")
                    return market_cap
            except (ValueError, TypeError):
                pass

        # If Alpha Vantage fails or returns 0, try Yahoo Finance
        logger.info(f"Alpha Vantage market cap fetch failed for {symbol}, trying Yahoo Finance...")
        yahoo_market_cap = await self._get_market_cap_from_yahoo(symbol, session)
        if yahoo_market_cap:
            return yahoo_market_cap

        logger.warning(f"Failed to get market cap for {symbol} from all sources")
        return None

    async def get_daily_data(self, symbol: str, start_date: datetime, end_date: datetime, 
                            session: ClientSession) -> List[Dict[str, Any]]:
        """Get daily price and volume data from Alpha Vantage."""
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "apikey": self.config.api_key,
            "outputsize": "full"
        }
        
        data = await self._make_request(session, self.config.base_url, params)
        if not data or "Time Series (Daily)" not in data:
            return []

        result = []
        daily_data = data["Time Series (Daily)"]
        
        for date_str, values in daily_data.items():
            date = datetime.strptime(date_str, "%Y-%m-%d")
            if start_date <= date <= end_date:
                try:
                    result.append({
                        "date": date_str,
                        "symbol": symbol,
                        "price": float(values["4. close"]),
                        "volume": int(values["6. volume"]),
                        "source": "alpha_vantage",
                        "is_estimated": False
                    })
                except (ValueError, KeyError) as e:
                    logger.error(f"Error processing data for {symbol} on {date_str}: {e}")
                    continue

        return result

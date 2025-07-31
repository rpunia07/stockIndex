import pandas as pd
from datetime import datetime, timedelta
import asyncio
import json
import os
from typing import List, Dict, Any, Optional, Tuple
import logging
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, alpha_vantage_api_key: str = None):
        """Initialize data fetcher with Alpha Vantage API key."""
        self.alpha_vantage_api_key = alpha_vantage_api_key or os.getenv('ALPHA_VANTAGE_KEY')
        if not self.alpha_vantage_api_key:
            raise ValueError("Alpha Vantage API key is required")
            
        # Cache settings
        self.cache_dir = 'data'
        self.universe_cache_file = f'{self.cache_dir}/universe_cache.json'
        self.market_cap_cache_file = f'{self.cache_dir}/market_cap_cache.json'
        self.cache_duration = timedelta(days=7)
        
        # Rate limiting
        self.rate_limit_delay = 12  # seconds between requests
        self.max_retries = 3
        
        # Ensure cache directory exists
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Load cached data
        self.universe_cache = self._load_cache(self.universe_cache_file)
        self.market_cap_cache = self._load_cache(self.market_cap_cache_file)

    def _load_cache(self, cache_file: str) -> Dict:
        """Load cached data from file."""
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    cache = json.load(f)
                    if cache.get('timestamp'):
                        cache_time = datetime.fromisoformat(cache['timestamp'])
                        if datetime.now() - cache_time < self.cache_duration:
                            return cache.get('data', {})
            return {}
        except Exception as e:
            logger.error(f"Error loading cache {cache_file}: {str(e)}")
            return {}

    def _save_cache(self, cache_file: str, data: Dict):
        """Save data to cache file."""
        try:
            cache = {
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            with open(cache_file, 'w') as f:
                json.dump(cache, f)
        except Exception as e:
            logger.error(f"Error saving cache {cache_file}: {str(e)}")

    async def _get_market_cap_yahoo(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Get market cap from Yahoo Finance as fallback."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            url = f"https://finance.yahoo.com/quote/{symbol}"
            
            async with session.get(url, headers=headers, timeout=ClientTimeout(total=30)) as response:
                if response.status != 200:
                    return None
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Find market cap element
                market_cap_td = soup.find('td', {'data-test': 'MARKET_CAP-value'})
                if market_cap_td:
                    mc_text = market_cap_td.text.strip()
                    # Convert text like "4.1T" to numeric value
                    if mc_text[-1].upper() in ['T', 'B', 'M']:
                        value_str = mc_text[:-1]
                        if '.' in value_str:
                            value = float(value_str)
                        else:
                            value = float(''.join(filter(str.isdigit, value_str)))
                        
                        unit = mc_text[-1].upper()
                        multipliers = {'T': 1e12, 'B': 1e9, 'M': 1e6}
                        market_cap = value * multipliers.get(unit, 1)
                        
                        logger.info(f"Yahoo Finance market cap for {symbol}: ${market_cap:,.0f}")
                        return market_cap
                        
        except Exception as e:
            logger.error(f"Error fetching Yahoo Finance data for {symbol}: {str(e)}")
        return None

    async def _get_market_cap_alpha_vantage(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Get market cap from Alpha Vantage."""
        try:
            params = {
                "function": "OVERVIEW",
                "symbol": symbol,
                "apikey": self.alpha_vantage_api_key
            }
            
            async with session.get("https://www.alphavantage.co/query", params=params) as response:
                data = await response.json()
                
                # Check for rate limit
                if "Note" in data and "call frequency" in data["Note"].lower():
                    logger.warning(f"Alpha Vantage rate limit hit for {symbol}")
                    return None
                    
                market_cap = float(data.get("MarketCapitalization", 0))
                if market_cap > 0:
                    logger.info(f"Alpha Vantage market cap for {symbol}: ${market_cap:,.0f}")
                    return market_cap
                    
        except Exception as e:
            logger.error(f"Error fetching Alpha Vantage market cap for {symbol}: {str(e)}")
        return None

    async def get_market_cap(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Get market cap with fallback logic: Alpha Vantage -> Yahoo Finance."""
        # Check cache first
        if symbol in self.market_cap_cache:
            cache_entry = self.market_cap_cache[symbol]
            cache_time = datetime.fromisoformat(cache_entry['timestamp'])
            if datetime.now() - cache_time < timedelta(hours=24):
                return cache_entry['market_cap']
        
        # Try Alpha Vantage first
        market_cap = await self._get_market_cap_alpha_vantage(symbol, session)
        
        # If Alpha Vantage fails, try Yahoo Finance
        if not market_cap:
            await asyncio.sleep(2)  # Brief delay before scraping
            market_cap = await self._get_market_cap_yahoo(symbol, session)
        
        # Cache result if successful
        if market_cap:
            self.market_cap_cache[symbol] = {
                'market_cap': market_cap,
                'timestamp': datetime.now().isoformat()
            }
            self._save_cache(self.market_cap_cache_file, self.market_cap_cache)
        
        return market_cap

    async def get_daily_data(self, symbol: str, start_date: str, end_date: str, session: ClientSession) -> List[Dict[str, Any]]:
        """Get daily stock data from Alpha Vantage."""
        try:
            await asyncio.sleep(self.rate_limit_delay)  # Rate limiting
            
            params = {
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": symbol,
                "apikey": self.alpha_vantage_api_key,
                "outputsize": "full"
            }
            
            async with session.get("https://www.alphavantage.co/query", params=params) as response:
                data = await response.json()
                
                # Check for errors
                if "Error Message" in data:
                    logger.error(f"Alpha Vantage error for {symbol}: {data['Error Message']}")
                    return []
                    
                if "Note" in data:
                    logger.warning(f"Alpha Vantage rate limit for {symbol}: {data['Note']}")
                    return []
                    
                if "Time Series (Daily)" not in data:
                    logger.warning(f"No daily data for {symbol}")
                    return []
                
                # Parse data
                daily_data = data["Time Series (Daily)"]
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
                
                result = []
                for date_str, values in daily_data.items():
                    date = datetime.strptime(date_str, "%Y-%m-%d")
                    if start <= date <= end:
                        try:
                            result.append({
                                "date": date_str,
                                "symbol": symbol,
                                "price": float(values["4. close"]),
                                "volume": int(values["6. volume"]),
                                "market_cap": 0  # Will be filled later
                            })
                        except (ValueError, KeyError) as e:
                            logger.error(f"Error parsing data for {symbol} on {date_str}: {e}")
                            continue
                
                return result
                
        except Exception as e:
            logger.error(f"Error fetching daily data for {symbol}: {str(e)}")
            return []

    async def fetch_symbols(self) -> List[str]:
        """Fetch top 100 US companies by market cap."""
        # Check cache first
        if self.universe_cache:
            cached_symbols = self.universe_cache.get('symbols', [])
            if cached_symbols:
                logger.info(f"Using cached universe of {len(cached_symbols)} symbols")
                return cached_symbols[:100]
        
        try:
            # Fetch S&P 500 list from Wikipedia
            logger.info("Fetching S&P 500 companies from Wikipedia...")
            sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            tables = pd.read_html(sp500_url)
            symbols = tables[0]['Symbol'].tolist()
            
            # For simplicity, just return first 100 symbols
            # In production, you'd sort by market cap
            top_100 = symbols[:100]
            
            # Cache the result
            self.universe_cache = {
                'symbols': top_100,
                'last_updated': datetime.now().isoformat()
            }
            self._save_cache(self.universe_cache_file, self.universe_cache)
            
            logger.info(f"Fetched {len(top_100)} symbols")
            return top_100
            
        except Exception as e:
            logger.error(f"Error fetching symbols: {str(e)}")
            # Fallback to hardcoded list
            return [
                'AAPL', 'MSFT', 'GOOG', 'AMZN', 'NVDA', 
                'META', 'BRK-B', 'LLY', 'TSLA', 'V',
                'UNH', 'JPM', 'XOM', 'JNJ', 'MA',
                'PG', 'HD', 'AVGO', 'MRK', 'CVX'
            ]

    async def fetch_all_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch all required data for the stock index."""
        logger.info(f"Starting data fetch for period {start_date} to {end_date}")
        
        symbols = await self.fetch_symbols()
        all_data = []
        
        timeout = ClientTimeout(total=60)
        async with ClientSession(timeout=timeout) as session:
            for i, symbol in enumerate(symbols):
                logger.info(f"Processing {symbol} ({i+1}/{len(symbols)})")
                
                try:
                    # Get stock data
                    stock_data = await self.get_daily_data(symbol, start_date, end_date, session)
                    
                    if stock_data:
                        # Get market cap
                        market_cap = await self.get_market_cap(symbol, session)
                        
                        # Add market cap to each data point
                        for data_point in stock_data:
                            data_point['market_cap'] = market_cap or 0
                        
                        all_data.extend(stock_data)
                        logger.info(f"Successfully processed {symbol}: {len(stock_data)} data points")
                    else:
                        logger.warning(f"No data for {symbol}")
                        
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {str(e)}")
                    continue
        
        logger.info(f"Data collection complete: {len(all_data)} total data points")
        return all_data

    @staticmethod
    def merge_data_sources(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and merge data from multiple sources."""
        if not data:
            return []
            
        df = pd.DataFrame(data)
        
        # Ensure required columns exist
        required_columns = ['date', 'symbol', 'price', 'market_cap', 'volume']
        for col in required_columns:
            if col not in df.columns:
                df[col] = 0
        
        # Convert types
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        # Remove duplicates and fill NaN values
        df = df.drop_duplicates(subset=['date', 'symbol'])
        df = df.fillna(0)
        
        return df.to_dict('records')

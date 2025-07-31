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

    async def _get_daily_data_fmp(self, symbol: str, start_date: str, end_date: str, session: ClientSession) -> List[Dict[str, Any]]:
        """Get daily stock data from Financial Modeling Prep (free tier) as third fallback."""
        try:
            # FMP Free API - no key required for basic historical data
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
            params = {
                'from': start_date,
                'to': end_date
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            async with session.get(url, headers=headers, params=params, timeout=ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.warning(f"FMP API HTTP {response.status} for {symbol}")
                    return []
                    
                data = await response.json()
                
                if 'historical' not in data:
                    logger.warning(f"No historical data from FMP for {symbol}")
                    return []
                
                historical_data = data['historical']
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
                
                result = []
                for entry in historical_data:
                    try:
                        date_str = entry['date']
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                        
                        if start <= date_obj <= end:
                            result.append({
                                "date": date_str,
                                "symbol": symbol,
                                "price": float(entry['close']),
                                "volume": int(entry.get('volume', 0)),
                                "market_cap": 0
                            })
                    except (ValueError, KeyError) as e:
                        logger.debug(f"Skipping invalid FMP data for {symbol}: {e}")
                        continue
                
                if result:
                    logger.info(f"FMP fetched {len(result)} data points for {symbol}")
                    return result
                else:
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching FMP data for {symbol}: {str(e)}")
        return []

    async def _get_market_cap_yahoo(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Get market cap from Yahoo Finance as fallback with enhanced anti-detection."""
        try:
            # Multiple header configurations
            header_configs = [
                {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Cache-Control': 'max-age=0'
                },
                {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                }
            ]
            
            # Multiple Yahoo URLs to try
            urls = [
                f"https://finance.yahoo.com/quote/{symbol}",
                f"https://finance.yahoo.com/quote/{symbol}/key-statistics",
                f"https://uk.finance.yahoo.com/quote/{symbol}"
            ]
            
            for header_idx, headers in enumerate(header_configs):
                for url_idx, url in enumerate(urls):
                    try:
                        # Progressive delay
                        await asyncio.sleep(2 + header_idx + url_idx)
                        
                        async with session.get(url, headers=headers, timeout=ClientTimeout(total=45)) as response:
                            if response.status == 401:
                                logger.warning(f"Yahoo Finance 401 for market cap of {symbol} - config {header_idx+1}, URL {url_idx+1}")
                                continue
                            elif response.status == 403:
                                logger.warning(f"Yahoo Finance 403 for market cap of {symbol} - config {header_idx+1}, URL {url_idx+1}")
                                continue
                            elif response.status == 429:
                                logger.warning(f"Yahoo Finance rate limited for market cap of {symbol} - waiting...")
                                await asyncio.sleep(15)
                                continue
                            elif response.status != 200:
                                logger.warning(f"Yahoo Finance HTTP {response.status} for market cap of {symbol}")
                                continue
                                
                            html = await response.text()
                            
                            # Check if we got blocked (common blocking patterns)
                            if 'blocked' in html.lower() or 'captcha' in html.lower() or len(html) < 1000:
                                logger.warning(f"Potential blocking detected for {symbol} - trying next config")
                                continue
                            
                            soup = BeautifulSoup(html, 'html.parser')
                            
                            # Multiple selectors to try for market cap
                            selectors = [
                                'td[data-test="MARKET_CAP-value"]',
                                'span[data-test="MARKET_CAP-value"]',
                                'td[data-symbol="MARKET_CAP"]',
                                '[data-field="marketCap"]'
                            ]
                            
                            for selector in selectors:
                                try:
                                    element = soup.select_one(selector)
                                    if element:
                                        mc_text = element.text.strip()
                                        if mc_text.upper() not in ['N/A', '--', 'NULL', ''] and len(mc_text) > 0:
                                            # Convert text like "4.1T" to numeric value
                                            if mc_text[-1].upper() in ['T', 'B', 'M', 'K']:
                                                try:
                                                    value_str = mc_text[:-1].replace(',', '')
                                                    value = float(value_str)
                                                    unit = mc_text[-1].upper()
                                                    multipliers = {'T': 1e12, 'B': 1e9, 'M': 1e6, 'K': 1e3}
                                                    market_cap = value * multipliers.get(unit, 1)
                                                    
                                                    logger.info(f"Yahoo Finance market cap for {symbol}: ${market_cap:,.0f} (config {header_idx+1}, URL {url_idx+1})")
                                                    return market_cap
                                                except ValueError:
                                                    continue
                                except Exception:
                                    continue
                                    
                    except Exception as e:
                        logger.warning(f"Yahoo Finance request failed for {symbol} market cap (config {header_idx+1}, URL {url_idx+1}): {str(e)}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error fetching Yahoo Finance market cap for {symbol}: {str(e)}")
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

    async def _get_daily_data_yahoo(self, symbol: str, start_date: str, end_date: str, session: ClientSession) -> List[Dict[str, Any]]:
        """Get daily stock data from Yahoo Finance as fallback with enhanced anti-detection."""
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
            start_timestamp = int(start.timestamp())
            end_timestamp = int(end.timestamp())
            
            # Multiple header configurations to try
            header_configs = [
                {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
                    'Accept': 'text/csv,application/csv,text/plain,*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'cross-site',
                    'Referer': 'https://finance.yahoo.com/',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                },
                {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json,text/plain,*/*',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Origin': 'https://finance.yahoo.com',
                    'Referer': 'https://finance.yahoo.com/',
                    'Connection': 'keep-alive'
                },
                {
                    'User-Agent': 'python-requests/2.31.0',
                    'Accept': 'text/csv,*/*',
                    'Accept-Encoding': 'gzip, deflate'
                }
            ]
            
            # Multiple endpoints to try
            endpoints = [
                f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}",
                f"https://query2.finance.yahoo.com/v7/finance/download/{symbol}",
                f"https://finance.yahoo.com/quote/{symbol}/history"
            ]
            
            params = {
                'period1': start_timestamp,
                'period2': end_timestamp,
                'interval': '1d',
                'events': 'history',
                'includeAdjustedClose': 'true'
            }
            
            for header_idx, headers in enumerate(header_configs):
                for endpoint_idx, url in enumerate(endpoints):
                    try:
                        # Progressive delay
                        await asyncio.sleep(1 + (header_idx * 2) + endpoint_idx)
                        
                        async with session.get(url, headers=headers, params=params, timeout=ClientTimeout(total=45)) as response:
                            if response.status == 401:
                                logger.warning(f"Yahoo Finance 401 for {symbol} - header {header_idx+1}, endpoint {endpoint_idx+1}")
                                continue
                            elif response.status == 429:
                                logger.warning(f"Yahoo Finance rate limited for {symbol} - waiting...")
                                await asyncio.sleep(10)
                                continue
                            elif response.status == 403:
                                logger.warning(f"Yahoo Finance forbidden for {symbol} - trying next config")
                                continue
                            elif response.status != 200:
                                logger.warning(f"Yahoo Finance HTTP {response.status} for {symbol}")
                                continue
                                
                            csv_data = await response.text()
                            
                            # Check if we got HTML instead of CSV (blocked request)
                            if csv_data.startswith('<!DOCTYPE html') or '<html' in csv_data:
                                logger.warning(f"Got HTML instead of CSV for {symbol} - blocked request")
                                continue
                            
                            lines = csv_data.strip().split('\n')
                            
                            if len(lines) < 2:
                                logger.warning(f"Insufficient data lines for {symbol}")
                                continue
                                
                            headers_line = lines[0].split(',')
                            if 'Date' not in headers_line or 'Close' not in headers_line:
                                logger.warning(f"Invalid CSV headers for {symbol}")
                                continue
                            
                            result = []
                            for line in lines[1:]:
                                try:
                                    values = line.split(',')
                                    if len(values) < 6:
                                        continue
                                        
                                    date_str = values[0]
                                    if not date_str or date_str == 'null':
                                        continue
                                        
                                    close_price = float(values[4])
                                    volume = int(float(values[6])) if len(values) > 6 and values[6] not in ['null', ''] else 0
                                    
                                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                                    if start <= date_obj <= end:
                                        result.append({
                                            "date": date_str,
                                            "symbol": symbol,
                                            "price": close_price,
                                            "volume": volume,
                                            "market_cap": 0
                                        })
                                        
                                except (ValueError, IndexError) as e:
                                    logger.debug(f"Skipping invalid line for {symbol}: {e}")
                                    continue
                            
                            if result:
                                logger.info(f"Yahoo Finance success for {symbol}: {len(result)} data points (config {header_idx+1}, endpoint {endpoint_idx+1})")
                                return result
                            else:
                                logger.warning(f"No valid data points extracted for {symbol}")
                                
                    except Exception as e:
                        logger.warning(f"Yahoo Finance request failed for {symbol} (config {header_idx+1}, endpoint {endpoint_idx+1}): {str(e)}")
                        continue
            
            logger.warning(f"All Yahoo Finance attempts failed for {symbol}")
            return []
                
        except Exception as e:
            logger.error(f"Error in Yahoo Finance fallback for {symbol}: {str(e)}")
        return []

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
            await asyncio.sleep(2)
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
        """Get daily stock data with multi-tier fallback logic: Alpha Vantage -> Yahoo Finance -> FMP."""
        try:
            await asyncio.sleep(self.rate_limit_delay)
            
            params = {
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": symbol,
                "apikey": self.alpha_vantage_api_key,
                "outputsize": "full"
            }
            
            async with session.get("https://www.alphavantage.co/query", params=params) as response:
                data = await response.json()
                
                # Check for successful Alpha Vantage response
                if "Time Series (Daily)" in data:
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
                                    "market_cap": 0
                                })
                            except (ValueError, KeyError):
                                continue
                    
                    if result:
                        logger.info(f"Alpha Vantage fetched {len(result)} data points for {symbol}")
                        return result
                
                # Check for rate limit or errors
                if "Note" in data and "call frequency" in data["Note"].lower():
                    logger.warning(f"Alpha Vantage rate limit hit for {symbol}")
                elif "Error Message" in data:
                    logger.warning(f"Alpha Vantage error for {symbol}: {data['Error Message']}")
                else:
                    logger.warning(f"No Alpha Vantage data for {symbol}")
                
                # Try Yahoo Finance fallback
                logger.info(f"Trying Yahoo Finance fallback for {symbol}")
                await asyncio.sleep(2)
                yahoo_result = await self._get_daily_data_yahoo(symbol, start_date, end_date, session)
                
                if yahoo_result:
                    logger.info(f"Yahoo Finance fallback successful for {symbol}")
                    return yahoo_result
                
                # Try FMP as third fallback
                logger.info(f"Trying FMP fallback for {symbol}")
                await asyncio.sleep(2)
                fmp_result = await self._get_daily_data_fmp(symbol, start_date, end_date, session)
                
                if fmp_result:
                    logger.info(f"FMP fallback successful for {symbol}")
                    return fmp_result
                else:
                    logger.warning(f"No data available from any source for {symbol}")
                    return []
                
        except Exception as e:
            logger.error(f"Error fetching daily data for {symbol}: {str(e)}")
            # Try all fallbacks as last resort
            try:
                logger.info(f"Trying all fallbacks for {symbol}")
                
                # Try Yahoo Finance
                yahoo_result = await self._get_daily_data_yahoo(symbol, start_date, end_date, session)
                if yahoo_result:
                    return yahoo_result
                
                # Try FMP
                await asyncio.sleep(2)
                fmp_result = await self._get_daily_data_fmp(symbol, start_date, end_date, session)
                if fmp_result:
                    return fmp_result
                    
            except Exception:
                pass
                
            logger.error(f"All data sources failed for {symbol}")
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

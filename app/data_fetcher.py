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
        
        # Rate limiting and batch settings
        self.rate_limit_delay = 12  # seconds between requests for Alpha Vantage
        self.yahoo_batch_delay = 2  # seconds between Yahoo Finance requests (can be faster)
        self.max_retries = 3
        self.batch_size = 10  # Process stocks in batches
        self.candidate_symbols = 200  # Pool of companies to evaluate for market cap
        self.max_symbols = 100  # Final number of top companies by market cap
        
        # Ensure cache directory exists
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Load cached data
        self.universe_cache = self._load_cache(self.universe_cache_file)
        self.market_cap_cache = self._load_cache(self.market_cap_cache_file)

    def configure_settings(self, max_symbols: int = None, candidate_symbols: int = None, batch_size: int = None, rate_limit_delay: int = None):
        """Configure data fetcher settings."""
        if max_symbols is not None:
            self.max_symbols = max_symbols
            logger.info(f"Updated max_symbols to {max_symbols}")
        
        if candidate_symbols is not None:
            self.candidate_symbols = candidate_symbols
            logger.info(f"Updated candidate_symbols to {candidate_symbols}")
        
        if batch_size is not None:
            self.batch_size = batch_size
            logger.info(f"Updated batch_size to {batch_size}")
            
        if rate_limit_delay is not None:
            self.rate_limit_delay = rate_limit_delay
            logger.info(f"Updated rate_limit_delay to {rate_limit_delay}s")
    
    def get_current_settings(self) -> Dict[str, Any]:
        """Get current configuration settings."""
        return {
            'candidate_symbols': self.candidate_symbols,
            'max_symbols': self.max_symbols,
            'batch_size': self.batch_size,
            'rate_limit_delay': self.rate_limit_delay,
            'yahoo_batch_delay': self.yahoo_batch_delay,
            'cache_duration_days': self.cache_duration.days
        }

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

    async def _get_daily_data_polygon(self, symbol: str, start_date: str, end_date: str, session: ClientSession) -> List[Dict[str, Any]]:
        """Get daily stock data from Polygon.io free tier as third fallback."""
        try:
            # Polygon.io free tier allows limited requests without API key
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            params = {
                'adjusted': 'true',
                'sort': 'asc'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
                'Accept': 'application/json',
                'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Microsoft Edge";v="122"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
            }
            
            async with session.get(url, headers=headers, params=params, timeout=ClientTimeout(total=30)) as response:
                if response.status == 429:
                    logger.warning(f"Polygon.io rate limited for {symbol}")
                    await asyncio.sleep(10)
                    return []
                elif response.status != 200:
                    logger.warning(f"Polygon.io HTTP {response.status} for {symbol}")
                    return []
                    
                data = await response.json()
                
                if 'results' not in data or not data['results']:
                    logger.warning(f"No results from Polygon.io for {symbol}")
                    return []
                
                results = data['results']
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
                
                result = []
                for entry in results:
                    try:
                        # Polygon returns timestamp in milliseconds
                        timestamp_ms = entry['t']
                        date_obj = datetime.fromtimestamp(timestamp_ms / 1000)
                        date_str = date_obj.strftime("%Y-%m-%d")
                        
                        if start <= date_obj <= end:
                            result.append({
                                "date": date_str,
                                "symbol": symbol,
                                "price": float(entry['c']),  # close price
                                "volume": int(entry.get('v', 0)),  # volume
                                "market_cap": 0
                            })
                    except (ValueError, KeyError) as e:
                        logger.debug(f"Skipping invalid Polygon data for {symbol}: {e}")
                        continue
                
                if result:
                    logger.info(f"Polygon.io fetched {len(result)} data points for {symbol}")
                    return result
                else:
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching Polygon.io data for {symbol}: {str(e)}")
        return []

    async def _get_daily_data_iex(self, symbol: str, start_date: str, end_date: str, session: ClientSession) -> List[Dict[str, Any]]:
        """Get daily stock data from IEX Cloud sandbox (free) as fourth fallback."""
        try:
            # IEX Cloud sandbox is free but has limited data
            url = f"https://sandbox.iexapis.com/stable/stock/{symbol}/chart/1y"
            params = {
                'token': 'Tpk_059b97af715d417d9f49f50b51b1c448',  # Sandbox public token
                'chartByDay': 'true'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
                'Accept': 'application/json',
                'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Microsoft Edge";v="122"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
            }
            
            async with session.get(url, headers=headers, params=params, timeout=ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.warning(f"IEX Cloud HTTP {response.status} for {symbol}")
                    return []
                    
                data = await response.json()
                
                if not data:
                    logger.warning(f"No data from IEX Cloud for {symbol}")
                    return []
                
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
                
                result = []
                for entry in data:
                    try:
                        date_str = entry['date']
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                        
                        if start <= date_obj <= end and entry.get('close'):
                            result.append({
                                "date": date_str,
                                "symbol": symbol,
                                "price": float(entry['close']),
                                "volume": int(entry.get('volume', 0)),
                                "market_cap": 0
                            })
                    except (ValueError, KeyError) as e:
                        logger.debug(f"Skipping invalid IEX data for {symbol}: {e}")
                        continue
                
                if result:
                    logger.info(f"IEX Cloud fetched {len(result)} data points for {symbol}")
                    return result
                else:
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching IEX Cloud data for {symbol}: {str(e)}")
        return []

    async def _get_daily_data_fmp_alternative(self, symbol: str, start_date: str, end_date: str, session: ClientSession) -> List[Dict[str, Any]]:
        """Get daily stock data from alternative FMP endpoints as fifth fallback."""
        try:
            # Try different FMP endpoints that might still be free
            endpoints = [
                f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?serietype=line",
                f"https://financialmodelingprep.com/api/v4/historical-price/{symbol}",
                f"https://fmpcloud.io/api/v3/historical-price-full/{symbol}"
            ]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
                'Accept': 'application/json',
                'Referer': 'https://financialmodelingprep.com/',
                'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Microsoft Edge";v="122"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
            }
            
            for endpoint in endpoints:
                try:
                    await asyncio.sleep(1)
                    async with session.get(endpoint, headers=headers, timeout=ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            # Handle different response formats
                            historical_data = None
                            if 'historical' in data:
                                historical_data = data['historical']
                            elif isinstance(data, list):
                                historical_data = data
                            
                            if historical_data:
                                start = datetime.strptime(start_date, "%Y-%m-%d")
                                end = datetime.strptime(end_date, "%Y-%m-%d")
                                
                                result = []
                                for entry in historical_data:
                                    try:
                                        date_str = entry.get('date') or entry.get('Date')
                                        close_price = entry.get('close') or entry.get('Close')
                                        volume = entry.get('volume') or entry.get('Volume', 0)
                                        
                                        if date_str and close_price:
                                            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                                            
                                            if start <= date_obj <= end:
                                                result.append({
                                                    "date": date_str,
                                                    "symbol": symbol,
                                                    "price": float(close_price),
                                                    "volume": int(volume),
                                                    "market_cap": 0
                                                })
                                    except (ValueError, KeyError) as e:
                                        logger.debug(f"Skipping invalid FMP alt data for {symbol}: {e}")
                                        continue
                                
                                if result:
                                    logger.info(f"FMP alternative fetched {len(result)} data points for {symbol}")
                                    return result
                        else:
                            logger.debug(f"FMP alternative endpoint failed: {response.status}")
                            
                except Exception as e:
                    logger.debug(f"FMP alternative endpoint error: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error fetching FMP alternative data for {symbol}: {str(e)}")
        return []

    async def _get_yahoo_crumb(self, session: ClientSession) -> Optional[str]:
        """Get Yahoo Finance crumb token needed for API calls."""
        try:
            # Try different URLs to get a crumb
            crumb_methods = [
                {
                    "name": "direct_crumb_endpoint",
                    "url": "https://query1.finance.yahoo.com/v1/test/getcrumb",
                    "method": "direct"
                },
                {
                    "name": "main_page_scrape",
                    "url": "https://finance.yahoo.com/",
                    "method": "html_parse"
                },
                {
                    "name": "quote_page_scrape", 
                    "url": "https://finance.yahoo.com/quote/AAPL",
                    "method": "html_parse"
                },
                {
                    "name": "alternative_crumb_endpoint",
                    "url": "https://query2.finance.yahoo.com/v1/test/getcrumb",
                    "method": "direct"
                }
            ]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none'
            }
            
            for method_info in crumb_methods:
                try:
                    await asyncio.sleep(1)  # Small delay between attempts
                    logger.info(f"Trying crumb method: {method_info['name']}")
                    
                    async with session.get(method_info["url"], headers=headers, timeout=ClientTimeout(total=30)) as response:
                        logger.info(f"Crumb method {method_info['name']} response: {response.status}")
                        
                        if response.status == 200:
                            if method_info["method"] == "direct":
                                # Direct crumb endpoint
                                crumb = await response.text()
                                crumb = crumb.strip()
                                
                                # Validate crumb format
                                if crumb and len(crumb) > 5 and crumb != 'null' and not crumb.startswith('<'):
                                    logger.info(f"Got Yahoo Finance crumb from {method_info['name']}: {crumb[:10]}...")
                                    return crumb
                                else:
                                    logger.warning(f"Invalid crumb from {method_info['name']}: {crumb[:20] if crumb else 'empty'}")
                                    
                            elif method_info["method"] == "html_parse":
                                # Parse HTML for crumb
                                html = await response.text()
                                logger.info(f"HTML length from {method_info['name']}: {len(html)}")
                                
                                # Enhanced crumb patterns
                                import re
                                crumb_patterns = [
                                    r'"crumb":"([^"]+)"',
                                    r'"CrumbStore"\s*:\s*\{\s*"crumb"\s*:\s*"([^"]+)"',
                                    r'{"crumb":"([^"]+)"}',
                                    r'"crumb":\s*"([^"]+)"',
                                    r'crumb["\']?\s*[:=]\s*["\']([^"\']+)["\']',
                                    r'window\.___CRUMB___\s*=\s*["\']([^"\']+)["\']',
                                    r'root\.App\.main\s*=.*?"crumb":"([^"]+)"',
                                    r'YAHOO\.context\s*=.*?"crumb":"([^"]+)"'
                                ]
                                
                                for i, pattern in enumerate(crumb_patterns):
                                    try:
                                        matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)
                                        if matches:
                                            for match in matches:
                                                crumb = match.strip()
                                                # Validate crumb format
                                                if len(crumb) > 5 and len(crumb) < 50 and not any(char in crumb for char in ['<', '>', '{', '}', '[', ']']):
                                                    logger.info(f"Found Yahoo Finance crumb with pattern {i+1} in {method_info['name']}: {crumb[:10]}...")
                                                    return crumb
                                    except Exception as pattern_error:
                                        logger.debug(f"Pattern {i+1} failed: {pattern_error}")
                                        continue
                                
                                logger.warning(f"No valid crumb found in HTML from {method_info['name']}")
                        
                        elif response.status == 403:
                            logger.warning(f"Access forbidden for {method_info['name']} - might be blocked")
                        elif response.status == 429:
                            logger.warning(f"Rate limited for {method_info['name']} - waiting...")
                            await asyncio.sleep(10)
                        else:
                            logger.warning(f"HTTP {response.status} for {method_info['name']}")
                            
                except Exception as e:
                    logger.debug(f"Failed to get crumb from {method_info['name']}: {e}")
                    continue
            
            logger.warning("Could not obtain Yahoo Finance crumb from any method")
            return None
            
        except Exception as e:
            logger.error(f"Error getting Yahoo Finance crumb: {e}")
            return None

    async def _get_market_cap_yahoo(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Get market cap from Yahoo Finance using multiple approaches."""
        try:
            # First try to get a crumb for authenticated API access
            crumb = await self._get_yahoo_crumb(session)
            
            # Try different Yahoo Finance approaches - start with crumb-free methods
            api_approaches = [
                # Approach 1: Chart API without crumb (often works)
                {
                    "name": "chart_API_no_crumb",
                    "url": f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                    "params": {"interval": "1d", "range": "1d"},
                    "type": "json",
                    "requires_crumb": False
                },
                # Approach 2: Search API without crumb
                {
                    "name": "search_API_no_crumb",
                    "url": f"https://query1.finance.yahoo.com/v1/finance/search",
                    "params": {"q": symbol, "quotesCount": 1, "newsCount": 0},
                    "type": "json",
                    "requires_crumb": False
                },
                # Approach 3: quoteSummary API with summaryDetail (with crumb if available)
                {
                    "name": "quoteSummary_summaryDetail",
                    "url": f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
                    "params": {"modules": "summaryDetail", "crumb": crumb} if crumb else {"modules": "summaryDetail"},
                    "type": "json",
                    "requires_crumb": True
                },
                # Approach 4: quoteSummary API with price module
                {
                    "name": "quoteSummary_price",
                    "url": f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
                    "params": {"modules": "price", "crumb": crumb} if crumb else {"modules": "price"},
                    "type": "json",
                    "requires_crumb": True
                },
                # Approach 5: Alternative query server
                {
                    "name": "quoteSummary_API_v2", 
                    "url": f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
                    "params": {"modules": "summaryDetail,price,defaultKeyStatistics", "crumb": crumb} if crumb else {"modules": "summaryDetail,price,defaultKeyStatistics"},
                    "type": "json",
                    "requires_crumb": True
                },
                # Approach 6: Quote page scraping (fallback)
                {
                    "name": "quote_page_scrape",
                    "url": f"https://finance.yahoo.com/quote/{symbol}",
                    "params": {},
                    "type": "html",
                    "requires_crumb": False
                }
            ]
            
            # If no crumb, skip crumb-required methods
            if not crumb:
                logger.warning(f"No crumb available for {symbol}, using crumb-free methods only")
                api_approaches = [approach for approach in api_approaches if not approach.get("requires_crumb", False)]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://finance.yahoo.com/',
                'Origin': 'https://finance.yahoo.com'
            }
            
            for approach_idx, approach in enumerate(api_approaches):
                try:
                    await asyncio.sleep(1 + approach_idx)
                    
                    logger.info(f"Trying Yahoo Finance market cap approach: {approach['name']} for {symbol}")
                    
                    async with session.get(approach["url"], headers=headers, params=approach["params"], timeout=ClientTimeout(total=30)) as response:
                        logger.info(f"Yahoo Finance {approach['name']} response status: {response.status} for {symbol}")
                        
                        if response.status == 401:
                            logger.warning(f"Yahoo Finance {approach['name']} 401 for {symbol}")
                            continue
                        elif response.status == 403:
                            logger.warning(f"Yahoo Finance {approach['name']} 403 for {symbol}")
                            continue
                        elif response.status == 429:
                            logger.warning(f"Yahoo Finance {approach['name']} rate limited for {symbol} - waiting...")
                            await asyncio.sleep(15)
                            continue
                        elif response.status == 404:
                            logger.warning(f"Yahoo Finance {approach['name']} 404 for {symbol}")
                            continue
                        elif response.status != 200:
                            logger.warning(f"Yahoo Finance {approach['name']} HTTP {response.status} for {symbol}")
                            continue
                        
                        try:
                            data = await response.json()
                            logger.info(f"Yahoo Finance {approach['name']} JSON response received for {symbol}")
                            
                            # Handle quoteSummary response
                            if 'quoteSummary' in data:
                                result = data['quoteSummary'].get('result', [])
                                if result and len(result) > 0:
                                    # Try different locations for market cap
                                    for module_name in ['summaryDetail', 'price', 'defaultKeyStatistics', 'financialData']:
                                        if module_name in result[0]:
                                            module_data = result[0][module_name]
                                            logger.info(f"Checking {module_name} module for market cap in {symbol}")
                                            
                                            # Try different field names for market cap
                                            for field_name in ['marketCap', 'marketCapitalization', 'sharesOutstanding']:
                                                if field_name in module_data:
                                                    mc_data = module_data[field_name]
                                                    logger.info(f"Found {field_name} in {module_name}: {mc_data} for {symbol}")
                                                    
                                                    if isinstance(mc_data, dict) and 'raw' in mc_data:
                                                        market_cap = float(mc_data['raw'])
                                                        logger.info(f"Yahoo Finance {approach['name']} market cap for {symbol}: ${market_cap:,.0f} from {module_name}.{field_name}")
                                                        return market_cap
                                                    elif isinstance(mc_data, (int, float)):
                                                        market_cap = float(mc_data)
                                                        # If it's shares outstanding, multiply by current price
                                                        if field_name == 'sharesOutstanding' and 'regularMarketPrice' in module_data:
                                                            price_data = module_data['regularMarketPrice']
                                                            if isinstance(price_data, dict) and 'raw' in price_data:
                                                                price = float(price_data['raw'])
                                                                market_cap = market_cap * price
                                                                logger.info(f"Calculated market cap from shares outstanding: {market_cap} for {symbol}")
                                                        logger.info(f"Yahoo Finance {approach['name']} market cap for {symbol}: ${market_cap:,.0f} from {module_name}.{field_name}")
                                                        return market_cap
                            
                            # Handle chart response
                            elif 'chart' in data:
                                chart_result = data['chart'].get('result', [])
                                if chart_result and len(chart_result) > 0:
                                    meta = chart_result[0].get('meta', {})
                                    logger.info(f"Chart meta keys: {list(meta.keys())} for {symbol}")
                                    
                                    # Check for market cap in meta
                                    for field_name in ['marketCap', 'marketCapitalization', 'sharesOutstanding']:
                                        if field_name in meta:
                                            logger.info(f"Found {field_name} in chart meta: {meta[field_name]} for {symbol}")
                                            if isinstance(meta[field_name], (int, float)):
                                                market_cap = float(meta[field_name])
                                                # If it's shares outstanding, we'd need price to calculate market cap
                                                if field_name == 'sharesOutstanding' and 'regularMarketPrice' in meta:
                                                    market_cap = market_cap * float(meta['regularMarketPrice'])
                                                    logger.info(f"Calculated market cap from chart shares outstanding: {market_cap} for {symbol}")
                                                logger.info(f"Yahoo Finance {approach['name']} market cap for {symbol}: ${market_cap:,.0f} from chart meta")
                                                return market_cap
                            
                            # Handle search response
                            elif 'quotes' in data:
                                quotes = data.get('quotes', [])
                                if quotes and len(quotes) > 0:
                                    quote = quotes[0]
                                    logger.info(f"Search quote keys: {list(quote.keys())} for {symbol}")
                                    for field_name in ['marketCap', 'marketCapitalization']:
                                        if field_name in quote:
                                            market_cap = float(quote[field_name])
                                            logger.info(f"Yahoo Finance {approach['name']} market cap for {symbol}: ${market_cap:,.0f} from search")
                                            return market_cap
                            
                            logger.info(f"No market cap found in {approach['name']} JSON response for {symbol}")
                            
                        except Exception as json_error:
                            logger.warning(f"Failed to parse JSON from Yahoo Finance {approach['name']} for {symbol}: {json_error}")
                            
                            # If JSON parsing failed, try HTML scraping as final fallback
                            if approach['url'] and 'quote' in approach['url']:
                                try:
                                    import re
                                    logger.info(f"Attempting HTML scraping fallback for {symbol}")
                                    
                                    # Try to extract market cap from HTML content
                                    html_content = await response.text()
                                    
                                    # Multiple regex patterns to find market cap
                                    patterns = [
                                        r'"marketCap":\s*{\s*"raw":\s*([0-9.]+)',
                                        r'"marketCap":\s*([0-9.]+)',
                                        r'Market Cap[^>]*>([0-9.]+[BT]?)',
                                        r'marketCap.*?([0-9]+\.?[0-9]*[BT]?)',
                                        r'Market\s*Cap.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?[BT]?)',
                                        r'data-reactid.*?Market Cap.*?(\d+\.?\d*[BT]?)',
                                        r'marketCapitalization.*?([0-9.]+)',
                                    ]
                                    
                                    for pattern in patterns:
                                        match = re.search(pattern, html_content, re.IGNORECASE)
                                        if match:
                                            value_str = match.group(1)
                                            logger.info(f"Found market cap pattern '{pattern}' with value '{value_str}' for {symbol}")
                                            
                                            try:
                                                # Handle different formats
                                                if 'T' in value_str.upper():
                                                    # Trillion
                                                    market_cap = float(value_str.upper().replace('T', '').replace(',', '')) * 1e12
                                                elif 'B' in value_str.upper():
                                                    # Billion
                                                    market_cap = float(value_str.upper().replace('B', '').replace(',', '')) * 1e9
                                                elif ',' in value_str:
                                                    # Raw number with commas
                                                    market_cap = float(value_str.replace(',', ''))
                                                else:
                                                    # Raw number
                                                    market_cap = float(value_str)
                                                
                                                # Validate reasonable market cap (> $1M and < $50T)
                                                if 1_000_000 <= market_cap <= 50_000_000_000_000:
                                                    logger.info(f"Yahoo Finance HTML scraping market cap for {symbol}: ${market_cap:,.0f}")
                                                    return market_cap
                                                else:
                                                    logger.warning(f"Unrealistic market cap from HTML scraping: {market_cap} for {symbol}")
                                                    
                                            except ValueError as ve:
                                                logger.warning(f"Could not parse market cap value '{value_str}' for {symbol}: {ve}")
                                                continue
                                    
                                    logger.info(f"No market cap found in HTML content for {symbol}")
                                    
                                except Exception as html_error:
                                    logger.warning(f"HTML scraping failed for {symbol}: {html_error}")
                            
                            continue
                        
                except Exception as e:
                    logger.error(f"Yahoo Finance {approach['name']} request failed for {symbol}: {str(e)}")
                    continue
            
            logger.warning(f"All Yahoo Finance API approaches failed for market cap of {symbol}")
            return None
                        
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
        """Get daily stock data from Yahoo Finance using API endpoints."""
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
            start_timestamp = int(start.timestamp())
            end_timestamp = int(end.timestamp())
            
            # Use Yahoo Finance API endpoints that should work
            endpoints = [
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
                f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}",
                f"https://query2.finance.yahoo.com/v7/finance/download/{symbol}"
            ]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
                'Accept': 'application/json, text/csv, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
            
            for endpoint_idx, url in enumerate(endpoints):
                try:
                    await asyncio.sleep(1 + endpoint_idx)
                    
                    logger.info(f"Trying Yahoo Finance stock data: endpoint {endpoint_idx+1} for {symbol}")
                    
                    # For chart API endpoints (v8), use different params
                    if '/chart/' in url:
                        params = {
                            'period1': start_timestamp,
                            'period2': end_timestamp,
                            'interval': '1d',
                            'includePrePost': 'false',
                            'events': 'div%2Csplit'
                        }
                    else:
                        # For download endpoints (v7)
                        params = {
                            'period1': start_timestamp,
                            'period2': end_timestamp,
                            'interval': '1d',
                            'events': 'history',
                            'includeAdjustedClose': 'true'
                        }
                    
                    async with session.get(url, headers=headers, params=params, timeout=ClientTimeout(total=45)) as response:
                        logger.info(f"Yahoo Finance stock data response status: {response.status} for {symbol}")
                        
                        if response.status == 401:
                            logger.warning(f"Yahoo Finance 401 for {symbol} - endpoint {endpoint_idx+1}")
                            continue
                        elif response.status == 429:
                            logger.warning(f"Yahoo Finance rate limited for {symbol} - waiting...")
                            await asyncio.sleep(20)
                            continue
                        elif response.status == 403:
                            logger.warning(f"Yahoo Finance forbidden for {symbol} - endpoint {endpoint_idx+1}")
                            continue
                        elif response.status == 404:
                            logger.warning(f"Yahoo Finance 404 for {symbol} - endpoint {endpoint_idx+1}")
                            continue
                        elif response.status != 200:
                            logger.warning(f"Yahoo Finance HTTP {response.status} for {symbol}")
                            continue
                        
                        content_type = response.headers.get('content-type', '').lower()
                        
                        # Handle JSON response (chart API)
                        if 'application/json' in content_type or '/chart/' in url:
                            try:
                                data = await response.json()
                                logger.info(f"Yahoo Finance JSON response received for {symbol}")
                                
                                if 'chart' in data and data['chart']['result']:
                                    chart_data = data['chart']['result'][0]
                                    timestamps = chart_data.get('timestamp', [])
                                    indicators = chart_data.get('indicators', {})
                                    
                                    if 'quote' in indicators and indicators['quote']:
                                        quote_data = indicators['quote'][0]
                                        closes = quote_data.get('close', [])
                                        volumes = quote_data.get('volume', [])
                                        
                                        result = []
                                        for i, timestamp in enumerate(timestamps):
                                            if i < len(closes) and closes[i] is not None:
                                                date_obj = datetime.fromtimestamp(timestamp)
                                                if start <= date_obj <= end:
                                                    result.append({
                                                        "date": date_obj.strftime("%Y-%m-%d"),
                                                        "symbol": symbol,
                                                        "price": float(closes[i]),
                                                        "volume": int(volumes[i]) if i < len(volumes) and volumes[i] is not None else 0,
                                                        "market_cap": 0
                                                    })
                                        
                                        if result:
                                            logger.info(f"Yahoo Finance chart API success for {symbol}: {len(result)} data points")
                                            return result
                                        
                            except Exception as json_error:
                                logger.warning(f"Failed to parse JSON from Yahoo Finance for {symbol}: {json_error}")
                                continue
                        
                        # Handle CSV response (download API)
                        else:
                            csv_data = await response.text()
                            logger.info(f"Yahoo Finance CSV data length: {len(csv_data)} for {symbol}")
                            
                            # Check if we got HTML instead of CSV (blocked request)
                            if csv_data.startswith('<!DOCTYPE html') or '<html' in csv_data:
                                logger.warning(f"Got HTML instead of CSV for {symbol} - blocked request")
                                continue
                            
                            lines = csv_data.strip().split('\n')
                            logger.info(f"Yahoo Finance CSV lines: {len(lines)} for {symbol}")
                            
                            if len(lines) < 2:
                                logger.warning(f"Insufficient data lines for {symbol}")
                                continue
                                
                            headers_line = lines[0].split(',')
                            if 'Date' not in headers_line or 'Close' not in headers_line:
                                logger.warning(f"Invalid CSV headers for {symbol}: {headers_line}")
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
                                logger.info(f"Yahoo Finance CSV success for {symbol}: {len(result)} data points")
                                return result
                            else:
                                logger.warning(f"No valid data points extracted from CSV for {symbol}")
                                
                except Exception as e:
                    logger.error(f"Yahoo Finance request failed for {symbol} (endpoint {endpoint_idx+1}): {str(e)}")
                    continue
            
            logger.warning(f"All Yahoo Finance API attempts failed for {symbol}")
            return []
                
        except Exception as e:
            logger.error(f"Error in Yahoo Finance API fallback for {symbol}: {str(e)}")
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
            # Only apply Alpha Vantage rate limiting if we actually use Alpha Vantage
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
                    logger.warning(f"Alpha Vantage rate limit hit for {symbol}, switching to Yahoo Finance")
                elif "Error Message" in data:
                    logger.warning(f"Alpha Vantage error for {symbol}: {data['Error Message']}, switching to Yahoo Finance")
                else:
                    logger.warning(f"No Alpha Vantage data for {symbol}, switching to Yahoo Finance")
                
                # Try Yahoo Finance fallback (no additional delay since it's faster)
                logger.info(f"Trying Yahoo Finance fallback for {symbol}")
                yahoo_result = await self._get_daily_data_yahoo(symbol, start_date, end_date, session)
                
                if yahoo_result:
                    logger.info(f"Yahoo Finance fallback successful for {symbol}")
                    return yahoo_result
                
                # Try Polygon.io as third fallback
                logger.info(f"Trying Polygon.io fallback for {symbol}")
                await asyncio.sleep(3)
                polygon_result = await self._get_daily_data_polygon(symbol, start_date, end_date, session)
                
                if polygon_result:
                    logger.info(f"Polygon.io fallback successful for {symbol}")
                    return polygon_result
                
                # Try IEX Cloud as fourth fallback
                logger.info(f"Trying IEX Cloud fallback for {symbol}")
                await asyncio.sleep(2)
                iex_result = await self._get_daily_data_iex(symbol, start_date, end_date, session)
                
                if iex_result:
                    logger.info(f"IEX Cloud fallback successful for {symbol}")
                    return iex_result
                
                # Try FMP alternative endpoints as fifth fallback
                logger.info(f"Trying FMP alternative fallback for {symbol}")
                await asyncio.sleep(2)
                fmp_alt_result = await self._get_daily_data_fmp_alternative(symbol, start_date, end_date, session)
                
                if fmp_alt_result:
                    logger.info(f"FMP alternative fallback successful for {symbol}")
                    return fmp_alt_result
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
                
                # Try Polygon.io
                await asyncio.sleep(2)
                polygon_result = await self._get_daily_data_polygon(symbol, start_date, end_date, session)
                if polygon_result:
                    return polygon_result
                
                # Try IEX Cloud
                await asyncio.sleep(2)
                iex_result = await self._get_daily_data_iex(symbol, start_date, end_date, session)
                if iex_result:
                    return iex_result
                
                # Try FMP alternative
                await asyncio.sleep(2)
                fmp_alt_result = await self._get_daily_data_fmp_alternative(symbol, start_date, end_date, session)
                if fmp_alt_result:
                    return fmp_alt_result
                    
            except Exception:
                pass
                
            logger.error(f"All data sources failed for {symbol}")
            return []

    async def fetch_symbols(self) -> List[str]:
        """Fetch top companies by actual market cap using a two-stage process:
        1. Get candidate_symbols (200) from S&P 500
        2. Fetch real market cap data for candidates
        3. Return top max_symbols (100) by actual market cap
        """
        # Check cache first
        if self.universe_cache:
            cached_symbols = self.universe_cache.get('symbols', [])
            cache_metadata = self.universe_cache.get('metadata', {})
            if cached_symbols and len(cached_symbols) >= self.max_symbols:
                logger.info(f"Using cached universe of {len(cached_symbols)} symbols (selected from {cache_metadata.get('candidates_evaluated', 'unknown')} candidates)")
                return cached_symbols[:self.max_symbols]
        
        logger.info(f"Starting two-stage symbol selection: {self.candidate_symbols} candidates → top {self.max_symbols} by market cap")
        
        # Stage 1: Get candidate symbols from S&P 500
        candidate_symbols = await self._get_candidate_symbols()
        if not candidate_symbols:
            logger.error("Failed to get candidate symbols, using fallback")
            return self._get_fallback_symbols()
        
        logger.info(f"Stage 1 complete: Got {len(candidate_symbols)} candidate symbols")
        
        # Stage 2: Fetch market cap data for candidates and select top companies
        top_symbols = await self._select_top_symbols_by_market_cap(candidate_symbols)
        
        if not top_symbols:
            logger.error("Failed to get market cap data, using candidate symbols as fallback")
            return candidate_symbols[:self.max_symbols]
        
        # Cache the result with metadata
        self.universe_cache = {
            'symbols': top_symbols,
            'last_updated': datetime.now().isoformat(),
            'metadata': {
                'candidates_evaluated': len(candidate_symbols),
                'final_selection': len(top_symbols),
                'selection_method': 'market_cap_based',
                'candidate_pool_size': self.candidate_symbols,
                'final_pool_size': self.max_symbols
            }
        }
        self._save_cache(self.universe_cache_file, self.universe_cache)
        
        logger.info(f"Two-stage selection complete: Selected top {len(top_symbols)} companies by market cap from {len(candidate_symbols)} candidates")
        return top_symbols
    
    async def _get_candidate_symbols(self) -> List[str]:
        """Stage 1: Get ALL S&P 500 companies and evaluate real market caps to select top candidates."""
        try:
            logger.info(f"Fetching ALL S&P 500 companies to evaluate real market caps...")
            sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            tables = pd.read_html(sp500_url)
            
            # The first table contains the S&P 500 companies
            df = tables[0]
            all_symbols = df['Symbol'].tolist()
            
            logger.info(f"Found {len(all_symbols)} S&P 500 companies. Evaluating ALL {len(all_symbols)} companies by real market cap...")
            
            # Evaluate ALL S&P 500 companies by real market cap
            market_cap_data = []
            failed_symbols = []
            
            # Process all symbols in smaller batches to get market cap data
            batch_size = 10  # Moderate batch size for market cap evaluation
            timeout = ClientTimeout(total=120)
            
            async with ClientSession(timeout=timeout) as session:
                for batch_start in range(0, len(all_symbols), batch_size):
                    batch_end = min(batch_start + batch_size, len(all_symbols))
                    batch_symbols = all_symbols[batch_start:batch_end]
                    
                    logger.info(f"Evaluating market cap for S&P 500 batch {batch_start//batch_size + 1}/{(len(all_symbols) + batch_size - 1)//batch_size}: {batch_symbols}")
                    
                    # Get market cap for each symbol in the batch
                    batch_tasks = []
                    for symbol in batch_symbols:
                        task = self._get_symbol_market_cap(symbol, session)
                        batch_tasks.append(task)
                    
                    # Wait for all tasks in the batch to complete
                    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    for symbol, result in zip(batch_symbols, batch_results):
                        if isinstance(result, Exception):
                            logger.warning(f"Error getting market cap for {symbol}: {result}")
                            failed_symbols.append(symbol)
                        elif result is not None:
                            market_cap_data.append({'symbol': symbol, 'market_cap': result})
                        else:
                            failed_symbols.append(symbol)
                    
                    # Rate limiting between batches
                    await asyncio.sleep(self.yahoo_batch_delay)
            
            # Sort by market cap and return ALL evaluated companies (will be trimmed later)
            if market_cap_data:
                market_cap_data.sort(key=lambda x: x['market_cap'], reverse=True)
                # Return ALL successfully evaluated companies, sorted by market cap
                candidate_symbols = [item['symbol'] for item in market_cap_data]
                
                logger.info(f"Successfully evaluated {len(candidate_symbols)} companies by real market cap from {len(all_symbols)} S&P 500 companies")
                top_5_info = [f"{item['symbol']} (${item['market_cap']:,.0f})" for item in market_cap_data[:5]]
                logger.info(f"Top 5 by market cap: {top_5_info}")
                logger.info(f"Failed to get market cap for {len(failed_symbols)} symbols: {failed_symbols[:10]}...")
                
                return candidate_symbols
            else:
                logger.warning("No market cap data obtained, falling back to all symbols in alphabetical order")
                return all_symbols  # Return all symbols if market cap evaluation fails
            
        except Exception as e:
            logger.error(f"Error fetching and evaluating S&P 500 symbols: {str(e)}")
            return self._get_fallback_symbols()
    
    async def _select_top_symbols_by_market_cap(self, candidate_symbols: List[str]) -> List[str]:
        """Stage 2: Select final top companies from already market cap-ranked candidates."""
        logger.info(f"Stage 2: Selecting top {self.max_symbols} from {len(candidate_symbols)} pre-ranked candidates...")
        
        # Candidates are already sorted by market cap from stage 1
        # Just take the top max_symbols
        final_symbols = candidate_symbols[:self.max_symbols]
        
        logger.info(f"Selected final {len(final_symbols)} symbols for the index")
        return final_symbols

    async def _get_symbol_market_cap(self, symbol: str, session: ClientSession) -> Optional[float]:
        """Get market cap for a single symbol with error handling."""
        try:
            # Use Yahoo Finance as it's faster and more reliable for market cap
            market_cap = await self._get_market_cap_yahoo(symbol, session)
            if market_cap and market_cap > 0:
                return market_cap
            
            # Fallback to Alpha Vantage if Yahoo fails
            await asyncio.sleep(1)
            market_cap = await self._get_market_cap_alpha_vantage(symbol, session)
            return market_cap if market_cap and market_cap > 0 else None
            
        except Exception as e:
            logger.debug(f"Error getting market cap for {symbol}: {str(e)}")
            return None
    
    def _get_fallback_symbols(self) -> List[str]:
        """Fallback list of known large companies."""
        fallback_symbols = [
            'AAPL', 'MSFT', 'NVDA', 'GOOG', 'GOOGL', 'AMZN', 'META', 'BRK-B', 
            'LLY', 'AVGO', 'TSLA', 'WMT', 'JPM', 'V', 'UNH', 'XOM', 'ORCL',
            'MA', 'HD', 'PG', 'JNJ', 'NFLX', 'BAC', 'CVX', 'ABBV', 'CRM',
            'COST', 'AMD', 'KO', 'PEP', 'TMO', 'LIN', 'CSCO', 'ACN', 'ABT',
            'DHR', 'VZ', 'MRK', 'CMCSA', 'ADBE', 'WFC', 'NOW', 'TXN', 'NEE',
            'QCOM', 'PM', 'DIS', 'IBM', 'SPGI', 'UBER', 'INTU', 'ISRG', 'CAT',
            'BKNG', 'HON', 'GS', 'AXP', 'T', 'LOW', 'AMGN', 'SYK', 'DE',
            'PANW', 'AMAT', 'PLD', 'GE', 'C', 'MDT', 'ETN', 'VRTX', 'BLK',
            'MDLZ', 'ADI', 'SCHW', 'ADP', 'GILD', 'CB', 'LRCX', 'FI', 'SO',
            'MU', 'KLAC', 'REGN', 'PYPL', 'PGR', 'CI', 'MELI', 'MMC', 'EOG'
        ][:self.max_symbols]
        
        logger.info(f"Using fallback list of {len(fallback_symbols)} large companies")
        return fallback_symbols

    async def fetch_all_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch all required data for the stock index with improved batch processing."""
        logger.info(f"Starting data fetch for period {start_date} to {end_date}")
        
        symbols = await self.fetch_symbols()
        all_data = []
        
        # Process symbols in batches for better performance
        total_symbols = len(symbols)
        logger.info(f"Processing {total_symbols} symbols in batches of {self.batch_size}")
        
        timeout = ClientTimeout(total=60)
        async with ClientSession(timeout=timeout) as session:
            for batch_start in range(0, total_symbols, self.batch_size):
                batch_end = min(batch_start + self.batch_size, total_symbols)
                batch_symbols = symbols[batch_start:batch_end]
                
                logger.info(f"Processing batch {batch_start//self.batch_size + 1}/{(total_symbols + self.batch_size - 1)//self.batch_size}: {batch_symbols}")
                
                # Process batch with concurrent requests for Yahoo Finance
                batch_tasks = []
                for symbol in batch_symbols:
                    batch_tasks.append(self._process_symbol(symbol, start_date, end_date, session))
                
                # Execute batch concurrently
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Collect successful results
                for i, result in enumerate(batch_results):
                    symbol = batch_symbols[i]
                    if isinstance(result, Exception):
                        logger.error(f"Error processing {symbol}: {str(result)}")
                    elif result:
                        all_data.extend(result)
                        logger.info(f"Successfully processed {symbol}: {len(result)} data points")
                    else:
                        logger.warning(f"No data for {symbol}")
                
                # Inter-batch delay to respect rate limits
                if batch_end < total_symbols:
                    logger.info(f"Batch complete. Waiting {self.yahoo_batch_delay}s before next batch...")
                    await asyncio.sleep(self.yahoo_batch_delay)
        
        logger.info(f"Data collection complete: {len(all_data)} total data points from {total_symbols} symbols")
        return all_data
    
    async def _process_symbol(self, symbol: str, start_date: str, end_date: str, session: ClientSession) -> List[Dict[str, Any]]:
        """Process a single symbol with error handling."""
        try:
            # Get stock data (Yahoo Finance is primary, so use shorter delay)
            stock_data = await self.get_daily_data(symbol, start_date, end_date, session)
            
            if stock_data:
                # Get market cap
                market_cap = await self.get_market_cap(symbol, session)
                
                # Add market cap to each data point
                for data_point in stock_data:
                    data_point['market_cap'] = market_cap or 0
                
                return stock_data
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error processing {symbol}: {str(e)}")
            return []

    async def debug_yahoo_finance(self, symbol: str, session: ClientSession) -> Dict[str, Any]:
        """Debug Yahoo Finance to see what's actually happening."""
        debug_results = {
            "symbol": symbol,
            "timestamp": datetime.now().isoformat(),
            "tests": []
        }
        
        # Simple test with minimal headers
        simple_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        url = f"https://finance.yahoo.com/quote/{symbol}"
        
        try:
            logger.info(f"Debug: Testing Yahoo Finance URL: {url}")
            
            async with session.get(url, headers=simple_headers, timeout=ClientTimeout(total=30)) as response:
                test_result = {
                    "url": url,
                    "headers": simple_headers,
                    "status_code": response.status,
                    "response_headers": dict(response.headers),
                    "success": response.status == 200
                }
                
                if response.status == 200:
                    html = await response.text()
                    test_result.update({
                        "html_length": len(html),
                        "html_preview": html[:1000],
                        "contains_market_cap": "market cap" in html.lower() or "Market Cap" in html,
                        "contains_apple": "Apple" in html or "AAPL" in html,
                        "contains_blocking": any(pattern in html.lower() for pattern in ['blocked', 'captcha', 'access denied', 'unauthorized'])
                    })
                else:
                    try:
                        error_content = await response.text()
                        test_result["error_content"] = error_content[:500]
                    except:
                        test_result["error_content"] = "Could not read error content"
                
                debug_results["tests"].append(test_result)
                logger.info(f"Debug result: Status {response.status}, HTML length: {len(html) if response.status == 200 else 'N/A'}")
                
        except Exception as e:
            test_result = {
                "url": url,
                "headers": simple_headers,
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }
            debug_results["tests"].append(test_result)
            logger.error(f"Debug error: {str(e)}")
        
        return debug_results

    async def test_yahoo_finance(self, symbol: str, start_date: str, end_date: str, session: ClientSession, test_type: str = "both") -> Dict[str, Any]:
        """Test Yahoo Finance functionality with detailed analysis and response logging."""
        results = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "test_type": test_type,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            # Test stock data if requested
            if test_type in ["both", "data"]:
                logger.info(f"Testing Yahoo Finance stock data for {symbol}")
                
                try:
                    stock_data = await self._get_daily_data_yahoo(symbol, start_date, end_date, session)
                    results["stock_data"] = {
                        "success": len(stock_data) > 0,
                        "data_points": len(stock_data),
                        "data_preview": stock_data[:5] if stock_data else [],
                        "method": "_get_daily_data_yahoo"
                    }
                    logger.info(f"Yahoo Finance stock data test: {len(stock_data)} data points")
                except Exception as e:
                    results["stock_data"] = {
                        "success": False,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "method": "_get_daily_data_yahoo"
                    }
                    logger.error(f"Yahoo Finance stock data test failed: {str(e)}")
            
            # Test market cap if requested
            if test_type in ["both", "market_cap"]:
                logger.info(f"Testing Yahoo Finance market cap for {symbol}")
                
                try:
                    market_cap = await self._get_market_cap_yahoo(symbol, session)
                    results["market_cap"] = {
                        "success": market_cap is not None,
                        "value": market_cap,
                        "formatted": f"${market_cap:,.0f}" if market_cap else None,
                        "method": "_get_market_cap_yahoo"
                    }
                    logger.info(f"Yahoo Finance market cap test: {market_cap}")
                except Exception as e:
                    results["market_cap"] = {
                        "success": False,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "method": "_get_market_cap_yahoo"
                    }
                    logger.error(f"Yahoo Finance market cap test failed: {str(e)}")
            
            # Overall success summary
            stock_success = results.get("stock_data", {}).get("success", False) if test_type in ["both", "data"] else True
            market_cap_success = results.get("market_cap", {}).get("success", False) if test_type in ["both", "market_cap"] else True
            
            results["test_summary"] = {
                "overall_success": stock_success and market_cap_success,
                "stock_data_success": stock_success if test_type in ["both", "data"] else None,
                "market_cap_success": market_cap_success if test_type in ["both", "market_cap"] else None,
                "uses_existing_logic": True,
                "note": "Uses existing DataFetcher Yahoo Finance methods with Microsoft Edge headers"
            }
            
        except Exception as e:
            results["global_error"] = {
                "error": str(e),
                "error_type": type(e).__name__
            }
            logger.error(f"Yahoo Finance test failed globally for {symbol}: {str(e)}")
        
        return results

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

import pandas as pd
from datetime import datetime, timedelta
import asyncio
import aiohttp
import requests
import json
import os
import re
from typing import List, Dict, Any, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor
import warnings
from aiohttp import ClientSession, ClientTimeout
from contextlib import asynccontextmanager
from bs4 import BeautifulSoup
warnings.filterwarnings('ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None

# Configure pandas to handle financial data better
pd.set_option('display.float_format', lambda x: '%.3f' % x)

class DataFetcher:
    def __init__(self, alpha_vantage_api_key: str = None, finnhub_api_key: str = None):
        """Initialize data fetcher with API keys."""
        # API Configuration
        self.alpha_vantage_api_key = alpha_vantage_api_key or os.getenv('ALPHA_VANTAGE_KEY')
        self.finnhub_api_key = finnhub_api_key or os.getenv('FINNHUB_KEY')
        
        if not self.alpha_vantage_api_key:
            raise ValueError("Alpha Vantage API key is required. Set it in the constructor or ALPHA_VANTAGE_KEY environment variable.")
            
        # Initialize provider factory
        from .data_providers.factory import DataProviderFactory
        self.provider_factory = DataProviderFactory(
            alpha_vantage_key=self.alpha_vantage_api_key,
            finnhub_key=self.finnhub_api_key
        )
            
        # Cache settings
        self.universe_cache_file = 'data/universe_cache.json'
        self.market_cap_cache_file = 'data/market_cap_cache.json'
        self.cache_duration = timedelta(days=7)  # Update universe weekly
        self.batch_size = 1  # Process one symbol at a time for better reliability
        self.delay_between_batches = 20  # Longer delay between batches due to multiple API calls
        self.max_retries = 3  # More retries with longer delays
        self.base_delay = 20  # Longer base delay for exponential backoff
        
        # Request limits - adjusted for multiple API calls per symbol
        self.requests_per_minute = 3  # Very conservative rate limit
        self.last_request_time = {}  # Track last request time per endpoint
        self.request_semaphore = asyncio.Semaphore(1)  # Only one request at a time
        
        # Ensure cache directory exists
        os.makedirs('data', exist_ok=True)
        
        # Load cached data
        self.universe_cache = self._load_cache(self.universe_cache_file)
        self.market_cap_cache = self._load_cache(self.market_cap_cache_file)
        
        # Initialize request headers
        self.headers = {
            'User-Agent': 'Stock Index Management Service',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
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
            print(f"Error loading cache {cache_file}: {str(e)}")
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
            print(f"Error saving cache {cache_file}: {str(e)}")

    async def _wait_for_rate_limit(self, endpoint: str):
        """Implements rate limiting for API requests."""
        now = datetime.now()
        if endpoint in self.last_request_time:
            last_request = self.last_request_time[endpoint]
            time_since_last = (now - last_request).total_seconds()
            if time_since_last < 60 / self.requests_per_minute:
                await asyncio.sleep(60 / self.requests_per_minute - time_since_last)
        self.last_request_time[endpoint] = datetime.now()

    async def _get_market_cap_with_retry(self, symbol: str, session: ClientSession, attempt: int = 0) -> Optional[Tuple[str, float]]:
        """Get market cap for a single symbol with retries and fallback to multiple providers."""
        try:
            # Check cache first
            if symbol in self.market_cap_cache:
                cache_entry = self.market_cap_cache[symbol]
                cache_time = datetime.fromisoformat(cache_entry['timestamp'])
                if datetime.now() - cache_time < timedelta(hours=24):
                    print(f"Using cached market cap for {symbol}")
                    return symbol, cache_entry['market_cap']
            
            # Try to get market cap using provider factory with fallback
            print(f"Fetching market cap for {symbol} using available providers...")
            market_cap = await self.provider_factory.get_market_cap(symbol, session)
            
            if market_cap and market_cap > 0:
                print(f"Successfully fetched market cap for {symbol}: ${market_cap:,.2f}")
                return symbol, market_cap
                
            if attempt < self.max_retries:
                delay = self.base_delay * (2 ** attempt)
                print(f"Failed to get market cap for {symbol}, retrying in {delay} seconds...")
                await asyncio.sleep(delay)
                return await self._get_market_cap_with_retry(symbol, session, attempt + 1)
            
            print(f"Failed to get market cap for {symbol} from all providers")
            return None

            # Next part - make API request
            async with session.get(url, params=params, headers=self.headers, timeout=ClientTimeout(total=30)) as response:
                    data = await response.json()
                    
                    # Check for rate limit or information messages
                    if "Note" in data or "Information" in data:
                        message = data.get("Note", data.get("Information", ""))
                        print(f"API message for {symbol}: {message}")
                        
                        # If it's a rate limit message, retry after delay
                        if "call frequency" in message.lower() or "api call frequency" in message.lower():
                            if attempt < self.max_retries:
                                delay = self.base_delay * (2 ** attempt)
                                print(f"Rate limit hit, waiting {delay} seconds before retry...")
                                await asyncio.sleep(delay)
                                return await self._get_market_cap_with_retry(symbol, session, attempt + 1)
                            print(f"Max retries reached for {symbol}")
                            return None
                        
                    if response.status == 429:
                        if attempt < self.max_retries:
                            delay = self.base_delay * (2 ** attempt)
                            print(f"Rate limit hit, waiting {delay} seconds before retry...")
                            await asyncio.sleep(delay)
                            return await self._get_market_cap_with_retry(symbol, session, attempt + 1)
                        print(f"Max retries reached for {symbol}")
                        return None
                    
                    # Try to get market cap from OVERVIEW
                    market_cap = float(data.get("MarketCapitalization", 0))
                    if market_cap > 0:
                        print(f"Successfully fetched market cap for {symbol} from OVERVIEW: ${market_cap:,.2f}")
                        return symbol, market_cap
                    
                    # Try alternative method using GLOBAL_QUOTE and INCOME_STATEMENT
                    await self._wait_for_rate_limit('alpha_vantage')
                    
                    # First get current price from GLOBAL_QUOTE
                    params = {
                        "function": "GLOBAL_QUOTE",
                        "symbol": symbol,
                        "apikey": self.alpha_vantage_api_key
                    }
                    
                    price = 0
                    async with session.get(url, params=params, headers=self.headers, timeout=ClientTimeout(total=30)) as quote_response:
                        quote_data = await quote_response.json()
                        if "Global Quote" in quote_data:
                            quote = quote_data["Global Quote"]
                            price = float(quote.get("05. price", 0))
                    
                    if price > 0:
                        # Then get income statement to estimate shares from EPS
                        await self._wait_for_rate_limit('alpha_vantage')
                        params = {
                            "function": "INCOME_STATEMENT",
                            "symbol": symbol,
                            "apikey": self.alpha_vantage_api_key
                        }
                        
                        async with session.get(url, params=params, headers=self.headers, timeout=ClientTimeout(total=30)) as income_response:
                            income_data = await income_response.json()
                            if "annualReports" in income_data and income_data["annualReports"]:
                                latest_report = income_data["annualReports"][0]
                                net_income = float(latest_report.get("netIncome", 0))
                                eps = float(latest_report.get("reportedEPS", 0))
                                
                                if eps > 0 and net_income > 0:
                                    # Estimate shares outstanding from net income and EPS
                                    shares_outstanding = abs(net_income / eps)
                                    market_cap = shares_outstanding * price
                                    print(f"Estimated market cap for {symbol} using income data: ${market_cap:,.2f}")
                                    return symbol, market_cap
                    
                    # As a last resort, try to scrape from Yahoo Finance
                    try:
                        yahoo_url = f"https://finance.yahoo.com/quote/{symbol}"
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                        }
                        async with session.get(yahoo_url, headers=headers, timeout=ClientTimeout(total=30)) as yahoo_response:
                            if yahoo_response.status != 200:
                                print(f"Failed to fetch Yahoo Finance data for {symbol}: Status {yahoo_response.status}")
                                return None
                                
                            html = await yahoo_response.text()
                            soup = BeautifulSoup(html, 'html.parser')
                            
                            # Try to find market cap in the page
                            market_cap_td = soup.find('td', {'data-test': 'MARKET_CAP-value'})
                            if market_cap_td:
                                mc_text = market_cap_td.text.strip()
                                # Convert text like "1.23T" to numeric
                                value = float(''.join(filter(str.isdigit, mc_text[:-1])))
                                unit = mc_text[-1].upper()
                                multiplier = {'B': 1e9, 'M': 1e6, 'T': 1e12}.get(unit, 1)
                                market_cap = value * multiplier
                                print(f"Fetched market cap for {symbol} from Yahoo Finance: ${market_cap:,.2f}")
                                return symbol, market_cap
                            
                            # Backup method: look in the JSON-LD data
                            script = soup.find('script', {'type': 'application/ld+json'})
                            if script:
                                try:
                                    data = json.loads(script.string)
                                    market_cap = float(data.get('marketCap', 0))
                                    if market_cap > 0:
                                        print(f"Fetched market cap for {symbol} from Yahoo Finance metadata: ${market_cap:,.2f}")
                                        return symbol, market_cap
                                except (json.JSONDecodeError, ValueError, TypeError):
                                    pass
                    
                    except Exception as e:
                        print(f"Error fetching from Yahoo Finance for {symbol}: {str(e)}")
                    
                    print(f"No market cap data available for {symbol} from any source")
                    return None
                    
        except Exception as e:
            if attempt < self.max_retries and ("429" in str(e) or "Too Many Requests" in str(e)):
                delay = self.base_delay ** attempt
                print(f"Rate limit hit for {symbol}, waiting {delay} seconds...")
                await asyncio.sleep(delay)
                return await self._get_market_cap_with_retry(symbol, session, attempt + 1)
            print(f"Error getting market cap for {symbol}: {str(e)}")
        return None

    async def _get_market_caps(self, symbols: List[str]) -> List[Tuple[str, float]]:
        """Get market caps for a list of symbols using multiple sources."""
        market_caps = []
        total_symbols = len(symbols)
        processed = 0
        
        print(f"\nStarting market cap fetching for {total_symbols} symbols")
        # Create a single aiohttp session for all requests
        timeout = ClientTimeout(total=60, connect=30, sock_connect=30, sock_read=30)
        async with ClientSession(headers=self.headers, timeout=timeout) as session:
            # Process symbols in batches
            total_batches = (len(symbols) + self.batch_size - 1) // self.batch_size
            print(f"Processing {total_batches} batches of {self.batch_size} symbols each...")
            
            for i in range(0, len(symbols), self.batch_size):
                batch_num = (i // self.batch_size) + 1
                batch = symbols[i:i + self.batch_size]
                print(f"\nProcessing batch {batch_num}/{total_batches} [{i+1}-{min(i+self.batch_size, len(symbols))} of {len(symbols)} symbols]")
                batch_results = []
                cached_in_batch = 0
                
                for symbol in batch:
                    # Check cache first
                    if symbol in self.market_cap_cache:
                        cache_entry = self.market_cap_cache[symbol]
                        cache_time = datetime.fromisoformat(cache_entry['timestamp'])
                        if datetime.now() - cache_time < timedelta(hours=24):
                            batch_results.append((symbol, cache_entry['market_cap']))
                            cached_in_batch += 1
                            continue
                    
                    result = await self._get_market_cap_with_retry(symbol, session)
                    if result:
                        symbol, market_cap = result
                        batch_results.append((symbol, market_cap))
                        # Update cache
                        self.market_cap_cache[symbol] = {
                            'market_cap': market_cap,
                            'timestamp': datetime.now().isoformat()
                        }
                        processed += 1
                        if processed % 10 == 0:  # Show progress every 10 symbols
                            print(f"Progress: {processed}/{total_symbols} symbols processed ({(processed/total_symbols*100):.1f}%)")
            
            market_caps.extend(batch_results)
            await asyncio.sleep(self.delay_between_batches)
            print(f"Batch complete. Waiting {self.delay_between_batches}s before next batch...")
        
        # Save updated cache
        self._save_cache(self.market_cap_cache_file, self.market_cap_cache)
        return market_caps

    async def fetch_symbols(self) -> List[str]:
        """Fetch list of top 100 US companies by market cap with caching."""
        try:
            print("Starting to fetch symbols...")
            # Check if we have a valid cached universe
            if self.universe_cache:
                cached_symbols = self.universe_cache.get('symbols', [])
                if cached_symbols:
                    print(f"Using cached universe of {len(cached_symbols)} symbols")
                    return cached_symbols[:100]
            
            print("No valid cache found, fetching fresh symbol data...")
            
            # Fetch fresh data if cache is invalid
            print("Fetching S&P 500 companies list from Wikipedia...")
            sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            tables = pd.read_html(sp500_url)
            sp500_table = tables[0]
            all_symbols = sp500_table['Symbol'].tolist()
            print(f"Found {len(all_symbols)} S&P 500 symbols")
            
            # Get and sort by market cap
            print("\nFetching market caps for all symbols...")
            print("This may take a while due to API rate limiting...")
            market_caps = await self._get_market_caps(all_symbols)
            
            # Log market cap results
            successful_fetches = len([mc for mc in market_caps if mc[1] > 0])
            print(f"\nMarket cap fetch summary:")
            print(f"Total symbols processed: {len(all_symbols)}")
            print(f"Successful market cap fetches: {successful_fetches}")
            print(f"Failed market cap fetches: {len(all_symbols) - successful_fetches}")
            
            # Sort and get top 100
            market_caps.sort(key=lambda x: x[1], reverse=True)
            top_100 = [symbol for symbol, _ in market_caps[:100]]
            
            # Log top companies
            print("\nTop 10 companies by market cap:")
            for i, (symbol, market_cap) in enumerate(market_caps[:10], 1):
                print(f"{i}. {symbol}: ${market_cap:,.2f}")
            
            # Update cache
            self.universe_cache = {
                'symbols': top_100,
                'last_updated': datetime.now().isoformat()
            }
            self._save_cache(self.universe_cache_file, self.universe_cache)
            print(f"\nCache updated with {len(top_100)} symbols")
            
            print(f"Successfully fetched top 100 companies. First few: {top_100[:5]}")
            return top_100
            
        except Exception as e:
            print(f"Error fetching symbols: {str(e)}")
            # Return cached data if available, otherwise use fallback
            if self.universe_cache:
                return self.universe_cache.get('symbols', [])[:100]
            
            return [
                'AAPL', 'MSFT', 'GOOG', 'AMZN', 'NVDA', 
                'META', 'BRK-B', 'LLY', 'TSLA', 'V',
                'UNH', 'JPM', 'XOM', 'JNJ', 'MA',
                'PG', 'HD', 'AVGO', 'MRK', 'CVX'
            ]  # Fallback to top 20

    async def fetch_batch_stock_data(self, symbols: List[str], start_date: str, end_date: str) -> list[dict]:
        """Fetch market data for multiple symbols using multiple providers with fallback."""
        print(f"\nFetching batch data for {len(symbols)} symbols...")
        all_data = []
        market_cap_stats = {
            'success': 0,
            'failed': 0,
            'cached': 0,
            'total_market_cap': 0
        }

        timeout = ClientTimeout(total=30)
        async with ClientSession(headers=self.headers, timeout=timeout) as session:
            for symbol in symbols:
                print(f"Fetching data for {symbol}...")
                data = None
                try:
                    data = await self.provider.get_daily_data(symbol, start_date, end_date, session)
                except Exception as e:
                    print(f"Error fetching data for {symbol}: {str(e)}")
                    market_cap_stats['failed'] += 1
                    continue
                    
                if data:
                    # Process the data we got from the provider
                    all_data.extend(data)
                    market_cap_stats['success'] += len(data)
                    
                    # Update market cap statistics
                    market_cap = next((item['market_cap'] for item in data if item['market_cap'] > 0), 0)
                    if market_cap > 0:
                        market_cap_stats['total_market_cap'] += market_cap
                else:
                    print(f"No data available for {symbol}")
                    market_cap_stats['failed'] += 1
        
        print(f"\nData collection complete:")
        print(f"Successful data points: {market_cap_stats['success']}")
        print(f"Failed data points: {market_cap_stats['failed']}")
        if market_cap_stats['success'] > 0:
            print(f"Average market cap: ${market_cap_stats['total_market_cap']/market_cap_stats['success']:,.2f}")
        
        return all_data




    async def fetch_all_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch data from Alpha Vantage using batch API."""
        print(f"\nStarting batch data fetch for period {start_date} to {end_date}")
        symbols = await self.fetch_symbols()
        print(f"Found {len(symbols)} symbols to process")
        all_data = []
        
        # Initialize statistics
        processed_symbols = 0
        symbols_with_market_cap = 0
        total_market_cap = 0
        data_points_with_market_cap = 0

        # Process symbols in batches of 100 (Alpha Vantage's batch limit)
        print("\nFetching data using Alpha Vantage Batch API")
        for i in range(0, len(symbols), 100):
            batch_symbols = symbols[i:i+100]
            print(f"\nProcessing batch {i//100 + 1} ({len(batch_symbols)} symbols)")
            batch_data = await self.fetch_batch_stock_data(batch_symbols, start_date, end_date)
            
            if batch_data:
                # Analyze batch data
                batch_symbols_with_data = len(set(item['symbol'] for item in batch_data))
                batch_symbols_with_market_cap = len(set(item['symbol'] for item in batch_data if item['market_cap'] > 0))
                batch_market_cap_sum = sum(item['market_cap'] for item in batch_data if item['market_cap'] > 0)
                batch_points_with_market_cap = len([item for item in batch_data if item['market_cap'] > 0])
                
                # Update statistics
                processed_symbols += batch_symbols_with_data
                symbols_with_market_cap += batch_symbols_with_market_cap
                total_market_cap += batch_market_cap_sum
                data_points_with_market_cap += batch_points_with_market_cap
                
                # Print batch statistics
                print(f"\nBatch Statistics:")
                print(f"Symbols with data: {batch_symbols_with_data}/{len(batch_symbols)}")
                print(f"Symbols with market cap: {batch_symbols_with_market_cap}")
                print(f"Data points with market cap: {batch_points_with_market_cap}")
                if batch_symbols_with_market_cap > 0:
                    print(f"Average market cap in batch: ${batch_market_cap_sum/batch_symbols_with_market_cap:,.2f}")
                
                all_data.extend(batch_data)
            
            # Respect rate limits between batches
            if i + 100 < len(symbols):  # If there are more batches to process
                print(f"\nWaiting {self.delay_between_batches}s before next batch...")
                await asyncio.sleep(self.delay_between_batches)

        # Print final statistics
        print(f"\n=== Final Data Collection Summary ===")
        print(f"Total symbols processed: {len(symbols)}")
        print(f"Symbols with data: {processed_symbols}")
        print(f"Symbols with market cap: {symbols_with_market_cap}")
        print(f"Total data points collected: {len(all_data)}")
        print(f"Data points with market cap: {data_points_with_market_cap}")
        if symbols_with_market_cap > 0:
            print(f"Average market cap across all symbols: ${total_market_cap/symbols_with_market_cap:,.2f}")
        print(f"Market cap coverage: {(symbols_with_market_cap/len(symbols)*100):.1f}%")
        print("=====================================")
        
        return all_data

    @staticmethod
    def merge_data_sources(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge and clean data from multiple sources."""
        if not data:
            return []
            
        df = pd.DataFrame([item for sublist in data if sublist for item in sublist])
        if df.empty:
            return []
            
        # Ensure all required columns exist
        required_columns = ['date', 'symbol', 'price', 'market_cap', 'volume']
        for col in required_columns:
            if col not in df.columns:
                df[col] = 0
        
        # Convert types to ensure proper aggregation
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        # Group by date and symbol, taking the mean of numeric columns
        merged = df.groupby(['date', 'symbol']).agg({
            'price': 'mean',
            'market_cap': 'max',  # Take the largest market cap value
            'volume': 'sum'
        }).reset_index()
        
        # Fill any remaining NaN values with 0
        merged = merged.fillna(0)
        
        return merged.to_dict('records')

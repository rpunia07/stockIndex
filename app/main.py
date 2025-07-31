from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
import redis
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import json
import os
import logging
import asyncio
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

logger.info("Environment variables loaded:")
logger.info(f"ALPHA_VANTAGE_KEY present: {bool(os.getenv('ALPHA_VANTAGE_KEY'))}")
logger.info(f"REDIS_HOST: {os.getenv('REDIS_HOST')}")
logger.info(f"REDIS_PORT: {os.getenv('REDIS_PORT')}")

from .database import Database
from .data_fetcher import DataFetcher

app = FastAPI(title="Stock Index Management Service", version="1.0.0")

# Initialize Redis (optional)
try:
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )
    redis_client.ping()
    redis_available = True
    logger.info("Redis connection established")
except:
    logger.warning("Redis not available. Running without caching.")
    redis_available = False

# Initialize database and data fetcher
db = Database()
data_fetcher = DataFetcher()

async def fetch_and_store_data(start_date: str, end_date: str):
    """Background task to fetch and store market data."""
    try:
        logger.info(f"Starting data fetch for {start_date} to {end_date}")
        data = await data_fetcher.fetch_all_data(start_date, end_date)
        
        if data:
            merged_data = data_fetcher.merge_data_sources(data)
            db.insert_market_data(merged_data)
            
            # Calculate and save index performance
            performance = db.calculate_index_performance(start_date, end_date)
            db.save_index_performance(performance)
            
            logger.info(f"Data fetch completed: {len(merged_data)} records")
        else:
            logger.warning("No data fetched")
            
    except Exception as e:
        logger.error(f"Error in background data fetch: {str(e)}")

@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "message": "Stock Index Management Service",
        "status": "healthy",
        "redis_available": redis_available
    }

@app.post("/data/fetch")
async def fetch_data(
    background_tasks: BackgroundTasks,
    start_date: str = None,
    end_date: str = None,
    force_refresh: bool = False
):
    """Fetch market data for the specified date range."""
    # Default to last 30 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    try:
        # Validate dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start > end:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Check cache first (if Redis available and not forcing refresh)
        cache_key = f"fetch_status:{start_date}:{end_date}"
        if redis_available and not force_refresh:
            cached_status = redis_client.get(cache_key)
            if cached_status:
                return JSONResponse({
                    "message": "Data fetch already in progress or completed recently",
                    "start_date": start_date,
                    "end_date": end_date,
                    "cached": True
                })
        
        # Start background task
        background_tasks.add_task(fetch_and_store_data, start_date, end_date)
        
        # Set cache to prevent duplicate requests
        if redis_available:
            redis_client.setex(cache_key, 3600, "in_progress")  # 1 hour expiry
        
        return JSONResponse({
            "message": "Data fetch initiated",
            "start_date": start_date,
            "end_date": end_date,
            "status": "in_progress"
        })
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Error initiating data fetch: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/index/performance")
async def get_index_performance(
    start_date: str = None,
    end_date: str = None,
    force_recalculate: bool = False
):
    """Get index performance data."""
    # Default to last 30 days
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    try:
        # Skip cache if force recalculation is requested
        performance_df = pd.DataFrame()
        
        if not force_recalculate:
            # Check cache first
            cache_key = f"performance:{start_date}:{end_date}"
            if redis_available:
                cached_data = redis_client.get(cache_key)
                if cached_data:
                    return JSONResponse(json.loads(cached_data))

            # Get performance data from database
            try:
                performance_df = db.get_index_performance(start_date, end_date)
            except Exception as db_error:
                logger.error(f"Error getting index performance from database: {str(db_error)}")
                performance_df = pd.DataFrame()

        if performance_df.empty or force_recalculate:
            # Calculate fresh performance data
            try:
                logger.info(f"{'Force recalculating' if force_recalculate else 'No cached'} performance data. Calculating for {start_date} to {end_date}")
                
                # Clear existing performance data for this date range if force recalculate
                if force_recalculate:
                    try:
                        db.conn.execute("DELETE FROM index_performance WHERE date BETWEEN ? AND ?", [start_date, end_date])
                        logger.info(f"Cleared existing performance data for {start_date} to {end_date}")
                    except Exception as clear_error:
                        logger.warning(f"Error clearing existing performance data: {str(clear_error)}")
                
                performance_df = db.calculate_index_performance(start_date, end_date)
                if not performance_df.empty:
                    try:
                        db.save_index_performance(performance_df)
                        logger.info(f"Saved calculated performance data for {start_date} to {end_date}")
                    except Exception as save_error:
                        logger.error(f"Error saving performance data: {str(save_error)}")
            except Exception as calc_error:
                logger.error(f"Error calculating index performance: {str(calc_error)}")
                performance_df = pd.DataFrame()

        if performance_df.empty:
            # Check if we have any market data at all
            try:
                market_data_df = db.get_market_data(start_date, end_date)
                if market_data_df.empty:
                    raise HTTPException(
                        status_code=404, 
                        detail=f"No market data available for the date range {start_date} to {end_date}. Please fetch data first using /data/fetch endpoint."
                    )
                else:
                    raise HTTPException(
                        status_code=500, 
                        detail="Market data exists but performance calculation failed. Check server logs."
                    )
            except HTTPException:
                raise
            except Exception as check_error:
                logger.error(f"Error checking market data availability: {str(check_error)}")
                raise HTTPException(
                    status_code=500, 
                    detail="Unable to determine data availability. Check server logs."
                )
        
        # Convert to JSON-serializable format
        # Handle pandas Timestamp objects that can't be JSON serialized
        if not performance_df.empty:
            # Convert any datetime/timestamp columns to strings
            for col in performance_df.columns:
                if pd.api.types.is_datetime64_any_dtype(performance_df[col]):
                    performance_df[col] = performance_df[col].dt.strftime('%Y-%m-%d')
                elif performance_df[col].dtype == 'object':
                    # Check if it contains Timestamp objects
                    if not performance_df[col].empty and hasattr(performance_df[col].iloc[0], 'strftime'):
                        performance_df[col] = performance_df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
        
        performance_data = {
            "start_date": start_date,
            "end_date": end_date,
            "data": performance_df.to_dict('records') if not performance_df.empty else []
        }
        
        # Cache the result
        if redis_available:
            redis_client.setex(cache_key, 1800, json.dumps(performance_data))  # 30 min cache
        
        return JSONResponse(performance_data)
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Error getting index performance: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/market-data")
async def get_market_data(
    start_date: str = None,
    end_date: str = None,
    symbols: str = None,
    limit: int = 1000
):
    """Get market data for specified criteria."""
    # Default to last 7 days
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    try:
        # Parse symbols if provided
        symbol_list = None
        if symbols:
            symbol_list = [s.strip().upper() for s in symbols.split(',')]
        
        # Get data from database
        try:
            market_df = db.get_market_data(start_date, end_date, symbol_list)
        except Exception as db_error:
            logger.error(f"Database error getting market data: {str(db_error)}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")
        
        if market_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No market data available for the specified criteria. Date range: {start_date} to {end_date}. Use /data/fetch to populate data first."
            )
        
        # Apply limit
        if len(market_df) > limit:
            market_df = market_df.head(limit)
        
        # Convert any datetime/timestamp columns to strings for JSON serialization
        if not market_df.empty:
            for col in market_df.columns:
                if pd.api.types.is_datetime64_any_dtype(market_df[col]):
                    market_df[col] = market_df[col].dt.strftime('%Y-%m-%d')
                elif market_df[col].dtype == 'object':
                    # Check if it contains Timestamp objects
                    if not market_df[col].empty and hasattr(market_df[col].iloc[0], 'strftime'):
                        market_df[col] = market_df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
        
        return JSONResponse({
            "start_date": start_date,
            "end_date": end_date,
            "symbols": symbol_list,
            "count": len(market_df),
            "data": market_df.to_dict('records') if not market_df.empty else []
        })
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Error getting market data: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/companies/top")
async def get_top_companies(
    date: str = None,
    limit: int = 100
):
    """Get top companies by market cap for a specific date."""
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    
    try:
        try:
            companies_df = db.get_top_companies(date, limit)
        except Exception as db_error:
            logger.error(f"Database error getting top companies: {str(db_error)}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")
        
        if companies_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No company data available for {date}. Use /data/fetch to populate data first."
            )
        
        # Convert any datetime/timestamp columns to strings for JSON serialization
        if not companies_df.empty:
            for col in companies_df.columns:
                if pd.api.types.is_datetime64_any_dtype(companies_df[col]):
                    companies_df[col] = companies_df[col].dt.strftime('%Y-%m-%d')
                elif companies_df[col].dtype == 'object':
                    # Check if it contains Timestamp objects
                    if not companies_df[col].empty and hasattr(companies_df[col].iloc[0], 'strftime'):
                        companies_df[col] = companies_df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
        
        return JSONResponse({
            "date": date,
            "count": len(companies_df),
            "companies": companies_df.to_dict('records') if not companies_df.empty else []
        })
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Error getting top companies: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/index/build")
async def build_index(
    background_tasks: BackgroundTasks,
    start_date: str = None,
    end_date: str = None,
    force_rebuild: bool = False
):
    """Build the stock index by fetching and processing market data."""
    # Default to last 30 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    try:
        # Validate dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start > end:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Check if data already exists (unless forcing rebuild)
        if not force_rebuild:
            existing_data = db.get_market_data(start_date, end_date)
            if not existing_data.empty:
                # Calculate and return index performance
                performance = db.calculate_index_performance(start_date, end_date)
                if not performance.empty:
                    db.save_index_performance(performance)
                    return JSONResponse({
                        "message": "Index already built for this period",
                        "start_date": start_date,
                        "end_date": end_date,
                        "data_points": len(existing_data),
                        "performance_calculated": True,
                        "force_rebuild": force_rebuild
                    })
        
        # Start background task to build index
        background_tasks.add_task(fetch_and_store_data, start_date, end_date)
        
        return JSONResponse({
            "message": "Index build initiated",
            "start_date": start_date,
            "end_date": end_date,
            "status": "building",
            "estimated_time": "5-10 minutes depending on data volume"
        })
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Error building index: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/test/symbol")
async def test_symbol_data(
    symbol: str,
    start_date: str = None,
    end_date: str = None
):
    """Test data fetching for a specific symbol with all fallback sources."""
    # Default to last 5 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    
    try:
        # Validate dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start > end:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Validate symbol format
        symbol = symbol.upper().strip()
        if not symbol or len(symbol) > 10:
            raise HTTPException(status_code=400, detail="Invalid symbol format")
        
        logger.info(f"Testing data fetch for symbol: {symbol}")
        
        # Test data fetching with detailed logging
        from aiohttp import ClientSession, ClientTimeout
        
        timeout = ClientTimeout(total=120)  # Longer timeout for testing
        async with ClientSession(timeout=timeout) as session:
            # Test stock data
            stock_data = await data_fetcher.get_daily_data(symbol, start_date, end_date, session)
            
            # Test market cap
            market_cap = await data_fetcher.get_market_cap(symbol, session)
            
            # Prepare detailed response
            response = {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "stock_data": {
                    "success": len(stock_data) > 0,
                    "data_points": len(stock_data),
                    "data": stock_data[:10] if stock_data else []  # Return first 10 points
                },
                "market_cap": {
                    "success": market_cap is not None,
                    "value": market_cap,
                    "formatted": f"${market_cap:,.0f}" if market_cap else None
                },
                "test_summary": {
                    "overall_success": len(stock_data) > 0 or market_cap is not None,
                    "stock_data_available": len(stock_data) > 0,
                    "market_cap_available": market_cap is not None,
                    "total_data_points": len(stock_data)
                }
            }
            
            # Add market cap to stock data if both are available
            if stock_data and market_cap:
                for data_point in response["stock_data"]["data"]:
                    data_point["market_cap"] = market_cap
            
            logger.info(f"Test completed for {symbol}: {response['test_summary']}")
            
            return JSONResponse(response)
        
    except ValueError as e:
        if "time data" in str(e):
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        else:
            raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error testing symbol {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error testing symbol: {str(e)}")

@app.get("/test/symbol/{symbol}")
async def test_symbol_data_get(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Test data fetching for a specific symbol via GET request."""
    return await test_symbol_data(symbol, start_date, end_date)

@app.get("/debug/yahoo/{symbol}")
async def debug_yahoo_finance(symbol: str):
    """Debug Yahoo Finance to see what's actually happening."""
    try:
        symbol = symbol.upper().strip()
        if not symbol or len(symbol) > 10:
            raise HTTPException(status_code=400, detail="Invalid symbol format")
        
        logger.info(f"Debug testing Yahoo Finance for symbol: {symbol}")
        
        from aiohttp import ClientSession, ClientTimeout
        
        timeout = ClientTimeout(total=60)
        async with ClientSession(timeout=timeout) as session:
            # Use DataFetcher's debug method
            debug_results = await data_fetcher.debug_yahoo_finance(symbol, session)
            
            logger.info(f"Debug test completed for {symbol}")
            return JSONResponse(debug_results)
        
    except Exception as e:
        logger.error(f"Error debugging Yahoo Finance for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error debugging Yahoo Finance: {str(e)}")

@app.get("/test/yahoo/{symbol}")
async def test_yahoo_finance(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    test_type: Optional[str] = "both"  # "both", "data", "market_cap"
):
    """Test Yahoo Finance specifically using DataFetcher's built-in logic."""
    # Default to last 3 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    
    try:
        # Validate inputs
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start > end:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        symbol = symbol.upper().strip()
        if not symbol or len(symbol) > 10:
            raise HTTPException(status_code=400, detail="Invalid symbol format")
        
        logger.info(f"Testing Yahoo Finance for symbol: {symbol}, type: {test_type}")
        
        from aiohttp import ClientSession, ClientTimeout
        
        timeout = ClientTimeout(total=120)
        async with ClientSession(timeout=timeout) as session:
            # Use DataFetcher's built-in Yahoo Finance testing method
            results = await data_fetcher.test_yahoo_finance(symbol, start_date, end_date, session, test_type)
            
            logger.info(f"Yahoo Finance test completed for {symbol}")
            return JSONResponse(results)
        
    except ValueError as e:
        if "time data" in str(e):
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        else:
            raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error testing Yahoo Finance for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error testing Yahoo Finance: {str(e)}")

@app.get("/health")
async def health_check():
    """Detailed health check."""
    health_status = {
        "timestamp": datetime.now().isoformat(),
        "redis": "healthy" if redis_available else "unavailable"
    }
    
    try:
        # Test database connection
        db.conn.execute("SELECT 1").fetchone()
        health_status["database"] = "healthy"
        
        # Test if market_data table exists and has data
        try:
            count_result = db.conn.execute("SELECT COUNT(*) as count FROM market_data").fetchdf()
            health_status["market_data_count"] = int(count_result.iloc[0]['count'])
        except Exception as table_error:
            health_status["market_data_count"] = 0
            health_status["market_data_error"] = str(table_error)
            
    except Exception as db_error:
        health_status["database"] = "unhealthy"
        health_status["database_error"] = str(db_error)
    
    overall_status = "healthy" if health_status["database"] == "healthy" else "unhealthy"
    health_status["status"] = overall_status
    
    return health_status

@app.get("/config/data-fetcher")
async def get_data_fetcher_config():
    """Get current data fetcher configuration."""
    global data_fetcher
    if not data_fetcher:
        data_fetcher = DataFetcher()
    
    settings = data_fetcher.get_current_settings()
    cache_info = {
        'universe_cache_size': len(data_fetcher.universe_cache),
        'market_cap_cache_size': len(data_fetcher.market_cap_cache)
    }
    
    return {
        "settings": settings,
        "cache_info": cache_info,
        "description": {
            "candidate_symbols": "Top N companies by market cap from ALL 500 S&P companies",
            "max_symbols": "Final number of companies selected for index",
            "batch_size": "Number of symbols processed concurrently",
            "process": "Two-stage: 1) Evaluate ALL 500 S&P companies by real market cap, 2) Select top candidates, then final top companies"
        }
    }

@app.post("/config/data-fetcher")
async def configure_data_fetcher(
    candidate_symbols: int = None,
    max_symbols: int = None, 
    batch_size: int = None,
    rate_limit_delay: int = None
):
    """Configure data fetcher settings."""
    global data_fetcher
    if not data_fetcher:
        data_fetcher = DataFetcher()
    
    data_fetcher.configure_settings(
        max_symbols=max_symbols,
        candidate_symbols=candidate_symbols,
        batch_size=batch_size,
        rate_limit_delay=rate_limit_delay
    )
    
    return {
        "message": "Configuration updated",
        "new_settings": data_fetcher.get_current_settings()
    }

@app.post("/test/symbol-selection")
async def test_symbol_selection():
    """Test the two-stage symbol selection process."""
    global data_fetcher
    if not data_fetcher:
        data_fetcher = DataFetcher()
    
    try:
        start_time = datetime.now()
        symbols = await data_fetcher.fetch_symbols()
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        
        return {
            "success": True,
            "symbols_selected": len(symbols),
            "symbols_preview": symbols[:20],  # First 20 symbols
            "duration_seconds": round(duration, 2),
            "settings_used": data_fetcher.get_current_settings(),
            "cache_metadata": data_fetcher.universe_cache.get('metadata', {}),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
import redis
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import json
import os
import logging
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

from .database_simple import Database
from .data_fetcher_simple import DataFetcher

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
    end_date: str = None
):
    """Get index performance data."""
    # Default to last 30 days
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    try:
        # Check cache first
        cache_key = f"performance:{start_date}:{end_date}"
        if redis_available:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                return JSONResponse(json.loads(cached_data))
        
        # Get performance data from database
        performance_df = db.get_index_performance(start_date, end_date)
        
        if performance_df.empty:
            # Try to calculate if no data exists
            performance_df = db.calculate_index_performance(start_date, end_date)
            if not performance_df.empty:
                db.save_index_performance(performance_df)
        
        if performance_df.empty:
            raise HTTPException(
                status_code=404, 
                detail="No performance data available for the specified date range"
            )
        
        # Convert to JSON-serializable format
        performance_data = {
            "start_date": start_date,
            "end_date": end_date,
            "data": performance_df.to_dict('records')
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
        market_df = db.get_market_data(start_date, end_date, symbol_list)
        
        if market_df.empty:
            raise HTTPException(
                status_code=404,
                detail="No market data available for the specified criteria"
            )
        
        # Apply limit
        if len(market_df) > limit:
            market_df = market_df.head(limit)
        
        return JSONResponse({
            "start_date": start_date,
            "end_date": end_date,
            "symbols": symbol_list,
            "count": len(market_df),
            "data": market_df.to_dict('records')
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
        companies_df = db.get_top_companies(date, limit)
        
        if companies_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No company data available for {date}"
            )
        
        return JSONResponse({
            "date": date,
            "count": len(companies_df),
            "companies": companies_df.to_dict('records')
        })
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Error getting top companies: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
async def health_check():
    """Detailed health check."""
    try:
        # Test database connection
        db.conn.execute("SELECT 1").fetchone()
        db_status = "healthy"
    except:
        db_status = "unhealthy"
    
    redis_status = "healthy" if redis_available else "unavailable"
    
    return {
        "status": "healthy" if db_status == "healthy" else "unhealthy",
        "database": db_status,
        "redis": redis_status,
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

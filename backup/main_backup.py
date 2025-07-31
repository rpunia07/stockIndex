from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
import redis
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import json
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Print environment variables (excluding the API key value for security)
print("Environment variables loaded:")
print(f"ALPHA_VANTAGE_KEY present: {bool(os.getenv('ALPHA_VANTAGE_KEY'))}")
print(f"REDIS_HOST: {os.getenv('REDIS_HOST')}")
print(f"REDIS_PORT: {os.getenv('REDIS_PORT')}")

from .database_simple import Database
from .data_fetcher_simple import DataFetcher

app = FastAPI(title="Stock Index Service")

# Initialize Redis
try:
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )
    redis_available = True
except:
    print("Warning: Redis not available. Running without caching.")
    redis_available = False

# Initialize database and data fetcher
db = Database()
data_fetcher = DataFetcher()

def cache_key(endpoint: str, **params) -> str:
    """Generate a cache key for Redis."""
    return f"index:{endpoint}:{json.dumps(params, sort_keys=True)}"

async def fetch_and_store_data(start_date: str, end_date: str):
    """Background task to fetch and store market data."""
    data = await data_fetcher.fetch_all_data(start_date, end_date)
    merged_data = data_fetcher.merge_data_sources(data)
    db.insert_market_data(merged_data)

@app.post("/build-index")
async def build_index(
    background_tasks: BackgroundTasks,
    start_date: str,
    end_date: str,
    force_refresh: bool = False
):
    """Build or rebuild the index for a date range."""
    try:
        print(f"\nStarting index build for period {start_date} to {end_date}")
        print(f"Force refresh: {force_refresh}")
        
        try:
            # Try to get any existing data for the date range
            print("Checking for existing data...")
            test_data = db.get_performance(start_date, end_date)
            
            if not test_data or force_refresh:
                print("No existing data found or force refresh requested")
                print("Starting data fetch from Alpha Vantage...")
                
                # If no data exists or force refresh is requested, fetch new data
                data = await data_fetcher.fetch_all_data(start_date, end_date)
                if data:
                    print(f"Successfully fetched {len(data)} data points")
                    print("Merging and storing data...")
                    merged_data = data_fetcher.merge_data_sources(data)
                    db.insert_market_data(merged_data)
                    print("Data successfully stored in database")
                else:
                    print("Error: No data was fetched from Alpha Vantage")
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to fetch market data. Please try again later."
                    )
        except Exception as e:
            print(f"Error checking/fetching data: {str(e)}")
            # Initial data fetch
            data = await data_fetcher.fetch_all_data(start_date, end_date)
            if data:
                merged_data = data_fetcher.merge_data_sources(data)
                db.insert_market_data(merged_data)
            else:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to fetch initial market data. Please try again later."
                )

        # Build the index
        db.build_index(start_date, end_date)
        
        # Track composition changes
        current = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        while current <= end:
            db.track_composition_changes(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)

        return {"message": "Index built successfully"}
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"Error building index: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build index: {str(e)}"
        )

@app.get("/index-performance")
async def get_index_performance(start_date: str, end_date: str):
    """Get index performance for a date range."""
    cache_id = cache_key("performance", start_date=start_date, end_date=end_date)
    
    # Try to get from cache if Redis is available
    if redis_available:
        try:
            cached = redis_client.get(cache_id)
            if cached:
                return json.loads(cached)
        except:
            pass
    
    # Get from database
    performance = db.get_performance(start_date, end_date)
    
    # Cache the result if Redis is available
    if redis_available:
        try:
            redis_client.setex(cache_id, 3600, json.dumps(performance))  # Cache for 1 hour
        except:
            pass
    
    return performance

@app.get("/index-composition")
async def get_index_composition(date: str):
    """Get index composition for a specific date."""
    cache_id = cache_key("composition", date=date)
    
    # Try to get from cache
    cached = redis_client.get(cache_id)
    if cached:
        return json.loads(cached)
    
    # Get from database
    composition = db.get_composition(date)
    
    # Cache the result
    redis_client.setex(cache_id, 3600, json.dumps(composition))
    
    return composition

@app.get("/composition-changes")
async def get_composition_changes(start_date: str, end_date: str):
    """Get composition changes for a date range."""
    cache_id = cache_key("changes", start_date=start_date, end_date=end_date)
    
    # Try to get from cache
    cached = redis_client.get(cache_id)
    if cached:
        return json.loads(cached)
    
    # Get from database
    changes = db.get_composition_changes(start_date, end_date)
    
    # Cache the result
    redis_client.setex(cache_id, 3600, json.dumps(changes))
    
    return changes

@app.post("/export-data")
async def export_data(start_date: str, end_date: str):
    """Export index data to Excel."""
    # Create a temporary directory if it doesn't exist
    export_dir = Path("temp_exports")
    export_dir.mkdir(exist_ok=True)
    
    # Generate Excel file
    excel_path = export_dir / f"index_data_{start_date}_to_{end_date}.xlsx"
    
    with pd.ExcelWriter(excel_path) as writer:
        # Export performance data
        performance = pd.DataFrame(db.get_performance(start_date, end_date))
        performance.to_excel(writer, sheet_name='Performance', index=False)
        
        # Export composition data
        composition = pd.DataFrame(db.get_composition(end_date))
        composition.to_excel(writer, sheet_name='Latest Composition', index=False)
        
        # Export changes data
        changes = pd.DataFrame(db.get_composition_changes(start_date, end_date))
        changes.to_excel(writer, sheet_name='Composition Changes', index=False)
    
    return FileResponse(
        path=excel_path,
        filename=excel_path.name,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

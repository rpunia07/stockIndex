# Stock Index Management Service

A comprehensive FastAPI-based service for managing stock market indices with real-time data fetching, market cap analysis, and performance tracking. The service uses advanced two-stage symbol selection to build indices based on actual market capitalization from all S&P 500 companies.

## 🏗️ Architecture Overview

### Core Components

1. **FastAPI Application** (`app/main.py`)
   - RESTful API endpoints for all operations
   - Background task management
   - Redis caching integration
   - Comprehensive error handling

2. **Data Fetcher** (`app/data_fetcher.py`)
   - Multi-source data aggregation (Yahoo Finance, Alpha Vantage, Polygon.io, IEX Cloud, FMP)
   - Two-stage symbol selection system
   - Real-time market capitalization evaluation
   - Intelligent fallback mechanisms

3. **Database Manager** (`app/database.py`)
   - DuckDB integration for efficient SQL operations
   - Market data storage and retrieval
   - Index performance calculations
   - Optimized timestamp handling

4. **Caching System**
   - Redis for API response caching
   - File-based caching for symbol universe
   - Market cap data caching with TTL

## 🚀 Key Features

### Advanced Symbol Selection
- **Comprehensive Evaluation**: Analyzes ALL 500 S&P companies by real market cap
- **Two-Stage Process**: 
  1. Evaluate all 500 S&P companies using Yahoo Finance market cap data
  2. Select top N candidates by market cap, then final top M for index
- **Dynamic Rankings**: Updates based on real market movements
- **Configurable Parameters**: Adjustable candidate pool and final selection sizes

### Multi-Source Data Integration
- **Primary**: Yahoo Finance (with CSRF crumb authentication)
- **Fallbacks**: Alpha Vantage → Polygon.io → IEX Cloud → FMP
- **Rate Limiting**: Intelligent delays and batch processing
- **Error Recovery**: Automatic fallback on source failures

### Performance Optimization
- **Concurrent Processing**: Batch requests with configurable concurrency
- **Smart Caching**: Multi-layer caching strategy
- **Database Efficiency**: DuckDB for fast analytical queries
- **Background Processing**: Non-blocking data fetching

## 📊 Data Sources

| Source | Usage | Features |
|--------|-------|----------|
| **Yahoo Finance** | Primary source for stock data and market cap | Real-time data, comprehensive coverage, CSRF authentication |
| **Alpha Vantage** | Fallback for stock data | High-quality data, API key required |
| **Polygon.io** | Secondary fallback | Free tier available, good coverage |
| **IEX Cloud** | Third fallback | Reliable, good for basic data |
| **FMP** | Final fallback | Financial modeling data |

## 🛠️ Setup and Installation

### Prerequisites
```bash
Python 3.8+
Redis (optional but recommended)
```

### Environment Variables
```bash
# Required
ALPHA_VANTAGE_KEY=your_alpha_vantage_api_key

# Optional
REDIS_HOST=localhost
REDIS_PORT=6379
```

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd assignment

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Start the server
python -m app.main
```

The server will start at `http://localhost:8000`

## 📚 API Documentation

### Health and Status Endpoints

#### `GET /`
**Health Check**
- **Description**: Basic health check endpoint
- **Response**: Service status and Redis availability
```json
{
  "message": "Stock Index Management Service",
  "status": "healthy",
  "redis_available": true
}
```

#### `GET /health`
**Detailed Health Check**
- **Description**: Comprehensive health status
- **Response**: Database, Redis, and timestamp information
```json
{
  "status": "healthy",
  "database": "healthy",
  "redis": "healthy",
  "timestamp": "2025-07-26T18:00:00.000000"
}
```

### Data Fetching Endpoints

#### `POST /data/fetch`
**Fetch Market Data**
- **Description**: Initiates background data fetching for specified date range
- **Parameters**: 
  - `start_date` (optional): YYYY-MM-DD format, defaults to 30 days ago
  - `end_date` (optional): YYYY-MM-DD format, defaults to today
  - `force_refresh` (optional): Boolean, bypasses cache
- **Response**: Task initiation confirmation
```json
{
  "message": "Data fetch initiated",
  "start_date": "2025-06-26",
  "end_date": "2025-07-26",
  "status": "in_progress"
}
```

#### `GET /market-data`
**Retrieve Market Data**
- **Description**: Get stored market data with filtering options
- **Parameters**:
  - `start_date` (optional): YYYY-MM-DD
  - `end_date` (optional): YYYY-MM-DD
  - `symbols` (optional): Comma-separated symbol list
  - `limit` (optional): Maximum records, default 1000
- **Response**: Filtered market data
```json
{
  "start_date": "2025-07-20",
  "end_date": "2025-07-26",
  "symbols": ["AAPL", "MSFT"],
  "count": 10,
  "data": [...]
}
```

### Index Management Endpoints

#### `POST /index/build`
**Build Stock Index**
- **Description**: Builds complete stock index with data fetching and performance calculation
- **Parameters**:
  - `start_date` (optional): YYYY-MM-DD
  - `end_date` (optional): YYYY-MM-DD
  - `force_rebuild` (optional): Boolean, forces complete rebuild
- **Response**: Index build status
```json
{
  "message": "Index build initiated",
  "start_date": "2025-06-26",
  "end_date": "2025-07-26",
  "status": "building",
  "estimated_time": "5-10 minutes depending on data volume"
}
```

#### `GET /index/performance`
**Get Index Performance**
- **Description**: Retrieve calculated index performance metrics
- **Parameters**:
  - `start_date` (optional): YYYY-MM-DD
  - `end_date` (optional): YYYY-MM-DD
- **Response**: Performance data and metrics
```json
{
  "start_date": "2025-06-26",
  "end_date": "2025-07-26",
  "data": [
    {
      "date": "2025-07-26",
      "index_value": 100.0,
      "daily_return": 0.025,
      "cumulative_return": 0.15
    }
  ]
}
```

#### `GET /companies/top`
**Get Top Companies**
- **Description**: Retrieve top companies by market cap for a specific date
- **Parameters**:
  - `date` (optional): YYYY-MM-DD, defaults to today
  - `limit` (optional): Number of companies, default 100
- **Response**: Ranked company list
```json
{
  "date": "2025-07-26",
  "count": 100,
  "companies": [
    {
      "symbol": "AAPL",
      "company_name": "Apple Inc.",
      "market_cap": 3194468958208,
      "rank": 1
    }
  ]
}
```

### Configuration Endpoints

#### `GET /config/data-fetcher`
**Get Data Fetcher Configuration**
- **Description**: Retrieve current data fetcher settings and cache information
- **Response**: Configuration details and cache status
```json
{
  "settings": {
    "candidate_symbols": 200,
    "max_symbols": 100,
    "batch_size": 10,
    "rate_limit_delay": 12,
    "yahoo_batch_delay": 2,
    "cache_duration_days": 7
  },
  "cache_info": {
    "universe_cache_size": 500,
    "market_cap_cache_size": 500
  },
  "description": {
    "candidate_symbols": "Top N companies by market cap from ALL 500 S&P companies",
    "max_symbols": "Final number of companies selected for index",
    "batch_size": "Number of symbols processed concurrently",
    "process": "Two-stage: 1) Evaluate ALL 500 S&P companies by real market cap, 2) Select top candidates, then final top companies"
  }
}
```

#### `POST /config/data-fetcher`
**Configure Data Fetcher**
- **Description**: Update data fetcher configuration settings
- **Parameters**:
  - `candidate_symbols` (optional): Number of top companies to evaluate
  - `max_symbols` (optional): Final number of companies for index
  - `batch_size` (optional): Concurrent processing batch size
  - `rate_limit_delay` (optional): Delay between API requests
- **Response**: Updated configuration
```json
{
  "message": "Configuration updated",
  "new_settings": {
    "candidate_symbols": 200,
    "max_symbols": 100,
    "batch_size": 15,
    "rate_limit_delay": 10
  }
}
```

### Testing Endpoints

#### `POST /test/symbol-selection`
**Test Symbol Selection Process**
- **Description**: Test the complete two-stage symbol selection system
- **Response**: Selection results and performance metrics
```json
{
  "success": true,
  "symbols_selected": 100,
  "symbols_preview": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"],
  "duration_seconds": 45.2,
  "settings_used": {
    "candidate_symbols": 200,
    "max_symbols": 100
  },
  "cache_metadata": {},
  "timestamp": "2025-07-26T18:00:00.000000"
}
```

#### `POST /test/symbol`
**Test Individual Symbol Data**
- **Description**: Test data fetching for a specific symbol across all sources
- **Parameters**:
  - `symbol` (required): Stock symbol
  - `start_date` (optional): YYYY-MM-DD
  - `end_date` (optional): YYYY-MM-DD
- **Response**: Comprehensive test results
```json
{
  "symbol": "AAPL",
  "start_date": "2025-07-21",
  "end_date": "2025-07-26",
  "stock_data": {
    "success": true,
    "data_points": 5,
    "data": [...]
  },
  "market_cap": {
    "success": true,
    "value": 3194468958208,
    "formatted": "$3,194,468,958,208"
  },
  "test_summary": {
    "overall_success": true,
    "stock_data_available": true,
    "market_cap_available": true,
    "total_data_points": 5
  }
}
```

#### `GET /test/symbol/{symbol}`
**Test Symbol Data (GET)**
- **Description**: GET version of symbol testing
- **Parameters**: Same as POST version but via URL path and query parameters

#### `GET /test/yahoo/{symbol}`
**Test Yahoo Finance Integration**
- **Description**: Specifically test Yahoo Finance data source
- **Parameters**:
  - `symbol` (required): Stock symbol
  - `start_date` (optional): YYYY-MM-DD
  - `end_date` (optional): YYYY-MM-DD
  - `test_type` (optional): "both", "data", or "market_cap"
- **Response**: Yahoo Finance specific test results
```json
{
  "symbol": "AAPL",
  "test_type": "both",
  "stock_data": {
    "success": true,
    "data_points": 3,
    "method": "_get_daily_data_yahoo"
  },
  "market_cap": {
    "success": true,
    "value": 3194468958208,
    "formatted": "$3,194,468,958,208",
    "method": "_get_market_cap_yahoo"
  },
  "test_summary": {
    "overall_success": true,
    "uses_existing_logic": true,
    "note": "Uses existing DataFetcher Yahoo Finance methods with Microsoft Edge headers"
  }
}
```

#### `GET /debug/yahoo/{symbol}`
**Debug Yahoo Finance**
- **Description**: Debug Yahoo Finance integration with detailed logging
- **Parameters**: `symbol` (required)
- **Response**: Detailed debug information

## 🔧 Configuration Parameters

### Data Fetcher Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `candidate_symbols` | 200 | Top N companies by market cap to evaluate from all 500 S&P companies |
| `max_symbols` | 100 | Final number of companies selected for the index |
| `batch_size` | 10 | Number of symbols processed concurrently |
| `rate_limit_delay` | 12 | Seconds between Alpha Vantage API requests |
| `yahoo_batch_delay` | 2 | Seconds between Yahoo Finance batch requests |
| `cache_duration_days` | 7 | Days to cache symbol universe and market cap data |

### Two-Stage Selection Process

1. **Stage 1: Comprehensive Evaluation**
   - Fetches ALL 500 S&P 500 companies from Wikipedia
   - Evaluates real market capitalization using Yahoo Finance
   - Processes companies in configurable batches (default: 10 concurrent)
   - Ranks all 500 companies by actual market cap
   - Selects top `candidate_symbols` (default: 200) companies

2. **Stage 2: Final Selection**
   - From the top candidates, selects final `max_symbols` (default: 100)
   - Ensures index represents truly largest companies by market cap

## 📈 Performance Features

### Caching Strategy
- **Redis Caching**: API responses cached for 30 minutes to 1 hour
- **File-based Universe Cache**: S&P 500 company list cached for 7 days
- **Market Cap Cache**: Individual company market caps cached for 7 days
- **Intelligent Cache Invalidation**: Automatic cache refresh based on data age

### Rate Limiting
- **Yahoo Finance**: 2-second delays between batch requests
- **Alpha Vantage**: 12-second delays between requests (free tier limit)
- **Batch Processing**: Configurable concurrent request limits
- **Error Handling**: Automatic retry with exponential backoff

### Database Optimization
- **DuckDB**: High-performance analytical database
- **Optimized Queries**: Efficient SQL for large dataset operations
- **Timestamp Handling**: Proper date format conversion for compatibility
- **Index Performance**: Fast calculations for large time series data

## 🔍 Example Usage Scenarios

### Building a Market Cap-Weighted Index
```bash
# 1. Configure for S&P 100 equivalent (top 100 by market cap)
curl -X POST "http://localhost:8000/config/data-fetcher?candidate_symbols=200&max_symbols=100"

# 2. Test the symbol selection
curl -X POST "http://localhost:8000/test/symbol-selection"

# 3. Build the index with historical data
curl -X POST "http://localhost:8000/index/build?start_date=2025-01-01&end_date=2025-07-26"

# 4. Get index performance
curl "http://localhost:8000/index/performance?start_date=2025-01-01&end_date=2025-07-26"
```

### Testing Data Sources
```bash
# Test Yahoo Finance for Apple
curl "http://localhost:8000/test/yahoo/AAPL"

# Test all sources for Microsoft
curl -X POST "http://localhost:8000/test/symbol" -d "symbol=MSFT" -H "Content-Type: application/json"

# Debug Yahoo Finance integration
curl "http://localhost:8000/debug/yahoo/TSLA"
```

### Monitoring and Configuration
```bash
# Check system health
curl "http://localhost:8000/health"

# View current configuration
curl "http://localhost:8000/config/data-fetcher"

# Get top companies by market cap
curl "http://localhost:8000/companies/top?limit=50"
```

## 🚨 Error Handling

### HTTP Status Codes
- **200**: Success
- **400**: Bad Request (invalid parameters)
- **404**: Data not found
- **500**: Internal server error

### Error Response Format
```json
{
  "detail": "Error description",
  "status_code": 400
}
```

### Common Error Scenarios
1. **Invalid Date Format**: Use YYYY-MM-DD format
2. **Missing API Keys**: Ensure ALPHA_VANTAGE_KEY is set
3. **Rate Limiting**: Automatic handling with delays
4. **Data Source Failures**: Automatic fallback to alternative sources

## 🔒 Security Considerations

- **API Key Management**: Environment variable based
- **Rate Limiting**: Built-in protection against API abuse
- **Input Validation**: Comprehensive parameter validation
- **Error Information**: Limited error details in production responses

## 📝 Logging

### Log Levels
- **INFO**: General operation information
- **WARNING**: Non-critical issues and fallbacks
- **ERROR**: Critical errors requiring attention
- **DEBUG**: Detailed debugging information

### Key Log Categories
- Data fetching operations
- API source fallbacks
- Market cap evaluations
- Cache operations
- Database transactions

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Stock Index Management Service** - Advanced market data aggregation and index management with intelligent symbol selection based on real market capitalization.

### GET /composition-changes
Identifies stocks entering or exiting the index within a date range.

```bash
curl "http://localhost:8000/composition-changes?start_date=2023-01-01&end_date=2023-01-31"
```

### POST /export-data
Exports all results to an Excel file.

```bash
curl -X POST "http://localhost:8000/export-data?start_date=2023-01-01&end_date=2023-01-31" --output index_data.xlsx
```

## Development

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run Tests
```bash
pytest tests/
```

### Running Locally
```bash
uvicorn app.main:app --reload
```

## Architecture

The service is built with:
- FastAPI for the REST API
- DuckDB for efficient SQL operations on market data
- Redis for API response caching
- Docker and Docker Compose for containerization
- Pandas for data manipulation
- yfinance and Alpha Vantage for market data

### Data Flow
1. Market data is fetched from multiple sources and merged
2. Raw data is stored in DuckDB tables
3. Index calculations are performed using SQL
4. Results are cached in Redis
5. APIs serve data from cache when available

## Database Schema

### market_data
- date: DATE
- symbol: VARCHAR
- price: DOUBLE
- market_cap: DOUBLE
- volume: BIGINT
- PRIMARY KEY (date, symbol)

### index_constituents
- date: DATE
- symbol: VARCHAR
- weight: DOUBLE
- rank: INT
- PRIMARY KEY (date, symbol)

### index_performance
- date: DATE PRIMARY KEY
- daily_return: DOUBLE
- cumulative_return: DOUBLE

### composition_changes
- date: DATE
- symbol: VARCHAR
- change_type: VARCHAR ('ENTRY' or 'EXIT')
- PRIMARY KEY (date, symbol)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

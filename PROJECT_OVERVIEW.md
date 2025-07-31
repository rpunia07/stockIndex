# Stock Index Management Service - Simplified

A simplified FastAPI service for managing stock market data and calculating market cap-weighted index performance.

## Project Structure

```
├── app/
│   ├── main.py          # FastAPI application with API endpoints
│   ├── database.py      # DuckDB operations for data storage
│   └── data_fetcher.py  # Data fetching with Alpha Vantage + Yahoo Finance fallback
├── backup/              # Backup of original complex implementation
├── data/                # Cache files for market data
├── tests/               # Test files
├── requirements.txt     # Python dependencies
├── .env                 # Environment variables
└── README.md           # This file
```

## Key Features

- **Simple Data Fetching**: Alpha Vantage API with Yahoo Finance web scraping fallback
- **Market Cap Calculation**: Handles formats like "4.1T", "2.5B", "500M"
- **Caching**: Redis for API responses, local JSON for market cap data
- **Database**: DuckDB for efficient SQL operations on market data
- **Rate Limiting**: Built-in delays to respect API limits

## API Endpoints

- `GET /` - Health check
- `POST /data/fetch` - Fetch market data for date range
- `GET /index/performance` - Get index performance data
- `GET /market-data` - Get raw market data
- `GET /companies/top` - Get top companies by market cap
- `GET /health` - Detailed health check

## Environment Variables

```
ALPHA_VANTAGE_KEY=your_api_key_here
REDIS_HOST=localhost
REDIS_PORT=6379
```

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up environment variables in `.env` file

3. Run the application:
   ```bash
   python -m uvicorn app.main:app --reload
   ```

4. Access the API at `http://localhost:8000`

## Data Flow

1. **Fetch Symbols**: Get S&P 500 list from Wikipedia
2. **Market Data**: Fetch daily prices from Alpha Vantage
3. **Market Cap**: Get from Alpha Vantage, fallback to Yahoo Finance scraping
4. **Storage**: Store in DuckDB with caching
5. **Index Calculation**: Market cap-weighted performance metrics

## Fallback Strategy

- Primary: Alpha Vantage API (rate-limited)
- Fallback: Yahoo Finance web scraping for market cap
- Cache: Local JSON files + Redis (optional)

This simplified version provides the same functionality as the original complex implementation but with much cleaner, more maintainable code.

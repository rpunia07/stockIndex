# API Quick Reference

## Base URL
```
http://localhost:8000
```

## Quick Command Reference

### Health Checks
```bash
# Basic health check
curl "http://localhost:8000/"

# Detailed health check
curl "http://localhost:8000/health"
```

### Configuration Management
```bash
# Get current configuration
curl "http://localhost:8000/config/data-fetcher"

# Update configuration (PowerShell)
Invoke-RestMethod -Uri "http://localhost:8000/config/data-fetcher?candidate_symbols=200&max_symbols=100" -Method POST

# Update configuration (bash)
curl -X POST "http://localhost:8000/config/data-fetcher?candidate_symbols=200&max_symbols=100"
```

### Symbol Selection Testing
```bash
# Test symbol selection process (PowerShell)
Invoke-RestMethod -Uri "http://localhost:8000/test/symbol-selection" -Method POST

# Test symbol selection process (bash)
curl -X POST "http://localhost:8000/test/symbol-selection"
```

### Individual Symbol Testing
```bash
# Test Yahoo Finance for specific symbol
curl "http://localhost:8000/test/yahoo/AAPL"

# Test all sources for symbol (PowerShell)
Invoke-RestMethod -Uri "http://localhost:8000/test/symbol/MSFT"

# Debug Yahoo Finance
curl "http://localhost:8000/debug/yahoo/TSLA"
```

### Data Management
```bash
# Fetch market data (PowerShell)
Invoke-RestMethod -Uri "http://localhost:8000/data/fetch?start_date=2025-07-01&end_date=2025-07-26" -Method POST

# Get market data
curl "http://localhost:8000/market-data?start_date=2025-07-20&symbols=AAPL,MSFT,GOOGL"

# Get top companies
curl "http://localhost:8000/companies/top?limit=50"
```

### Index Operations
```bash
# Build index (PowerShell)
Invoke-RestMethod -Uri "http://localhost:8000/index/build?start_date=2025-07-01&end_date=2025-07-26" -Method POST

# Get index performance
curl "http://localhost:8000/index/performance?start_date=2025-07-01&end_date=2025-07-26"
```

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `candidate_symbols` | 200 | Companies to evaluate from S&P 500 |
| `max_symbols` | 100 | Final companies in index |
| `batch_size` | 10 | Concurrent requests |
| `rate_limit_delay` | 12 | Seconds between API calls |

## Response Status Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 400 | Bad Request |
| 404 | Not Found |
| 500 | Internal Error |

## Two-Stage Selection Process

1. **Stage 1**: Evaluate ALL 500 S&P companies by real market cap
2. **Stage 2**: Select top N candidates, then final M companies

## Data Sources (in order of preference)

1. **Yahoo Finance** (Primary) - Real-time data with CSRF auth
2. **Alpha Vantage** (Fallback) - Requires API key
3. **Polygon.io** (Secondary) - Free tier available
4. **IEX Cloud** (Third) - Basic data
5. **FMP** (Final) - Financial modeling data

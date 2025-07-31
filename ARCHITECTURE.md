# Service Architecture Documentation

## System Overview

The Stock Index Management Service is built with a modular, scalable architecture that enables efficient processing of large-scale financial data with intelligent fallback mechanisms.

## Component Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Client Applications                          │
│                     (Web, Mobile, API Clients)                     │
└─────────────────────────┬───────────────────────────────────────────┘
                          │ HTTP/REST
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      FastAPI Application                           │
│                        (app/main.py)                               │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────┐   │
│  │   Health Check  │ │  Configuration  │ │   Background Tasks   │   │
│  │   Endpoints     │ │   Management    │ │    Management       │   │
│  └─────────────────┘ └─────────────────┘ └─────────────────────┘   │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Data Fetcher Service                            │
│                     (app/data_fetcher.py)                         │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                Two-Stage Symbol Selection                   │   │
│  │                                                             │   │
│  │  Stage 1: ALL 500 S&P → Market Cap Evaluation             │   │
│  │  Stage 2: Top N Candidates → Final M Selection             │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Multi-Source Data Aggregation                 │   │
│  │                                                             │   │
│  │  Yahoo Finance → Alpha Vantage → Polygon.io → IEX → FMP    │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Database Layer                                 │
│                    (app/database.py)                               │
│                                                                     │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────┐   │
│  │   Market Data   │ │     Index       │ │    Performance      │   │
│  │    Storage      │ │   Calculations  │ │    Analytics        │   │
│  └─────────────────┘ └─────────────────┘ └─────────────────────┘   │
│                                                                     │
│                        DuckDB Engine                               │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Caching Layer                                 │
│                                                                     │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────┐   │
│  │   Redis Cache   │ │  File-based     │ │   Market Cap        │   │
│  │  (API Responses)│ │  Universe Cache │ │     Cache           │   │
│  └─────────────────┘ └─────────────────┘ └─────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow Architecture

### Symbol Selection Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Wikipedia     │───▶│    Stage 1:     │───▶│    Stage 2:     │
│   S&P 500 List  │    │  Evaluate ALL   │    │  Select Final   │
│   (500 symbols) │    │  500 Companies  │    │  100 Companies  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │                        │
                              ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │  Yahoo Finance  │    │   Top 200 by    │
                       │   Market Cap    │    │   Market Cap    │
                       │   Evaluation    │    │   → Top 100     │
                       └─────────────────┘    └─────────────────┘
```

### Data Fetching Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   API Request   │───▶│   Data Fetcher  │───▶│    Database     │
│                 │    │                 │    │    Storage      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │   Fallback Chain    │
                    │                     │
                    │  1. Yahoo Finance   │
                    │  2. Alpha Vantage   │
                    │  3. Polygon.io      │
                    │  4. IEX Cloud       │
                    │  5. FMP             │
                    └─────────────────────┘
```

## Service Responsibilities

### FastAPI Application (main.py)
- **Role**: API Gateway and Request Orchestration
- **Responsibilities**:
  - HTTP request routing and validation
  - Background task management
  - Redis cache integration
  - Error handling and response formatting
  - Authentication and authorization (future)

### Data Fetcher (data_fetcher.py)
- **Role**: Data Acquisition and Processing Engine
- **Responsibilities**:
  - Multi-source data aggregation
  - Intelligent fallback management
  - Rate limiting and batch processing
  - Market cap evaluation and ranking
  - Symbol selection algorithms

### Database Manager (database.py)
- **Role**: Data Persistence and Analytics
- **Responsibilities**:
  - DuckDB connection management
  - Market data storage and retrieval
  - Index performance calculations
  - SQL query optimization
  - Data integrity validation

## Scalability Features

### Horizontal Scaling
- **Stateless Design**: Application instances can be load balanced
- **Background Tasks**: Decoupled processing for long-running operations
- **Cache Layer**: Redis can be clustered for high availability
- **Database**: DuckDB optimized for analytical workloads

### Vertical Scaling
- **Concurrent Processing**: Batch operations with configurable concurrency
- **Memory Optimization**: Efficient data structures and caching
- **I/O Optimization**: Async operations for external API calls

## Performance Optimizations

### Caching Strategy
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Redis Cache   │    │  File Cache     │    │ Memory Cache    │
│   (30-60 min)   │    │   (7 days)      │    │  (Session)      │
│                 │    │                 │    │                 │
│ • API responses │    │ • Symbol lists  │    │ • Connections   │
│ • Performance   │    │ • Market caps   │    │ • Session data  │
│   metrics       │    │ • Metadata      │    │ • Temp results  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Rate Limiting
- **Yahoo Finance**: 2-second delays between batch requests
- **Alpha Vantage**: 12-second delays (free tier compliance)
- **Batch Processing**: Configurable concurrent request limits
- **Circuit Breaker**: Automatic source switching on failures

## Error Handling and Resilience

### Fallback Mechanisms
1. **Data Source Fallback**: Automatic switching between data providers
2. **Cache Fallback**: Serve stale data if sources unavailable
3. **Graceful Degradation**: Partial results when some sources fail

### Error Recovery
- **Exponential Backoff**: Progressive retry delays
- **Circuit Breaker Pattern**: Prevent cascade failures
- **Health Monitoring**: Continuous source availability checking

## Security Architecture

### API Security
- **Input Validation**: Comprehensive parameter sanitization
- **Rate Limiting**: Per-endpoint request limits
- **Error Handling**: Limited error information exposure

### Data Security
- **Environment Variables**: Secure API key management
- **Connection Security**: HTTPS for external API calls
- **Data Validation**: Input/output data integrity checks

## Monitoring and Observability

### Logging Strategy
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│      INFO       │    │     WARNING     │    │     ERROR       │
│                 │    │                 │    │                 │
│ • Operations    │    │ • Fallbacks     │    │ • Critical      │
│ • Success paths │    │ • Rate limits   │    │   failures      │
│ • Performance   │    │ • Degradation   │    │ • System errors │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Health Checks
- **Database Connectivity**: DuckDB connection status
- **External APIs**: Data source availability
- **Cache Systems**: Redis connectivity
- **Performance Metrics**: Response times and throughput

## Configuration Management

### Runtime Configuration
- **Symbol Selection**: Adjustable candidate pool sizes
- **Rate Limiting**: Configurable delays and batch sizes
- **Caching**: TTL and size limits
- **Data Sources**: Enable/disable specific providers

### Environment-based Configuration
- **Development**: Faster rates, smaller datasets
- **Production**: Conservative rates, full datasets
- **Testing**: Mock sources, isolated caches

## Future Enhancements

### Planned Features
1. **Real-time Streaming**: WebSocket support for live data
2. **Authentication**: JWT-based API authentication
3. **Analytics Dashboard**: Web-based monitoring interface
4. **Custom Indices**: User-defined index construction
5. **Machine Learning**: Predictive analytics integration

### Scalability Roadmap
1. **Microservices**: Split into dedicated services
2. **Message Queues**: Event-driven architecture
3. **Container Orchestration**: Kubernetes deployment
4. **Global CDN**: Geographic data distribution

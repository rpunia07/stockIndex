# Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the Stock Index Management Service in various environments.

## Prerequisites

### System Requirements
- **Python**: 3.8 or higher
- **Memory**: Minimum 4GB RAM (8GB recommended)
- **Storage**: 10GB available space
- **Network**: Internet connectivity for API access

### Required Services
- **Redis** (optional but recommended): For caching
- **Git**: For repository cloning

## Environment Setup

### 1. Environment Variables

Create a `.env` file in the project root:

```bash
# Required
ALPHA_VANTAGE_KEY=your_alpha_vantage_api_key_here

# Optional - Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379

# Optional - Database Configuration
DB_PATH=data/market_data.db

# Optional - Logging
LOG_LEVEL=INFO
```

### 2. API Key Setup

#### Alpha Vantage API Key
1. Visit [Alpha Vantage](https://www.alphavantage.co/support/#api-key)
2. Sign up for a free account
3. Copy your API key
4. Add to `.env` file: `ALPHA_VANTAGE_KEY=your_key_here`

## Installation Methods

### Method 1: Local Development

```bash
# Clone repository
git clone <repository-url>
cd assignment

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start the service
python -m app.main
```

### Method 2: Docker (Recommended for Production)

#### Using Docker Compose

1. **Create docker-compose.yml**:
```yaml
version: '3.8'

services:
  stock-service:
    build: .
    ports:
      - "8000:8000"
    environment:
      - ALPHA_VANTAGE_KEY=${ALPHA_VANTAGE_KEY}
      - REDIS_HOST=redis
      - REDIS_PORT=6379
    depends_on:
      - redis
    volumes:
      - ./data:/app/data

  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

volumes:
  redis_data:
```

2. **Deploy**:
```bash
# Build and start services
docker-compose up --build -d

# View logs
docker-compose logs -f stock-service

# Stop services
docker-compose down
```

#### Using Docker Only

```bash
# Build the image
docker build -t stock-index-service .

# Run with Redis
docker run -d --name redis redis:alpine
docker run -d \
  --name stock-service \
  --link redis:redis \
  -p 8000:8000 \
  -e ALPHA_VANTAGE_KEY=your_key_here \
  -e REDIS_HOST=redis \
  -v $(pwd)/data:/app/data \
  stock-index-service
```

### Method 3: Production Deployment

#### Using systemd (Linux)

1. **Create service file** `/etc/systemd/system/stock-index.service`:
```ini
[Unit]
Description=Stock Index Management Service
After=network.target

[Service]
Type=simple
User=stockservice
WorkingDirectory=/opt/stock-index-service
Environment=PATH=/opt/stock-index-service/.venv/bin
ExecStart=/opt/stock-index-service/.venv/bin/python -m app.main
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

2. **Deploy and start**:
```bash
# Enable and start service
sudo systemctl enable stock-index.service
sudo systemctl start stock-index.service

# Check status
sudo systemctl status stock-index.service

# View logs
sudo journalctl -u stock-index.service -f
```

## Nginx Reverse Proxy (Production)

### Nginx Configuration

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeout settings for long-running requests
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 300s;
    }

    # Health check endpoint
    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        access_log off;
    }
}
```

## Configuration

### Runtime Configuration

After deployment, configure the service:

```bash
# Set processing parameters
curl -X POST "http://localhost:8000/config/data-fetcher?candidate_symbols=200&max_symbols=100&batch_size=10"

# Test configuration
curl "http://localhost:8000/config/data-fetcher"
```

### Performance Tuning

#### Memory Settings
```bash
# For high-volume processing
export PYTHONMAXMEMORY=8G
```

#### Database Optimization
```python
# In database.py, adjust connection settings
conn = duckdb.connect(
    database=db_path,
    config={
        'memory_limit': '4GB',
        'threads': 4,
        'max_memory': '8GB'
    }
)
```

## Monitoring

### Health Checks

```bash
# Basic health check
curl http://localhost:8000/health

# Detailed system status
curl http://localhost:8000/config/data-fetcher
```

### Log Monitoring

```bash
# Follow application logs
tail -f /var/log/stock-index-service.log

# Using journalctl (systemd)
sudo journalctl -u stock-index.service -f

# Docker logs
docker-compose logs -f stock-service
```

### Performance Monitoring

```bash
# Monitor system resources
htop

# Monitor network connections
netstat -tulpn | grep :8000

# Monitor Redis (if used)
redis-cli monitor
```

## Backup and Recovery

### Data Backup

```bash
# Backup DuckDB database
cp data/market_data.db data/market_data.db.backup.$(date +%Y%m%d)

# Backup cache files
tar -czf cache_backup_$(date +%Y%m%d).tar.gz data/universe_cache.json data/market_cap_cache.json
```

### Recovery

```bash
# Restore database
cp data/market_data.db.backup.20250726 data/market_data.db

# Clear cache to force fresh data
rm -f data/universe_cache.json data/market_cap_cache.json

# Restart service
sudo systemctl restart stock-index.service
```

## Security

### Firewall Configuration

```bash
# Allow HTTP traffic
sudo ufw allow 8000/tcp

# Allow SSH (if needed)
sudo ufw allow 22/tcp

# Enable firewall
sudo ufw enable
```

### SSL/TLS (with Let's Encrypt)

```bash
# Install Certbot
sudo apt install certbot python3-certbot-nginx

# Obtain certificate
sudo certbot --nginx -d your-domain.com

# Auto-renewal (add to crontab)
0 12 * * * /usr/bin/certbot renew --quiet
```

## Troubleshooting

### Common Issues

#### 1. Service Won't Start
```bash
# Check logs
sudo journalctl -u stock-index.service -n 50

# Common fixes
# - Check environment variables
# - Verify Python path
# - Check file permissions
```

#### 2. API Key Issues
```bash
# Verify API key
curl "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey=YOUR_KEY"

# Check environment
echo $ALPHA_VANTAGE_KEY
```

#### 3. Database Connection Errors
```bash
# Check database file permissions
ls -la data/market_data.db

# Create data directory if missing
mkdir -p data
chmod 755 data
```

#### 4. Redis Connection Issues
```bash
# Test Redis connection
redis-cli ping

# Check Redis status
sudo systemctl status redis
```

### Performance Issues

#### High Memory Usage
```bash
# Monitor memory usage
free -h
ps aux | grep python

# Solutions:
# - Reduce batch_size
# - Increase cache_duration_days
# - Add memory limits
```

#### Slow API Responses
```bash
# Check rate limiting
curl "http://localhost:8000/config/data-fetcher"

# Solutions:
# - Increase batch_size (if APIs allow)
# - Reduce rate_limit_delay
# - Enable Redis caching
```

## Maintenance

### Regular Maintenance Tasks

#### Daily
- Monitor application logs
- Check system resources
- Verify API functionality

#### Weekly
- Review error logs
- Update cache if needed
- Check data accuracy

#### Monthly
- Backup database
- Review performance metrics
- Update dependencies (if needed)

### Updates and Upgrades

```bash
# Pull latest code
git pull origin main

# Update dependencies
pip install -r requirements.txt --upgrade

# Restart service
sudo systemctl restart stock-index.service
```

## Scaling

### Horizontal Scaling

For high-traffic deployments:

1. **Load Balancer Setup**:
```nginx
upstream stock_service {
    server 127.0.0.1:8000;
    server 127.0.0.1:8001;
    server 127.0.0.1:8002;
}

server {
    location / {
        proxy_pass http://stock_service;
    }
}
```

2. **Multiple Instances**:
```bash
# Start multiple instances
python -m app.main --port 8000 &
python -m app.main --port 8001 &
python -m app.main --port 8002 &
```

### Vertical Scaling

For data-intensive operations:

```python
# Increase processing capacity
data_fetcher.configure_settings(
    batch_size=20,  # Increase concurrent requests
    rate_limit_delay=5,  # Reduce delays (if APIs allow)
    candidate_symbols=500,  # Process more symbols
    max_symbols=200  # Larger index
)
```

## Support and Maintenance

### Log Analysis
```bash
# Find error patterns
grep -i error /var/log/stock-index-service.log | tail -20

# Monitor API success rates
grep "market cap" /var/log/stock-index-service.log | wc -l
```

### Performance Optimization
```bash
# Profile memory usage
python -m memory_profiler app/main.py

# Profile CPU usage
python -m cProfile app/main.py
```

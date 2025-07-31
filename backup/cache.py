from typing import Optional, Dict, Any, List
import json
from datetime import datetime, timedelta
import duckdb
import redis
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class CacheConfig:
    redis_host: str
    redis_port: int
    redis_db: int
    duckdb_path: str
    market_cap_ttl: int
    price_ttl: int

class DataCache:
    def __init__(self, config: CacheConfig):
        self.config = config
        self.redis = redis.Redis(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            decode_responses=True
        )
        self.db = duckdb.connect(config.duckdb_path)
        self._init_db()

    def _init_db(self):
        """Initialize DuckDB tables."""
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                symbol VARCHAR,
                date DATE,
                price DOUBLE,
                volume BIGINT,
                market_cap DOUBLE,
                source VARCHAR,
                is_estimated BOOLEAN,
                timestamp TIMESTAMP,
                PRIMARY KEY (symbol, date)
            )
        """)
        
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS data_quality (
                symbol VARCHAR,
                date DATE,
                field VARCHAR,
                source VARCHAR,
                is_estimated BOOLEAN,
                confidence_score DOUBLE,
                timestamp TIMESTAMP,
                PRIMARY KEY (symbol, date, field)
            )
        """)

    def get_cached_market_cap(self, symbol: str) -> Optional[float]:
        """Get market cap from Redis cache."""
        key = f"market_cap:{symbol}"
        data = self.redis.get(key)
        if data:
            return float(data)
        return None

    def set_cached_market_cap(self, symbol: str, value: float):
        """Cache market cap in Redis."""
        key = f"market_cap:{symbol}"
        self.redis.setex(key, self.config.market_cap_ttl, str(value))

    def get_cached_daily_data(self, symbol: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get daily data from DuckDB."""
        query = """
            SELECT symbol, date, price, volume, market_cap, source, is_estimated
            FROM market_data
            WHERE symbol = ? AND date BETWEEN ? AND ?
            ORDER BY date
        """
        result = self.db.execute(query, [symbol, start_date, end_date]).fetchall()
        return [dict(zip(['symbol', 'date', 'price', 'volume', 'market_cap', 'source', 'is_estimated'], row)) 
                for row in result]

    def save_daily_data(self, data: List[Dict[str, Any]]):
        """Save daily data to DuckDB."""
        if not data:
            return

        # Convert to tuple format for batch insert
        rows = [(
            d['symbol'],
            datetime.strptime(d['date'], '%Y-%m-%d') if isinstance(d['date'], str) else d['date'],
            d['price'],
            d['volume'],
            d.get('market_cap', 0),
            d.get('source', 'unknown'),
            d.get('is_estimated', False),
            datetime.now()
        ) for d in data]

        self.db.execute("""
            INSERT OR REPLACE INTO market_data (
                symbol, date, price, volume, market_cap, source, is_estimated, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

    def log_data_quality(self, symbol: str, date: datetime, field: str, source: str, 
                        is_estimated: bool, confidence_score: float):
        """Log data quality metrics."""
        self.db.execute("""
            INSERT OR REPLACE INTO data_quality (
                symbol, date, field, source, is_estimated, confidence_score, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [symbol, date, field, source, is_estimated, confidence_score, datetime.now()])

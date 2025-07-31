import duckdb
import pandas as pd
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = "market_data.db"):
        """Initialize DuckDB connection and create tables."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._create_tables()

    def _create_tables(self):
        """Create required database tables."""
        # Market data table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                date DATE,
                symbol VARCHAR,
                price DOUBLE,
                market_cap DOUBLE,
                volume BIGINT,
                PRIMARY KEY (date, symbol)
            )
        """)

        # Index performance table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_performance (
                date DATE PRIMARY KEY,
                daily_return DOUBLE,
                cumulative_return DOUBLE,
                total_market_cap DOUBLE
            )
        """)

    def insert_market_data(self, data: List[Dict[str, Any]]):
        """Insert market data into the database."""
        if not data:
            return
            
        try:
            df = pd.DataFrame(data)
            
            # Clean data
            df = df.dropna(subset=['date', 'symbol', 'price'])
            df['market_cap'] = df['market_cap'].fillna(0)
            df['volume'] = df['volume'].fillna(0)
            
            # Insert using DuckDB
            self.conn.execute("DELETE FROM market_data WHERE date IN (SELECT DISTINCT date FROM df)")
            self.conn.execute("INSERT INTO market_data SELECT * FROM df")
            
            logger.info(f"Inserted {len(df)} market data records")
            
        except Exception as e:
            logger.error(f"Error inserting market data: {str(e)}")

    def calculate_index_performance(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Calculate market cap weighted index performance."""
        try:
            query = """
            WITH daily_totals AS (
                SELECT 
                    date,
                    SUM(market_cap) as total_market_cap,
                    SUM(price * market_cap) / SUM(market_cap) as weighted_price
                FROM market_data 
                WHERE date BETWEEN ? AND ? 
                  AND market_cap > 0
                GROUP BY date
                ORDER BY date
            ),
            returns AS (
                SELECT 
                    date,
                    total_market_cap,
                    weighted_price,
                    LAG(weighted_price) OVER (ORDER BY date) as prev_price,
                    (weighted_price / LAG(weighted_price) OVER (ORDER BY date) - 1) as daily_return
                FROM daily_totals
            )
            SELECT 
                date,
                COALESCE(daily_return, 0) as daily_return,
                total_market_cap,
                EXP(SUM(LN(1 + COALESCE(daily_return, 0))) OVER (ORDER BY date)) - 1 as cumulative_return
            FROM returns
            ORDER BY date
            """
            
            result = self.conn.execute(query, [start_date, end_date]).fetchdf()
            return result
            
        except Exception as e:
            logger.error(f"Error calculating index performance: {str(e)}")
            return pd.DataFrame()

    def get_market_data(self, start_date: str, end_date: str, symbols: List[str] = None) -> pd.DataFrame:
        """Get market data for specified date range and symbols."""
        try:
            base_query = """
            SELECT date, symbol, price, market_cap, volume 
            FROM market_data 
            WHERE date BETWEEN ? AND ?
            """
            params = [start_date, end_date]
            
            if symbols:
                placeholders = ','.join(['?' for _ in symbols])
                base_query += f" AND symbol IN ({placeholders})"
                params.extend(symbols)
            
            base_query += " ORDER BY date, symbol"
            
            result = self.conn.execute(base_query, params).fetchdf()
            return result
            
        except Exception as e:
            logger.error(f"Error getting market data: {str(e)}")
            return pd.DataFrame()

    def get_top_companies(self, date: str, limit: int = 100) -> pd.DataFrame:
        """Get top companies by market cap for a specific date."""
        try:
            query = """
            SELECT symbol, price, market_cap, volume
            FROM market_data 
            WHERE date = ? AND market_cap > 0
            ORDER BY market_cap DESC
            LIMIT ?
            """
            
            result = self.conn.execute(query, [date, limit]).fetchdf()
            return result
            
        except Exception as e:
            logger.error(f"Error getting top companies: {str(e)}")
            return pd.DataFrame()

    def save_index_performance(self, performance_data: pd.DataFrame):
        """Save index performance data."""
        try:
            if performance_data.empty:
                return
                
            # Delete existing data for the date range
            dates = performance_data['date'].tolist()
            placeholders = ','.join(['?' for _ in dates])
            self.conn.execute(f"DELETE FROM index_performance WHERE date IN ({placeholders})", dates)
            
            # Insert new data
            self.conn.execute("INSERT INTO index_performance SELECT * FROM performance_data")
            
            logger.info(f"Saved {len(performance_data)} index performance records")
            
        except Exception as e:
            logger.error(f"Error saving index performance: {str(e)}")

    def get_index_performance(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Get index performance for date range."""
        try:
            query = """
            SELECT date, daily_return, cumulative_return, total_market_cap
            FROM index_performance 
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            """
            
            result = self.conn.execute(query, [start_date, end_date]).fetchdf()
            return result
            
        except Exception as e:
            logger.error(f"Error getting index performance: {str(e)}")
            return pd.DataFrame()

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

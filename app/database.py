import duckdb
import pandas as pd
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = "market_data_v2.db"):
        """Initialize DuckDB connection and create tables."""
        self.db_path = db_path
        self.conn = None
        
        # Try to connect to the specified database
        success = self._try_connect(db_path)
        
        if not success:
            # If primary database is locked, try to use the most recent fresh database
            fresh_dbs = self._find_fresh_databases()
            if fresh_dbs:
                logger.info(f"Primary database locked, trying most recent fresh database: {fresh_dbs[0]}")
                success = self._try_connect(fresh_dbs[0])
            
            # If still no success, create a new fresh database
            if not success:
                import time
                fresh_db_path = f"market_data_fresh_{int(time.time())}.db"
                logger.info(f"Creating new fresh database: {fresh_db_path}")
                success = self._try_connect(fresh_db_path)
                
        if not success:
            raise RuntimeError("Failed to create or connect to any database")
            
        self._create_tables()
    
    def _find_fresh_databases(self):
        """Find existing fresh databases sorted by creation time (newest first)."""
        import os
        import glob
        fresh_dbs = glob.glob("market_data_fresh_*.db")
        # Sort by modification time, newest first
        fresh_dbs.sort(key=os.path.getmtime, reverse=True)
        return fresh_dbs
    
    def _try_connect(self, db_path: str) -> bool:
        """Try to connect to a database file."""
        try:
            self.conn = duckdb.connect(db_path)
            self.db_path = db_path
            logger.info(f"Connected to database: {db_path}")
            return True
        except Exception as e:
            logger.warning(f"Could not connect to {db_path}: {e}")
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
                self.conn = None
            return False

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
            # Create DataFrame with explicit column ordering to prevent field swapping
            # First, ensure all data dictionaries have the correct structure
            cleaned_data = []
            for item in data:
                cleaned_item = {
                    'date': item.get('date'),
                    'symbol': item.get('symbol'),
                    'price': item.get('price'),
                    'market_cap': item.get('market_cap', 0),
                    'volume': item.get('volume', 0)
                }
                cleaned_data.append(cleaned_item)
            
            # Create DataFrame with explicit column order
            df = pd.DataFrame(cleaned_data, columns=['date', 'symbol', 'price', 'market_cap', 'volume'])
            
            # Clean data
            df = df.dropna(subset=['date', 'symbol', 'price'])
            df['market_cap'] = df['market_cap'].fillna(0)
            df['volume'] = df['volume'].fillna(0)
            
            # Ensure date column is in proper DATE format for DuckDB
            if 'date' in df.columns:
                if df['date'].dtype == 'object':
                    # If it's object type (string), convert to datetime first then to date
                    df['date'] = pd.to_datetime(df['date']).dt.date
                elif pd.api.types.is_datetime64_any_dtype(df['date']):
                    # If it's already datetime (including TIMESTAMP_NS), convert to date
                    df['date'] = df['date'].dt.date
            
            # Debug: Log the first row to verify correct field mapping
            if len(df) > 0:
                sample_row = df.iloc[0]
                logger.info(f"Sample row being inserted: date={sample_row['date']}, symbol={sample_row['symbol']}, price={sample_row['price']}, market_cap={sample_row['market_cap']}, volume={sample_row['volume']}")
                
                # Additional validation for large values
                if sample_row['market_cap'] > 1_000_000_000_000:  # > 1T
                    logger.info(f"✓ Market cap looks correct: ${sample_row['market_cap']:,.0f}")
                elif sample_row['volume'] > 1_000_000_000_000:  # > 1T  
                    logger.error(f"✗ FIELD SWAP DETECTED: volume={sample_row['volume']:,.0f} > market_cap={sample_row['market_cap']:,.0f}")
            
            # Insert using DuckDB with explicit column mapping to prevent field swapping
            self.conn.execute("DELETE FROM market_data WHERE date IN (SELECT DISTINCT date FROM df)")
            self.conn.execute("""
                INSERT INTO market_data (date, symbol, price, market_cap, volume) 
                SELECT date, symbol, price, market_cap, volume FROM df
            """)
            
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
                    SUM(price * market_cap) / NULLIF(SUM(market_cap), 0) as weighted_price
                FROM market_data 
                WHERE date BETWEEN ? AND ? 
                  AND market_cap > 0
                  AND price > 0
                GROUP BY date
                HAVING SUM(market_cap) > 0
                ORDER BY date
            ),
            returns AS (
                SELECT 
                    date,
                    total_market_cap,
                    weighted_price,
                    LAG(weighted_price) OVER (ORDER BY date) as prev_price,
                    CASE 
                        WHEN LAG(weighted_price) OVER (ORDER BY date) IS NULL THEN 0
                        WHEN LAG(weighted_price) OVER (ORDER BY date) = 0 THEN 0
                        ELSE (weighted_price / LAG(weighted_price) OVER (ORDER BY date) - 1)
                    END as daily_return
                FROM daily_totals
            ),
            cumulative_calc AS (
                SELECT 
                    date,
                    daily_return,
                    total_market_cap,
                    weighted_price,
                    ROW_NUMBER() OVER (ORDER BY date) as row_num
                FROM returns
            )
            SELECT 
                date,
                daily_return,
                total_market_cap,
                -- Calculate cumulative return: start at 0, then add daily returns progressively
                CASE 
                    WHEN row_num = 1 THEN 0  -- First day has cumulative return of 0
                    ELSE SUM(daily_return) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - daily_return
                END as cumulative_return
            FROM cumulative_calc
            ORDER BY date
            """
            
            result = self.conn.execute(query, [start_date, end_date]).fetchdf()
            
            # Ensure date column is in proper format for database insertion
            if not result.empty and 'date' in result.columns:
                # Convert date column to ensure it's in DATE format, not TIMESTAMP_NS
                if pd.api.types.is_datetime64_any_dtype(result['date']):
                    result['date'] = result['date'].dt.date
                elif result['date'].dtype == 'object':
                    # If it's string, convert through datetime to date
                    result['date'] = pd.to_datetime(result['date']).dt.date
            
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
            
            # Create a copy to avoid modifying the original
            df_copy = performance_data.copy()
            
            # Convert date column to proper DATE format for DuckDB
            if 'date' in df_copy.columns:
                # Handle different date formats that might come from pandas
                if df_copy['date'].dtype == 'object':
                    # If it's object type (string), convert to datetime first then to date
                    df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date
                elif pd.api.types.is_datetime64_any_dtype(df_copy['date']):
                    # If it's already datetime (including TIMESTAMP_NS), convert to date
                    df_copy['date'] = df_copy['date'].dt.date
                
            # Delete existing data for the date range
            dates = df_copy['date'].tolist()
            placeholders = ','.join(['?' for _ in dates])
            self.conn.execute(f"DELETE FROM index_performance WHERE date IN ({placeholders})", dates)
            
            # Insert new data using the cleaned dataframe
            self.conn.execute("INSERT INTO index_performance SELECT * FROM df_copy")
            
            logger.info(f"Saved {len(df_copy)} index performance records")
            
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
        """Close database connection properly."""
        if self.conn:
            try:
                self.conn.close()
                logger.info(f"Closed database connection: {self.db_path}")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")
            finally:
                self.conn = None
    
    def cleanup_old_fresh_databases(self, keep_count: int = 3):
        """Clean up old fresh database files, keeping only the most recent ones."""
        try:
            fresh_dbs = self._find_fresh_databases()
            if len(fresh_dbs) > keep_count:
                import os
                to_delete = fresh_dbs[keep_count:]  # Keep only the first keep_count
                for db_file in to_delete:
                    try:
                        if db_file != self.db_path:  # Don't delete the currently active database
                            os.remove(db_file)
                            logger.info(f"Cleaned up old database file: {db_file}")
                    except Exception as e:
                        logger.warning(f"Could not delete {db_file}: {e}")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")

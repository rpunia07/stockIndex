import duckdb
from pathlib import Path

class Database:
    def __init__(self, db_path: str = "market_data.db"):
        """Initialize the DuckDB connection and create tables if they don't exist."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._create_tables()

    def _create_tables(self):
        """Create the required database tables if they don't exist."""
        # Raw market data table
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

        # Index constituents table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_constituents (
                date DATE,
                symbol VARCHAR,
                weight DOUBLE,
                rank INT,
                PRIMARY KEY (date, symbol)
            )
        """)

        # Index performance table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_performance (
                date DATE PRIMARY KEY,
                daily_return DOUBLE,
                cumulative_return DOUBLE
            )
        """)

        # Index composition changes table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS composition_changes (
                date DATE,
                symbol VARCHAR,
                change_type VARCHAR,  -- 'ENTRY' or 'EXIT'
                PRIMARY KEY (date, symbol)
            )
        """)

    def insert_market_data(self, data: list[dict]):
        """Insert market data into the database."""
        if not data:
            return
            
        # Convert data to list of tuples for bulk insert
        values = [(
            d['date'],
            d['symbol'],
            d['price'],
            d['market_cap'],
            d['volume']
        ) for d in data]
        
        # First delete any existing data for these dates and symbols
        existing_data = [(d['date'], d['symbol']) for d in data]
        if existing_data:
            self.conn.execute("""
                DELETE FROM market_data 
                WHERE (date, symbol) IN (
                    SELECT UNNEST(?), UNNEST(?)
                )
            """, [
                [d[0] for d in existing_data],
                [d[1] for d in existing_data]
            ])
        
        # Then insert new data
        self.conn.execute("""
            INSERT INTO market_data (date, symbol, price, market_cap, volume)
            VALUES (?, ?, ?, ?, ?)
        """, values)

    def build_index(self, start_date: str, end_date: str):
        """Build the index for the given date range."""
        # First, clear existing constituents for this date range
        self.conn.execute("""
            DELETE FROM index_constituents 
            WHERE date BETWEEN ? AND ?
        """, [start_date, end_date])
        
        # Then identify top 100 stocks by market cap for each day
        self.conn.execute("""
            WITH ranked_stocks AS (
                SELECT 
                    date,
                    symbol,
                    market_cap,
                    ROW_NUMBER() OVER (PARTITION BY date ORDER BY market_cap DESC) as rank
                FROM market_data
                WHERE date BETWEEN ? AND ?
            )
            INSERT INTO index_constituents (date, symbol, weight, rank)
            SELECT 
                date,
                symbol,
                1.0/COUNT(*) OVER (PARTITION BY date) as weight,  -- Equal weighting
                rank
            FROM ranked_stocks
            WHERE rank <= 100
        """, [start_date, end_date])

        # Calculate daily returns
        self.conn.execute("""
            WITH daily_index_return AS (
                SELECT 
                    m.date,
                    SUM(ic.weight * 
                        (m.price / LAG(m.price) OVER (PARTITION BY m.symbol ORDER BY m.date) - 1)
                    ) as daily_return
                FROM index_constituents ic
                JOIN market_data m ON ic.date = m.date AND ic.symbol = m.symbol
                GROUP BY m.date
            )
            INSERT INTO index_performance (date, daily_return, cumulative_return)
            SELECT 
                date,
                daily_return,
                EXP(SUM(LN(1 + COALESCE(daily_return, 0))) OVER (ORDER BY date)) - 1 as cumulative_return
            FROM daily_index_return
        """)

    def track_composition_changes(self, date: str):
        """Track changes in index composition."""
        self.conn.execute("""
            WITH prev_constituents AS (
                SELECT symbol 
                FROM index_constituents 
                WHERE date = DATE(?) - 1
            ),
            curr_constituents AS (
                SELECT symbol 
                FROM index_constituents 
                WHERE date = ?
            )
            INSERT INTO composition_changes (date, symbol, change_type)
            SELECT 
                ?,
                symbol,
                'ENTRY' as change_type
            FROM curr_constituents 
            WHERE symbol NOT IN (SELECT symbol FROM prev_constituents)
            UNION ALL
            SELECT 
                ?,
                symbol,
                'EXIT' as change_type
            FROM prev_constituents 
            WHERE symbol NOT IN (SELECT symbol FROM curr_constituents)
        """, [date, date, date, date])

    def get_performance(self, start_date: str, end_date: str) -> list[dict]:
        """Retrieve index performance for a date range."""
        return self.conn.execute("""
            SELECT 
                date,
                daily_return,
                cumulative_return
            FROM index_performance
            WHERE date BETWEEN ? AND ?
            ORDER BY date
        """, [start_date, end_date]).fetchall()

    def get_composition(self, date: str) -> list[dict]:
        """Get index composition for a specific date."""
        return self.conn.execute("""
            SELECT 
                ic.symbol,
                ic.weight,
                m.price,
                m.market_cap
            FROM index_constituents ic
            JOIN market_data m ON ic.date = m.date AND ic.symbol = m.symbol
            WHERE ic.date = ?
            ORDER BY ic.rank
        """, [date]).fetchall()

    def get_composition_changes(self, start_date: str, end_date: str) -> list[dict]:
        """Get composition changes for a date range."""
        return self.conn.execute("""
            SELECT 
                date,
                symbol,
                change_type
            FROM composition_changes
            WHERE date BETWEEN ? AND ?
            ORDER BY date, change_type
        """, [start_date, end_date]).fetchall()

    def close(self):
        """Close the database connection."""
        self.conn.close()

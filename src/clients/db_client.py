import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import logging
from typing import Dict, Optional, List, Tuple

load_dotenv()
logger = logging.getLogger(__name__)


class DBClient:
    """
    Database client for Google Cloud SQL PostgreSQL.
    """
    
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        self.conn.autocommit = False
        logger.info("Connected to database")

    def get_instrument_id(self, ticker: str, exchange: str) -> Optional[int]:
        """
        Check if instrument exists and return its ID.
        Only returns active, non-delisted instruments.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT id FROM instruments 
                WHERE ticker = %s AND exchange = %s 
                AND is_active = TRUE AND delisted_at IS NULL""",
                (ticker, exchange)
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_timestamp_range(self, instrument_id: int) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """
        Get the earliest and latest timestamps for an instrument.
        
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT MIN(timestamp), MAX(timestamp) 
                FROM market_history 
                WHERE instrument_id = %s""",
                (instrument_id,)
            )
            result = cur.fetchone()
            
            # Check if we got results and they're not NULL
            if result and result[0] is not None:
                # Convert to UTC-aware pandas Timestamps for compatibility
                min_ts = pd.to_datetime(result[0], utc=True)
                max_ts = pd.to_datetime(result[1], utc=True)
                return (min_ts, max_ts)
            
            return (None, None)

    def insert_market_data(self, df: pd.DataFrame) -> int:
        """
        Bulk insert OHLC market data.
        Expects df with columns: instrument_id, timestamp, open, high, low, close, volume
        If ANY conflict occurs (duplicate timestamp), the ENTIRE insert is reverted.
        
        Returns: number of rows inserted (0 if conflict)
        """
        if df.empty:
            return 0
        
        # Efficient conversion using to_records (faster than iterrows)
        records = df[['instrument_id', 'timestamp', 'open', 'high', 'low', 'close', 'volume']].to_records(index=False).tolist()
        
        try:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """INSERT INTO market_history 
                    (instrument_id, timestamp, open, high, low, close, volume)
                    VALUES %s""",
                    records,
                    page_size=1000
                )
                self.conn.commit()
                logger.info(f"Inserted {len(records)} rows")
                return len(records)
                
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            logger.warning(f"Conflict detected, rolled back entire insert: {e}")
            return 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Insert failed: {e}")
            raise
        
    def get_instruments_by_exchange(self, exchange: str) -> Dict[str, int]:
        """
        Get all active instruments for a given exchange.
        Returns: Dict mapping ticker to instrument_id
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """SELECT ticker, id 
                    FROM instruments 
                    WHERE exchange = %s 
                    AND is_active = TRUE 
                    AND delisted_at IS NULL""",
                    (exchange,)
                )
                results = cur.fetchall()
                return {ticker: inst_id for ticker, inst_id in results}
        except Exception as e:
            logger.error(f"Failed to fetch instruments for {exchange}: {e}")
            return {}
    
    def get_market_data(
        self, 
        instrument_id: int, 
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Read OHLC market data for an instrument.
        Times should be ISO format strings or None.
        """
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM market_history
            WHERE instrument_id = %s
        """
        params = [instrument_id]
        
        if start_time:
            query += " AND timestamp >= %s"
            params.append(start_time)
        if end_time:
            query += " AND timestamp <= %s"
            params.append(end_time)
        
        query += " ORDER BY timestamp ASC"
        
        return pd.read_sql(query, self.conn, params=params)

    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """Execute custom SQL query and return DataFrame."""
        return pd.read_sql(sql, self.conn, params=params)

    def close(self):
        """Close database connection."""
        self.conn.close()
        logger.info("Database connection closed")
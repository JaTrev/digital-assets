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
        """Initialize with no connection (lazy initialization)"""
        self.conn = None
        self.cursor = None
    
    def __enter__(self):
        """Enter context manager - establish database connection"""
        self.conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()
        logger.info("Connected to database")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - commit/rollback and close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if exc_type is None:
                self.conn.commit()
                logger.info("Transaction committed")
            else:
                self.conn.rollback()
                logger.warning(f"Transaction rolled back due to: {exc_val}")
            self.conn.close()
            logger.info("Database connection closed")
        return False
    
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
    
    def insert_funding_data(self, df: pd.DataFrame) -> int:
        """
        Bulk insert funding rate data for perpetual futures.
        
        Expects df with columns: 
        - instrument_id (required)
        - timestamp (required)
        - At least ONE of: funding_rate_relative, funding_rate_absolute, or premium
        
        Exchange-specific patterns:
        - Hyperliquid: funding_rate_relative + premium (both fields, hourly)
        - dYdX: funding_rate_relative only (hourly rate)
        - Kraken (if supported): funding_rate_relative + funding_rate_absolute
        
        Column mapping:
        - funding_rate_relative: The actual funding rate charged (as %)
        - funding_rate_absolute: Funding in absolute USD per contract (rare)
        - premium: Premium component, mark-index spread (Hyperliquid specific)
        
        If ANY conflict occurs (duplicate timestamp), the ENTIRE insert is reverted.
        
        Returns: number of rows inserted (0 if conflict)
        """
        if df.empty:
            return 0
        
        # Convert DataFrame to list of tuples with native Python types
        records = []
        for _, row in df.iterrows():
            funding_rel = float(row['funding_rate_relative']) if 'funding_rate_relative' in df.columns and pd.notna(row.get('funding_rate_relative')) else None
            funding_abs = float(row['funding_rate_absolute']) if 'funding_rate_absolute' in df.columns and pd.notna(row.get('funding_rate_absolute')) else None
            premium = float(row['premium']) if 'premium' in df.columns and pd.notna(row.get('premium')) else None
            
            records.append((
                int(row['instrument_id']),
                row['timestamp'],
                funding_rel,
                funding_abs,
                premium
            ))
        
        try:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """INSERT INTO funding_history 
                    (instrument_id, timestamp, funding_rate_relative, funding_rate_absolute, premium)
                    VALUES %s""",
                    records,
                    page_size=1000
                )
                self.conn.commit()
                logger.info(f"Inserted {len(records)} funding rate rows")
                return len(records)
                
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            logger.warning(f"Funding rate conflict detected, rolled back entire insert: {e}")
            return 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Funding rate insert failed: {e}")
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
    
    def get_perpetuals(self, exchange: str) -> pd.DataFrame:
        """
        Get all active perpetual futures for a given exchange.
        
        Args:
            exchange: Exchange name (e.g., 'hyperliquid', 'kraken')
        
        Returns:
            DataFrame with columns: id, ticker, base_asset, quote_asset, 
            settle_asset, margin_mode, kind
        """
        return self.query(
            """SELECT id, ticker, base_asset, quote_asset, settle_asset, 
            margin_mode, kind
            FROM instruments 
            WHERE exchange = %s AND kind = 'perp' AND is_active = TRUE
            ORDER BY ticker""",
            (exchange,)
        )
    
    def get_perpetuals_dict(self, exchange: str) -> Dict[str, int]:
        """
        Get all active perpetual futures as a dictionary.
        
        Args:
            exchange: Exchange name (e.g., 'hyperliquid', 'kraken')
        
        Returns:
            Dict mapping ticker to instrument_id
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """SELECT ticker, id 
                    FROM instruments 
                    WHERE exchange = %s AND kind = 'perp' AND is_active = TRUE""",
                    (exchange,)
                )
                results = cur.fetchall()
                return {ticker: inst_id for ticker, inst_id in results}
        except Exception as e:
            logger.error(f"Failed to fetch perpetuals for {exchange}: {e}")
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
    
    def get_funding_timestamp_range(self, instrument_id):
        """
        Get min and max timestamp for existing funding data.
        
        Args:
            instrument_id: Instrument ID to check
            
        Returns:
            tuple: (min_timestamp, max_timestamp) or (None, None) if no data exists
        """
        query = """
            SELECT MIN(timestamp), MAX(timestamp)
            FROM funding_history
            WHERE instrument_id = %s
        """
        self.cursor.execute(query, (instrument_id,))
        result = self.cursor.fetchone()
        return result if result[0] is not None else (None, None)

    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """Execute custom SQL query and return DataFrame."""
        return pd.read_sql(sql, self.conn, params=params)

    def close(self):
        """Close database connection."""
        self.conn.close()
        logger.info("Database connection closed")
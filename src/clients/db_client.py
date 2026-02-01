import os
import io
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self):
        self.url = os.getenv("SUPABASE_DB_URL")
        if not self.url:
            raise EnvironmentError("SUPABASE_DB_URL not found in environment.")
        
        try:
            self.conn = psycopg2.connect(self.url)
            self.conn.autocommit = False # Better for transaction safety
        except Exception as e:
            logger.error(f"Failed to connect to Supabase: {e}")
            raise

    def write_df(self, df: pd.DataFrame, table_name: str):
        if df.empty:
            return
            
        # ensuring the buffer closed automatically
        with io.StringIO() as buffer:
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            try:
                with self.conn.cursor() as cursor:
                    cursor.copy_from(buffer, table_name, sep=',', columns=list(df.columns))
                self.conn.commit()
                logging.info(f"Successfully wrote {len(df)} rows to {table_name}")
            except Exception as e:
                self.conn.rollback()
                logging.error(f"Database write failed: {e}")
                raise

    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """Standard query method for Research/Backtesting."""
        return pd.read_sql(sql, self.conn, params=params)

    def close(self):
        self.conn.close()
import pandas as pd
from pathlib import Path
from typing import List
import logging

logger = logging.getLogger(__name__)


class KrakenCSVClient:
    """
    Client for loading and parsing Kraken OHLC CSV files.
    CSV columns: [UNIX Timestamp, Open, High, Low, Close, Volume, Count]
    """
    
    def __init__(self, data_path: str = "data"):
        self.data_path = Path(data_path)
        if not self.data_path.exists():
            raise FileNotFoundError(f"Data path not found: {self.data_path}")
    
    def load_csv_files(self, pattern: str = "*_60.csv") -> List[Path]:
        files = list(self.data_path.glob(pattern))
        logger.info(f"Found {len(files)} CSV files matching '{pattern}'")
        return sorted(files)
    
    def parse_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Parse Kraken OHLC CSV file.
        
        Format: [UNIX Timestamp, Open, High, Low, Close, Volume, Count]
        - Removes Count column (not needed)
        - Converts UNIX timestamp to UTC-aware datetime
        - Prices are in quote currency (e.g., USD for XBTUSD)
        - Volume is in base currency (e.g., BTC for XBTUSD)
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        try:
            df = pd.read_csv(file_path)
            
            # Rename columns to standard names
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'count']
            
            # Convert UNIX timestamp to UTC-aware datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
            
            # Drop count column
            df = df.drop(columns=['count'])
            
            logger.info(f"Parsed {len(df)} rows from {file_path.name}")
            return df
            
        except Exception as e:
            logger.error(f"Error parsing {file_path}: {e}")
            raise
    
    def get_ticker_from_filename(self, file_path: Path) -> str:
        return file_path.name.replace("_60.csv", "")

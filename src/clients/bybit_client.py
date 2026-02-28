import requests
import pandas as pd
import time
import logging
from typing import List, Optional
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class BybitClient:
    """
    Client for fetching historical OHLCV data from Bybit V5 API.
    
    API Documentation: https://bybit-exchange.github.io/docs/v5/market/kline
    """
    
    def __init__(
        self, 
        request_delay: float = 0.05,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize Bybit API client.
        
        Args:
            request_delay: Delay between requests in seconds (default: 0.05s = 20 req/s)
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Initial delay for exponential backoff (default: 1.0s)
        """
        self.base_url = "https://api.bybit.com/v5/market/kline" 
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay 
        
    def _interval_to_milliseconds(self, interval: str) -> int:
        """
        Convert Bybit interval string to milliseconds.
        """
        if interval == 'D':
            return 86400000  # 24 hours
        elif interval == 'W':
            return 604800000  # 7 days
        elif interval == 'M':
            return 2592000000  # 30 days (approximate)
        else:
            # Numeric intervals are in minutes
            return int(interval) * 60000
    
    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000
    ) -> List[dict]:
        """
        Fetch a single batch of klines from Bybit API.
        Returns list of dicts for easier processing.
        """
        params = {
            "category": "spot",
            "symbol": symbol,
            "interval": interval,
            "start": start_ms,
            "end": end_ms,
            "limit": limit
        }
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get("retCode") != 0:
                    logger.error(f"Bybit API error: {data.get('retMsg', 'Unknown error')}")
                    return []
                
                # Convert array format to dict format
                result = data.get("result", {})
                candles = result.get("list", [])
                
                # Convert to list of dicts for easier handling
                return [
                    {
                        'timestamp': c[0],
                        'open': c[1],
                        'high': c[2],
                        'low': c[3],
                        'close': c[4],
                        'volume': c[5]
                    }
                    for c in candles
                ]
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}), retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed after {self.max_retries} attempts: {e}")
                    return []
        
        return []
    
    def download_klines(
        self,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        Download all klines for a date range from Bybit API.
        Automatically handles pagination for date ranges > 1000 records.
        
        Note: Bybit returns data in REVERSE chronological order (newest first).
        """
        # Convert dates to millisecond timestamps
        start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        end_ms = int(end_dt.timestamp() * 1000) - 1  # Subtract 1ms to exclude midnight
        
        # Calculate interval in milliseconds
        interval_ms = self._interval_to_milliseconds(interval)
        print(f"Downloading {symbol} from {start_date} to {end_date}...")
        
        all_data = []
        current_end = end_ms  # Start from the END date (get newest data first)
        page = 0
        
        while current_end > start_ms:
            page += 1
            
            # Fetch batch (Bybit returns newest first)
            batch = self.fetch_klines(symbol, interval, start_ms, current_end)
            
            if not batch:
                break
            
            all_data.extend(batch)
            
            # Progress indicator
            if page % 10 == 0:
                print(f"Fetched {len(all_data)} candles (page {page})...")
            
            # Check if we got less than 1000 records (reached start of data)
            if len(batch) < 1000:
                break
            
            # Update end time to just BEFORE the oldest candle in this batch
            # Since Bybit returns reverse chronological, the LAST item is the OLDEST
            oldest_timestamp = int(batch[-1]['timestamp'])
            
            # Move backwards in time
            current_end = oldest_timestamp - interval_ms
            
            # Safety check: prevent infinite loop
            if current_end >= end_ms:
                print(f"Warning: Pagination not advancing (stuck at {current_end})")
                break
            
            time.sleep(self.request_delay)
        
        if not all_data:
            print("  ✗ No data available")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='ms', utc=True)
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        
        # Sort by timestamp ascending (convert from reverse to chronological)
        df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp']).reset_index(drop=True)
        
        print(f"Downloaded {len(df)} candles ({page} API calls)")
        return df


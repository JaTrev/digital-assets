import requests
import pandas as pd
import time
from typing import Optional, List
from datetime import datetime, timezone, timedelta


class OKXClient:
    """
    Client for fetching historical OHLCV data from OKX public API.
    """
    
    def __init__(
        self,
        base_url: str = "https://www.okx.com/api/v5/market/history-candles",
        request_delay: float = 0.2,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize OKX API client.
        
        Args:
            base_url: OKX API endpoint
            request_delay: Delay between requests in seconds (default: 0.2s = 5 req/sec)
            max_retries: Maximum retry attempts for failed requests
            retry_delay: Initial retry delay in seconds (exponential backoff)
        """
        self.base_url = base_url
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.batch_size = 100  # OKX API limit
    
    def fetch_candles(
        self,
        inst_id: str,
        bar: str,
        after: Optional[int] = None,
        limit: int = 100
    ) -> Optional[List[list]]:
        """
        Fetch candles from OKX API with retry logic.
        
        Args:
            inst_id: Instrument ID (e.g., 'BTC-USDT')
            bar: Bar interval (e.g., '1H')
            after: Pagination timestamp in milliseconds (optional)
            limit: Number of candles to fetch (max 100)
        
        Returns:
            List of candle data or None on failure
        """
        params = {
            "instId": inst_id,
            "bar": bar,
            "limit": limit
        }
        
        if after:
            params["after"] = after
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Check for API errors
                if data.get("code") != "0":
                    print(f"API error: {data.get('msg', 'Unknown error')}")
                    return None
                
                return data.get("data", [])
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    print(
                        f"Request failed (attempt {attempt + 1}/{self.max_retries}), "
                        f"retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    print(f"Failed after {self.max_retries} attempts: {e}")
                    return None
        
        return None
    
    def download_klines(
        self,
        inst_id: str,
        bar: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        Download all klines for a date range from OKX API.
        
        Uses pagination to fetch all data. OKX returns candles in reverse
        chronological order (newest first).
        
        Args:
            inst_id: Instrument ID (e.g., 'BTC-USDT')
            bar: Bar interval (e.g., '1H')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            DataFrame with OHLCV data
        """
        # Convert dates to timestamps (milliseconds)
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        end_ts = int(end_dt.timestamp() * 1000) - 1  # Subtract 1ms to exclude next day's midnight
        
        print(f"Downloading {inst_id} from {start_date} to {end_date}...")
        
        all_candles = []
        after = None  # Start with most recent data
        page = 0
        
        while True:
            page += 1
            
            # Fetch batch
            candles = self.fetch_candles(inst_id, bar, after=after, limit=self.batch_size)
            
            if not candles:
                break
            
            # Filter candles within date range
            valid_candles = [
                c for c in candles
                if start_ts <= int(c[0]) <= end_ts
            ]
            
            all_candles.extend(valid_candles)
            
            # Check if we've reached the start date
            oldest_ts = int(candles[-1][0])
            if oldest_ts <= start_ts:
                break
            
            # Update pagination cursor (timestamp of oldest candle)
            after = oldest_ts
            
            # Progress indicator
            if page % 10 == 0:
                print(f"Fetched {len(all_candles)} candles (page {page})...")
            
            # Rate limiting
            time.sleep(self.request_delay)
        
        if not all_candles:
            print("No data available")
            return pd.DataFrame()
        
        # Convert to DataFrame
        # OKX format: [timestamp, open, high, low, close, volume, volume_currency, volume_quote, confirm]
        df = pd.DataFrame(all_candles, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'volume_currency', 'volume_quote', 'confirm'
        ])
        
        # Convert timestamp from milliseconds to UTC datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='ms', utc=True)
        
        # Select and convert relevant columns
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        
        # Sort by timestamp ascending
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        print(f"Downloaded {len(df)} candles ({page} API calls)")
        return df
    

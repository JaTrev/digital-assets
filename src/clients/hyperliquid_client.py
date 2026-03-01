import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from time import sleep
from typing import Optional, List


class HyperliquidClient:
    """
    Client for DEX Hyperliquid perpetual futures API.
    
    API Documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
    """
    
    def __init__(
        self,
        base_url: str = "https://api.hyperliquid.xyz/info",
        request_delay: float = 0.2,  # 5 req/s conservative
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self.base_url = base_url
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def _make_request(self, payload: dict) -> dict:
        """Make HTTP POST request with retry logic."""
        for attempt in range(self.max_retries):
            try:
                response = requests.post(self.base_url, json=payload, timeout=30)
                response.raise_for_status()
                sleep(self.request_delay)
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    print(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                    print(f"  Retrying in {wait_time}s...")
                    sleep(wait_time)
                else:
                    print(f"Request failed after {self.max_retries} attempts: {e}")
                    raise
    
    def get_available_perpetuals(self) -> List[dict]:
        """
        Get list of all available perpetual contracts.
        
        Returns:
            List of perpetual contract metadata
        """
        payload = {"type": "meta"}
        data = self._make_request(payload)
        return data.get('universe', [])
    
    def fetch_candles(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int
    ) -> Optional[pd.DataFrame]:
        """
        Fetch OHLC candle data for a perpetual contract.
        
        Args:
            symbol: Coin symbol (e.g., 'BTC', 'ETH')
            interval: Candle interval ('1m', '5m', '15m', '1h', '4h', '1d')
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
        
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_ms,
                "endTime": end_ms
            }
        }
        
        try:
            data = self._make_request(payload)
            
            if not data or not isinstance(data, list):
                print(f"✗ No candle data returned for {symbol}")
                return None
            
            if len(data) == 0:
                print(f"✗ Empty candle data for {symbol}")
                return None
            
            # Parse candles into DataFrame
            df = pd.DataFrame(data)
            
            # Rename columns: t=time, o=open, h=high, l=low, c=close, v=volume
            df = df.rename(columns={
                't': 'timestamp',
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume'
            })
            
            # Convert timestamp from milliseconds to UTC datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            
            # Select only needed columns
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            
            return df
            
        except Exception as e:
            print(f"✗ Error fetching candles for {symbol}: {e}")
            return None
    
    def download_ohlc(
        self,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        Download complete OHLC history for a date range.
        
        Hyperliquid returns max ~500-1000 candles per request depending on interval.
        We'll chunk by 30 days for hourly data.
        
        Args:
            symbol: Coin symbol (e.g., 'BTC')
            interval: Candle interval ('1h', '4h', '1d', etc.)
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
        
        Returns:
            DataFrame with all OHLC data
        """
        print(f"Downloading {symbol} {interval} from {start_date} to {end_date}")
        
        # Convert dates to timestamps (milliseconds)
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        end_dt = end_dt - timedelta(milliseconds=1)
        
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        
        all_data = []
        
        # Chunk by 30 days for hourly data
        chunk_size_ms = 30 * 24 * 60 * 60 * 1000
        current_start = start_ms
        
        while current_start < end_ms:
            chunk_end = min(current_start + chunk_size_ms, end_ms)
            
            chunk_start_dt = datetime.fromtimestamp(current_start / 1000, tz=timezone.utc)
            chunk_end_dt = datetime.fromtimestamp(chunk_end / 1000, tz=timezone.utc)
            
            print(f"Fetching {chunk_start_dt.date()} to {chunk_end_dt.date()}...")
            
            df_chunk = self.fetch_candles(symbol, interval, current_start, chunk_end)
            
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)
                print(f"Got {len(df_chunk)} candles")
            else:
                print(f"No data for this chunk")
            
            current_start = chunk_end + 1
        
        if not all_data:
            print(f"No data collected for {symbol}")
            return pd.DataFrame()
        
        # Combine all chunks
        df_combined = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates and sort
        df_combined = df_combined.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        
        print(f"Downloaded {len(df_combined)} total candles for {symbol}")
        print(f"Date range: {df_combined['timestamp'].min()} to {df_combined['timestamp'].max()}")
        
        return df_combined
    
    def fetch_funding_rates(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical funding rates for a perpetual contract.
        
        Args:
            symbol: Coin symbol (e.g., 'BTC')
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
        
        Returns:
            DataFrame with columns: timestamp, funding_rate_relative, premium
        """
        payload = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_ms,
            "endTime": end_ms
        }
        
        try:
            data = self._make_request(payload)
            
            if not data or not isinstance(data, list):
                print(f"✗ No funding rate data returned for {symbol}")
                return None
            
            if len(data) == 0:
                print(f"✗ Empty funding rates for {symbol}")
                return None
            
            # Parse into DataFrame
            df = pd.DataFrame(data)
            
            # Hyperliquid returns: coin, fundingRate, premium, time
            df = df.rename(columns={
                'time': 'timestamp',
                'fundingRate': 'funding_rate_relative',
                'premium': 'premium'
            })
            
            # Convert timestamp from milliseconds to UTC datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            
            # Select only needed columns
            df = df[['timestamp', 'funding_rate_relative', 'premium']]
            
            return df
            
        except Exception as e:
            print(f"✗ Error fetching funding rates for {symbol}: {e}")
            return None
    
    def download_funding_rates(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        Download complete funding rate history for a date range.
        
        Hyperliquid funding rates are published hourly.
        Timestamp represents when the rate was published/became active.
        
        Args:
            symbol: Coin symbol (e.g., 'BTC')
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
        
        Returns:
            DataFrame with all funding rate data
        """
        print(f"Downloading funding rates for {symbol} from {start_date} to {end_date}")
        
        # Convert dates to timestamps (milliseconds)
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        end_dt = end_dt - timedelta(milliseconds=1)
        
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        
        all_data = []
        
        # Hyperliquid returns max 500 funding rates per request
        # At hourly rate, that's ~20 days, so chunk by 20 days
        chunk_size_ms = 20 * 24 * 60 * 60 * 1000
        current_start = start_ms
        
        while current_start < end_ms:
            chunk_end = min(current_start + chunk_size_ms, end_ms)
            
            chunk_start_dt = datetime.fromtimestamp(current_start / 1000, tz=timezone.utc)
            chunk_end_dt = datetime.fromtimestamp(chunk_end / 1000, tz=timezone.utc)
            
            print(f"Fetching {chunk_start_dt.date()} to {chunk_end_dt.date()}...")
            
            df_chunk = self.fetch_funding_rates(symbol, current_start, chunk_end)
            
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)
                print(f"Got {len(df_chunk)} funding rate records")
            else:
                print(f"No funding rate data for this chunk")
            
            current_start = chunk_end + 1
        
        if not all_data:
            print(f"No funding rate data collected for {symbol}")
            return pd.DataFrame()
        
        # Combine all chunks
        df_combined = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates and sort
        df_combined = df_combined.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        
        print(f"Downloaded {len(df_combined)} funding rate records for {symbol}")
        print(f"Date range: {df_combined['timestamp'].min()} to {df_combined['timestamp'].max()}")
        
        return df_combined

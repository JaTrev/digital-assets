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
        request_delay: float = 0.05,
        max_retries: int = 5,
        retry_delay: float = 1.0
    ):
        self.base_url = base_url
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
    
    def _ms_to_str(self, ms: int) -> str:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    
    def _make_request(self, payload: dict) -> dict:
        """Make HTTP POST request with retry logic."""
        for attempt in range(self.max_retries):
            try:
                response = self.session.post(self.base_url, json=payload, timeout=30)
                
                response_body = response.text
                
                if not response.ok:
                    status = response.status_code
                    if status == 429:
                        # Handle rate limit response
                        wait_time = self.retry_delay * (2 ** attempt) * 2
                        print(f"Rate limit hit on attempt {attempt + 1}/{self.max_retries}: {response_body} ")
                        sleep(wait_time)
                        continue
                    
                    response.raise_for_status()
                    
                sleep(self.request_delay)
                return response.json()
            
            except requests.exceptions.RequestException as e:
                response_body = getattr(e.response, 'text', str(e))
                
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    print(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                    print(f"Exception:{e}")
                    print(f"Body:{response_body}")
                    print(f"Retrying in {wait_time}s...")
                    sleep(wait_time)
                else:
                    print(f"Request failed after {self.max_retries} attempts: ")
                    print(f"Exception:{e}")
                    print(f"Body:{response_body}")
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
        except Exception as e:
            print(f"Failed to fetch candles for {symbol} ({interval}) from {self._ms_to_str(start_ms)} to {self._ms_to_str(end_ms)}: {e}")
            return None
        
        if not data or not isinstance(data, list) or len(data) == 0:
            print(f"No candle data returned for {symbol} ({interval}) from {self._ms_to_str(start_ms)} to {self._ms_to_str(end_ms)}")
            return None
        
        df = pd.DataFrame(data)
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

        # Cast OHLCV columns to float (API returns them as strings)
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)

        # Select only needed columns
        return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        
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
        except Exception as e:
            print(f"Failed to fetch funding rates for {symbol} from {self._ms_to_str(start_ms)} to {self._ms_to_str(end_ms)}: {e}")
            return None
        
        if not data or not isinstance(data, list) or len(data) == 0:
            print(f"No funding rate data returned for {symbol} from {self._ms_to_str(start_ms)} to {self._ms_to_str(end_ms)}")
            return None
        
        df = pd.DataFrame(data)
                
        # Hyperliquid returns: coin, fundingRate, premium, time
        df = df.rename(columns={'time': 'timestamp', 'fundingRate': 'funding_rate_relative', 'premium': 'premium'})
                
        # Convert timestamp from milliseconds to UTC datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df["symbol"] = symbol
                        
        return df[['symbol', 'timestamp', 'funding_rate_relative', 'premium']]
        
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
        Uses dynamic chunk sizing based on interval and tracks latest timestamp to avoid gaps.
        
        Args:
            symbol: Coin symbol (e.g., 'BTC')
            interval: Candle interval ('1m', '5m', '15m', '1h', '4h', '1d', etc.)
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
        
        # Dynamically set chunk size based on interval
        # API typically returns 500-1000 candles per request
        interval_to_chunk_days = {
            '1m': 1,      # 1 day = 1440 candles
            '5m': 3,      # 3 days = 864 candles
            '15m': 10,    # 10 days = 960 candles
            '1h': 30,     # 30 days = 720 candles
            '4h': 90,     # 90 days = 540 candles
            '1d': 365,    # 365 days = 365 candles
        }
        
        chunk_days = interval_to_chunk_days.get(interval, 30)
        chunk_size_ms = chunk_days * 24 * 60 * 60 * 1000
        
        all_data = []
        current_start = start_ms
        
        while current_start < end_ms:
            chunk_end = min(current_start + chunk_size_ms, end_ms)
            
            df_chunk = self.fetch_candles(symbol, interval, current_start, chunk_end)
            
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)
                latest_timestamp = df_chunk['timestamp'].max()
                latest_timestamp_ms = int(latest_timestamp.timestamp() * 1000)
                
                new_start = latest_timestamp_ms + 1
                if new_start <= current_start:
                    # No progress made – API returned data at or before current position.
                    # Skip to end of chunk.
                    current_start = chunk_end + 1
                else:
                    current_start = new_start
            else:
                # If no data, move to next chunk period
                current_start = chunk_end + 1
        
        if not all_data:
            print(f"No data collected for {symbol}")
            return pd.DataFrame()
        
        # Combine all chunks
        df_combined = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates and sort
        df_combined = df_combined.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        
        print(f"Downloaded {len(df_combined)} candles: {df_combined['timestamp'].min()} to {df_combined['timestamp'].max()}")
        
        return df_combined
    
    
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
            
        Note:
            - There is no (1-hour) gap between announcement and payment, the rate is effective immediately at the timestamp provided.
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
            df_chunk = self.fetch_funding_rates(symbol, current_start, chunk_end)
            
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)
                
                latest_ms = int(df_chunk['timestamp'].max().timestamp() * 1000)
                new_start = latest_ms + 1
                current_start = new_start if new_start > current_start else chunk_end + 1
            else:
                current_start = chunk_end + 1
        
        if not all_data:
            print(f"No funding rate data collected for {symbol}")
            return pd.DataFrame(columns=['symbol', 'timestamp', 'funding_rate_relative', 'premium'])
        
        # Combine all chunks
        df_combined = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates and sort
        df_combined = df_combined.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        
        print(f"Downloaded {len(df_combined)} funding rate records for {symbol}")
        print(f"Date range: {df_combined['timestamp'].min()} to {df_combined['timestamp'].max()}")
        
        return df_combined

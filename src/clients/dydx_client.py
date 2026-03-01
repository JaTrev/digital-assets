"""
dYdX v4 API Client

Fetches OHLC and funding rate data from dYdX v4 chain (Cosmos-based).
All timestamps are in UTC.

API Documentation: https://docs.dydx.xyz/indexer-client/http
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import requests
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, List, Dict


class DydxClient:
    """
    Client for interacting with dYdX v4 Indexer API.
    """
    
    def __init__(self, base_url: str = "https://indexer.dydx.trade/v4"):
        """
        Initialize dYdX client.
        
        Args:
            base_url: Base URL for dYdX Indexer API (default: mainnet)
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        print(f"Initialized dYdX client with base URL: {base_url}")
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make GET request to dYdX API."""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {url}: {e}")
            raise
    
    def get_available_perpetuals(self) -> List[Dict]:
        """
        Get all available perpetual markets.
        
        Returns:
            List of market dictionaries with ticker, volume, open interest, etc.
        """
        print("Fetching available perpetual markets")
        data = self._make_request("perpetualMarkets")
        
        markets = []
        for ticker, market in data.get('markets', {}).items():
            markets.append({
                'ticker': ticker,
                'base_asset': market.get('baseAsset', ''),
                'quote_asset': market.get('quoteAsset', 'USD'),
                'status': market.get('status', ''),
                'volume_24h': float(market.get('volume24H', 0)),
                'open_interest': float(market.get('openInterestUSDC', 0)) / 1e6,
                'oracle_price': float(market.get('oraclePrice', 0)),
            })
        
        print(f"Found {len(markets)} perpetual markets")
        return markets
    
    def download_ohlc(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        interval: str = '1h',
    ) -> pd.DataFrame:
        """
        Download OHLC data for a symbol.
        Automatically paginates to fetch all data between start_date and end_date.
        
        Args:
            symbol: Market ticker (e.g., 'BTC-USD')
            interval: Candle resolution ('1m', '5m', '15m', '30m', '1h', '4h', '1d')
            start_date: Start date (ISO format or 'YYYY-MM-DD')
            end_date: End date (ISO format or 'YYYY-MM-DD')
            limit: Ignored (kept for compatibility)
            
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        # Map interval formats
        interval_map = {
            '1m': '1MIN',
            '5m': '5MINS',
            '15m': '15MINS',
            '30m': '30MINS',
            '1h': '1HOUR',
            '4h': '4HOURS',
            '1d': '1DAY'
        }
        
        resolution = interval_map.get(interval, '1HOUR')
        
        # Build params
        params = {'resolution': resolution}
        
        start_date = f"{start_date}T00:00:00.000Z"
        params['fromISO'] = start_date
        
        end_date = f"{end_date}T23:59:59.999Z"
        params['toISO'] = end_date
        
        # dYdX returns max ~1000 candles per request, need pagination for historical data
        all_candles = []
        endpoint = f"candles/perpetualMarkets/{symbol}"
        current_end = params.get('toISO')
        
        # Handle timezone for start_date comparison
        if start_date:
            start_ts = pd.to_datetime(start_date)

        else:
            start_ts = None
        
        print(f"Downloading OHLC for {symbol} ({resolution})")
        
        # Pagination loop: walk backwards from end_date to start_date
        while True:
            # Update end date for this batch
            if current_end:
                params['toISO'] = current_end
            
            data = self._make_request(endpoint, params)
            candles = data.get('candles', [])
            
            if not candles:
                break
            
            all_candles.extend(candles)
            
            # Get oldest timestamp from this batch
            oldest_ts = pd.to_datetime(min(c['startedAt'] for c in candles))
            
            # Stop if we've reached the start_date
            if start_ts and oldest_ts <= start_ts:
                break
            
            # If we got fewer candles than expected, we've reached the end of available data
            if len(candles) < 1000:
                break
            
            # Set next batch end to just before the oldest timestamp we got
            current_end = (oldest_ts - pd.Timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        if not all_candles:
            print(f"No OHLC data available for {symbol}")
            return pd.DataFrame()
        
        # Parse all candles
        df = pd.DataFrame(all_candles)
        df['timestamp'] = pd.to_datetime(df['startedAt'])
        
        # Convert to numeric types
        df['open'] = pd.to_numeric(df['open'])
        df['high'] = pd.to_numeric(df['high'])
        df['low'] = pd.to_numeric(df['low'])
        df['close'] = pd.to_numeric(df['close'])
        df['volume'] = pd.to_numeric(df['baseTokenVolume'])
        
        # Select and order columns
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
        
        # Filter to requested date range
        if start_ts:
            df = df[df['timestamp'] >= start_ts]
        
        print(f"Downloaded {len(df)} OHLC records for {symbol}")
        return df
    
    def download_funding_rates(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Download historical funding rates for a symbol.
        Automatically paginates to fetch all data between start_date and end_date.
        
        Args:
            symbol: Market ticker (e.g., 'BTC-USD')
            start_date: Start date (ISO format or 'YYYY-MM-DD')
            end_date: End date (ISO format or 'YYYY-MM-DD')
            limit: Ignored (kept for compatibility)
            
        Returns:
            DataFrame with columns: timestamp, funding_rate_relative
            
        Note:
            - Rate is expressed as a percentage for 1 hour (e.g., 0.01 = 0.01% per hour)
            - Pagination walks backwards from end_date to start_date automatically
            - Timestamps normalized to hour boundaries (XX:00:00)
        """
        # Convert dates to timezone-aware timestamps
        start_ts = pd.to_datetime(start_date).tz_localize('UTC')
        end_ts = pd.to_datetime(end_date).tz_localize('UTC')
    
        all_funding = []
        current_end = end_ts
    
        print(f"Downloading funding rates for {symbol}")
    
        while True:
            params = {
                'effectiveBeforeOrAt': current_end.isoformat()
            }
            
            endpoint = f"historicalFunding/{symbol}"
            data = self._make_request(endpoint, params)
            
            funding_records = data.get('historicalFunding', [])
            
            if not funding_records:
                print(f"  No more funding data available")
                break
            
            all_funding.extend(funding_records)
            print(f"  Fetched {len(funding_records)} funding records (total: {len(all_funding)})")
            
            oldest_ts = pd.to_datetime(min(r['effectiveAt'] for r in funding_records))
            
            if oldest_ts <= start_ts:
                break
            
            if len(funding_records) < 100:
                print(f"  Reached end of available data")
                break
            
            current_end = oldest_ts - pd.Timedelta(seconds=1)
    
        if not all_funding:
            print(f"No funding data available for {symbol}")
            return pd.DataFrame()
    
        df = pd.DataFrame(all_funding)
        df['timestamp'] = pd.to_datetime(df['effectiveAt']).dt.floor('h')
        df['funding_rate_relative'] = pd.to_numeric(df['rate'])
    
        df = df[['timestamp', 'funding_rate_relative']]
        df = df.drop_duplicates(subset=['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)
    
        df = df[(df['timestamp'] >= start_ts) & (df['timestamp'] <= end_ts)]
    
        print(f"Final dataset: {len(df)} funding records from {df['timestamp'].min()} to {df['timestamp'].max()}")
        return df


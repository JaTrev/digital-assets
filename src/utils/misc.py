import pandas as pd

def resample_ohlcv(df_1m: pd.DataFrame, rule: str) -> pd.DataFrame:
    resampled = (
        df_1m.resample(rule, on="timestamp", closed="left", label="left")
        .agg({
            "symbol": "first", "open": "first", "high": "max", 
            "low": "min", "close": "last", "volume": "sum"
        })
        .dropna().reset_index()
    )

    if resampled.empty:
        return resampled

    # Logic: If the 'next' candle should have started by now, 
    # then the 'current' last candle is definitely complete.
    last_bar_start = resampled["timestamp"].iloc[-1]
    next_bar_expected_start = last_bar_start + pd.Timedelta(rule)
    
    # Check the actual wall-clock end of your data
    data_end_time = df_1m["timestamp"].max()

    # If our data hasn't even reached the start time of the NEXT potential candle,
    # the current one is still 'open' and subject to change.
    if data_end_time < next_bar_expected_start:
        return resampled.iloc[:-1]
    
    return resampled

def ema(series: pd.Series, span: int) -> pd.Series:
    """Calculate Exponential Moving Average (EMA) for a given series."""
    return series.ewm(span=span, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> float:
    """Calculate Relative Strength Index (RSI) for a given series."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    rs = gain.ewm(com=period - 1, adjust=False).mean() / loss.ewm(com=period - 1, adjust=False).mean()
    return float((100 - 100 / (1 + rs)).iloc[-1])

def slow_stoch(df: pd.DataFrame, k_period: int = 14, d_period: int = 3, slow_k: int = 3) -> tuple[float, float]:
    """Calculate Stochastic Oscillator slow %K and %D for a given DataFrame."""
    low14  = df["low"].rolling(k_period).min()
    high14 = df["high"].rolling(k_period).max()
    raw_k  = (df["close"] - low14) / (high14 - low14) * 100
    sk     = raw_k.rolling(slow_k).mean()   # slow K = 3-bar SMA of raw %K
    k      = float(sk.iloc[-1])
    d      = float(sk.rolling(d_period).mean().iloc[-1])
    return k, d

def slow_stoch_series(df: pd.DataFrame, k_period: int = 14, slow_k: int = 3, d_period: int = 3) -> tuple[pd.Series, pd.Series]:
    """Return full slow-%K and %D series."""
    low_n  = df["low"].rolling(k_period).min()
    high_n = df["high"].rolling(k_period).max()
    raw_k  = (df["close"] - low_n) / (high_n - low_n) * 100
    sk     = raw_k.rolling(slow_k).mean()
    d      = sk.rolling(d_period).mean()
    return sk, d

def atr(df: pd.DataFrame, period: int = 14) -> float:
    """Calculate Average True Range (ATR) for a given series."""
    # True Range = max(H-L, |H-Cprev|, |L-Cprev|), then EMA-smoothed
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return float(tr.ewm(span=period, adjust=False).mean().iloc[-1])

def ema_slope_pct(ema_series: pd.Series, lookback: int = 4) -> float:
    """% change of EMA7 over the last `lookback` bars."""
    return float((ema_series.iloc[-1] - ema_series.iloc[-lookback]) / ema_series.iloc[-lookback] * 100)


def hl_liquidation_price(symbol: str, notional: float, leverage: float, trade_price: float, direction: str) -> float:
    """Calculates the Hypliquid-specific liquidation price for a given entry price, leverage, and direction."""
    assert direction in ("long", "short"), "Direction must be 'long' or 'short'"
    
    MMR_CONFIG = {
        "BTC": 0.0125,          # 40x max -> (1/40 * 0.5)
        "ETH": 0.02,            # 25x max -> (1/25 * 0.5)
        "SOL": 0.025,           # 20x max -> (1/20 * 0.5)
        "XRP": 0.025,           # 20x max
        "HYPE": 0.05,           # 10x max -> (1/10 * 0.5)
        "DOGE": 0.05,           # 10x max
        "AAVE": 0.05,           # 10x max
        "xyz:SP500": 0.01,      # 50x max -> (1/50 * 0.5)
        "xyz:GOLD": 0.02,       # 25x max
        "xyz:SILVER": 0.02,     # 25x max
        "xyz:CL": 0.025,        # 20x max
        "xyz:BRENTOIL": 0.025,  # 20x max
        "DEFAULT": 0.025        # Safe fallback for unknown alts
    }
    
    maintenance_margin_rate = MMR_CONFIG.get(symbol, MMR_CONFIG.get("DEFAULT", 0.005))
    maintenance_margin = notional * maintenance_margin_rate
    initial_margin = notional / leverage
    qty = notional / trade_price
    side_multiplier = 1 if direction == "long" else -1
    margin_available = initial_margin - maintenance_margin
        
    liquidation_price = trade_price - side_multiplier * (margin_available / qty) / (1 - maintenance_margin_rate * side_multiplier)

    return liquidation_price
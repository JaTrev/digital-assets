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
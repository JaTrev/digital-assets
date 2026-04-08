import pandas as pd

def resample_ohlcv(df_1m: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample a 1m OHLCV DataFrame to any pandas offset rule (e.g. '15min')."""
    return (
        df_1m.resample(rule, on="timestamp", closed="left", label="right")
        .agg(
            symbol=("symbol", "first"), 
            open=("open","first"), 
            high=("high","max"), 
            low=("low","min"), 
            close=("close","last"), 
            volume=("volume","sum")
        )
        .dropna()
        .reset_index()
    )
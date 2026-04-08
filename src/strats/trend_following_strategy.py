from __future__ import annotations

import numpy as np
import pandas as pd
from models.backtesting_models import Direction, TradeSignal

class TrendFollowingStrategy:
    """
    Enhanced 15m Pullback strategy.
    Enters on retracements to the EMA during confirmed HTF trends.
    """

    def __init__(
        self,
        *,
        timeframe: str = "15m",
        htf_timeframe: str = "1h",
        ema_fast_len: int = 20,
        ema_slow_len: int = 50,
        htf_ema_len: int = 100,
        min_volume_ratio: float = 1.5,  # Higher bar for conviction
        atr_len: int = 14,
        cooldown_bars: int = 5,
    ):
        self.timeframe = timeframe
        self.htf_timeframe = htf_timeframe
        self.ema_fast_len = ema_fast_len
        self.ema_slow_len = ema_slow_len
        self.htf_ema_len = htf_ema_len
        self.min_volume_ratio = min_volume_ratio
        self.atr_len = atr_len
        self.cooldown_bars = cooldown_bars

    def _compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        
        # 1. EMAs & Slope
        # We look back 3 bars to see if the EMA is actually "pointing" up/down
        out["ema_fast"] = out["close"].ewm(span=self.ema_fast_len, adjust=False).mean()
        out["ema_slow"] = out["close"].ewm(span=self.ema_slow_len, adjust=False).mean()
        out["ema_slope"] = out["ema_fast"].diff(3) 
        
        # 2. ATR (Volatility)
        high_low = out["high"] - out["low"]
        high_cp = (out["high"] - out["close"].shift()).abs()
        low_cp = (out["low"] - out["close"].shift()).abs()
        tr = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1)
        out["atr"] = tr.rolling(self.atr_len).mean()

        # 3. ADX (Trend Strength Filter)
        # Standard ADX calculation: measures the strength of the move
        plus_dm = out["high"].diff().clip(lower=0)
        minus_dm = -out["low"].diff().clip(lower=0)
        
        atr_smooth = tr.rolling(self.atr_len).mean()
        plus_di = 100 * (plus_dm.rolling(self.atr_len).mean() / atr_smooth)
        minus_di = 100 * (minus_dm.rolling(self.atr_len).mean() / atr_smooth)
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        out["adx"] = dx.rolling(self.atr_len).mean()
        
        # 4. Volume Ratio (Crucial for Perp DEX conviction)
        out["vol_sma"] = out["volume"].rolling(20).mean()
        out["volume_ratio"] = out["volume"] / out["vol_sma"]
        
        return out
    
    def _calculate_conviction(self, row: pd.Series, direction: Direction) -> float:
        """
        Calculates a score between 0.0 and 1.0 based on trend quality.
        """
        # A. ADX Component (Weight: 40%)
        # Map ADX 25-50 to 0.0-1.0
        adx_score = np.clip((row["adx"] - 25) / 25, 0, 1)

        # B. Volume Component (Weight: 30%)
        # Map Volume Ratio 1.0-2.5 to 0.0-1.0
        vol_score = np.clip((row["volume_ratio"] - 1.0) / 1.5, 0, 1)

        # C. EMA Stack Component (Weight: 30%)
        # Check if EMAs are fanned out in the right order
        if direction == Direction.LONG:
            is_stacked = row["ema_fast"] > row["ema_slow"]
        else:
            is_stacked = row["ema_fast"] < row["ema_slow"]
        
        stack_score = 1.0 if is_stacked else 0.5

        # Weighted Average
        total_conviction = (adx_score * 0.4) + (vol_score * 0.3) + (stack_score * 0.3)
        return float(round(total_conviction, 2))

    def generate_trade_signals(self, market_bars: dict[str, dict[str, pd.DataFrame]]) -> list[TradeSignal]:
        signals: list[TradeSignal] = []
        buffer_pct = 0.001 
        adx_threshold = 25  # Standard: >25 means a strong trend is present

        for symbol, bars in market_bars.items():
            df_ltf = self._resolve_timeframe(bars, self.timeframe)
            df_htf = self._resolve_timeframe(bars, self.htf_timeframe)
            if df_ltf is None or df_htf is None: continue

            # HTF Logic (Keeping the 1h EMA 100 but checking its slope too)
            df_htf["htf_ema"] = df_htf["close"].ewm(span=self.htf_ema_len).mean()
            df_htf["htf_slope"] = df_htf["htf_ema"].diff(2)
            
            # Map HTF trend & slope to LTF
            df_htf_resampled = df_htf.set_index("timestamp")[["htf_ema", "htf_slope"]].reindex(df_ltf["timestamp"], method="ffill")
            df_ltf["htf_bullish"] = df_ltf["close"] > df_htf_resampled["htf_ema"].values
            df_ltf["htf_moving"] = df_htf_resampled["htf_slope"].values != 0 # Basic check

            work = self._compute_features(df_ltf)
            warmup = 50 
            cooldown_count = 0

            for i in range(warmup, len(work)):
                row = work.iloc[i]
                prev_row = work.iloc[i-1]
                
                if cooldown_count > 0:
                    cooldown_count -= 1
                    continue

                # --- THE CHOP FILTERS ---
                is_trending = row["adx"] > adx_threshold
                slope_up = row["ema_slope"] > 0
                slope_down = row["ema_slope"] < 0

                entry = float(row["close"])
                ema_f = float(row["ema_fast"])

                # LONG: HTF Trend + ADX Strength + Upward Slope + Pullback
                long_trigger = (
                    row["htf_bullish"] and is_trending and slope_up
                    and prev_row["low"] <= ema_f * (1 + buffer_pct)
                    and entry > ema_f
                )

                if long_trigger:
                    signals.append(
                        TradeSignal(
                            symbol=symbol, 
                            timestamp=pd.to_datetime(row["timestamp"], utc=True),
                            direction=Direction.LONG, 
                            conviction=self._calculate_conviction(row, Direction.LONG),
                            entry=entry, 
                            metadata={"type": "anti_chop_pullback", "adx": row["adx"], "atr": row["atr"]}
                        )
                        )
                    cooldown_count = self.cooldown_bars

                # SHORT: HTF Trend + ADX Strength + Downward Slope + Pullback
                short_trigger = (
                    not row["htf_bullish"] and is_trending and slope_down
                    and prev_row["high"] >= ema_f * (1 - buffer_pct)
                    and entry < ema_f
                )

                if short_trigger:
                    signals.append(
                        TradeSignal(
                            symbol=symbol, 
                            timestamp=pd.to_datetime(row["timestamp"], utc=True),
                            direction=Direction.SHORT, 
                            conviction=self._calculate_conviction(row, Direction.SHORT),
                            entry=entry,
                            metadata={"type": "anti_chop_pullback", "adx": row["adx"], "atr": row["atr"]}
                    ))
                    cooldown_count = self.cooldown_bars

        return sorted(signals, key=lambda s: (s.timestamp, s.symbol))

    def _resolve_timeframe(self, bars: dict, tf: str) -> pd.DataFrame | None:
        if tf in bars:
            df = bars[tf].copy()
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df.sort_values("timestamp")
        return None
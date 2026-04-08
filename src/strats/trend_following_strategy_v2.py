from __future__ import annotations
import numpy as np
import pandas as pd
from models.backtesting_models import Direction, TradeSignal

class TrendFollowingStrategy:
    def __init__(
        self,
        *,
        timeframe: str = "15m",
        htf_timeframe: str = "1h",
        ema_fast_len: int = 20,
        ema_slow_len: int = 50,
        atr_len: int = 14,
        cooldown_bars: int = 6,
    ):
        self.timeframe = timeframe
        self.htf_timeframe = htf_timeframe
        self.ema_fast_len = ema_fast_len
        self.ema_slow_len = ema_slow_len
        self.atr_len = atr_len
        self.cooldown_bars = cooldown_bars

    def _compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        # 1. EMAs & Basis
        out["ema_fast"] = out["close"].ewm(span=self.ema_fast_len, adjust=False).mean()
        out["ema_slow"] = out["close"].ewm(span=self.ema_slow_len, adjust=False).mean()
        
        # 2. Volatility & Relative Strength
        tr = pd.concat([
            out["high"] - out["low"],
            (out["high"] - out["close"].shift(1)).abs(),
            (out["low"] - out["close"].shift(1)).abs()
        ], axis=1).max(axis=1)
        out["atr"] = tr.rolling(self.atr_len).mean()
        
        # Pullback Quality: Is the current volatility lower than the trend volatility?
        out["vol_efficiency"] = out["atr"] / out["atr"].rolling(50).mean()

        # 3. Momentum (ADX)
        plus_dm = out["high"].diff().clip(lower=0)
        minus_dm = (-out["low"].diff()).clip(lower=0)
        plus_di = 100 * (plus_dm.rolling(self.atr_len).mean() / out["atr"])
        minus_di = 100 * (minus_dm.rolling(self.atr_len).mean() / out["atr"])
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)) * 100
        out["adx"] = dx.rolling(self.atr_len).mean()

        # 4. Volume Confirmation (RVOL)
        out["rvol"] = out["volume"] / out["volume"].rolling(30).mean()

        # 5. Distance from Mean (The "Rubber Band")
        # How many ATRs is price away from the slow EMA?
        out["dist_ema"] = (out["close"] - out["ema_slow"]).abs() / out["atr"]

        return out

    def _calculate_dynamic_conviction(self, row: pd.Series) -> float:
        """
        Calculates a conviction score [0.1 - 1.0] based on 3 pillars.
        No forward looking: uses current row (which is the close of the signal bar).
        """
        # Pillar 1: Trend Strength (ADX) - Higher is better up to 50
        trend_score = np.clip(row["adx"] / 50.0, 0, 1)

        # Pillar 2: Volume (RVOL) - We want to see 'Effort vs Result'
        vol_score = np.clip(row["rvol"] / 2.5, 0, 1)

        # Pillar 3: Mean Reversion Risk (Penalty for being too far)
        # If we are > 3 ATRs away from the EMA, we are 'overextended'
        dist_penalty = np.clip(1.0 - (row["dist_ema"] / 4.0), 0, 1)

        # Weighted calculation
        score = (trend_score * 0.4) + (vol_score * 0.3) + (dist_penalty * 0.3)
        return float(round(np.clip(score, 0.1, 1.0), 2))

    def generate_trade_signals(self, market_bars: dict) -> list[TradeSignal]:
        signals = []

        for symbol, bars in market_bars.items():
            df_ltf = self._resolve_timeframe(bars, self.timeframe)
            df_htf = self._resolve_timeframe(bars, self.htf_timeframe)
            if df_ltf is None or df_htf is None: continue

            # HTF Baseline (Price vs 100 EMA on 1H)
            df_htf["htf_ema"] = df_htf["close"].ewm(span=100, adjust=False).mean()
            htf_data = df_htf.set_index("timestamp")[["htf_ema"]].reindex(df_ltf["timestamp"], method="ffill")
            df_ltf["htf_ema"] = htf_data["htf_ema"].values

            work = self._compute_features(df_ltf)
            cooldown = 0

            for i in range(50, len(work)):
                if cooldown > 0:
                    cooldown -= 1
                    continue

                row = work.iloc[i]
                prev = work.iloc[i-1]
                
                # --- ENTRY FILTERS ---
                # A. HTF Alignment
                is_bullish_regime = row["close"] > row["htf_ema"]
                
                # B. The Pullback Setup
                # Did the previous candle dip into the 'Value Zone' (Fast EMA)?
                did_pullback = prev["low"] < prev["ema_fast"]
                
                # C. The Confirmation (The Trigger)
                # 1. Price closes back above Fast EMA
                # 2. Bullish candle (Close > Open)
                # 3. Volatility isn't exploding (vol_efficiency < 1.2) - prevents catching wicks
                is_valid_bounce = (
                    row["close"] > row["ema_fast"] and 
                    row["close"] > row["open"] and
                    row["vol_efficiency"] < 1.3
                )

                if is_bullish_regime and did_pullback and is_valid_bounce and row["adx"] > 20:
                    conviction = self._calculate_dynamic_conviction(row)
                    
                    # Only take trades with at least moderate conviction
                    if conviction > 0.45:
                        signals.append(TradeSignal(
                            symbol=symbol,
                            timestamp=pd.to_datetime(row["timestamp"], utc=True),
                            direction=Direction.LONG,
                            conviction=conviction,
                            entry=float(row["close"]),
                            metadata={"rvol": row["rvol"], "dist": row["dist_ema"], "atr": row["atr"]}
                        ))
                        cooldown = self.cooldown_bars

                # --- SHORT LOGIC ---
                elif (row["close"] < row["htf_ema"]) and (prev["high"] > prev["ema_fast"]) and \
                     (row["close"] < row["ema_fast"]) and (row["close"] < row["open"]) and \
                     (row["vol_efficiency"] < 1.3) and (row["adx"] > 20):
                    
                    conviction = self._calculate_dynamic_conviction(row)
                    if conviction > 0.45:
                        signals.append(TradeSignal(
                            symbol=symbol,
                            timestamp=pd.to_datetime(row["timestamp"], utc=True),
                            direction=Direction.SHORT,
                            conviction=conviction,
                            entry=float(row["close"]),
                            metadata={"rvol": row["rvol"], "dist": row["dist_ema"], "atr": row["atr"]}
                        ))
                        cooldown = self.cooldown_bars

        return signals

    def _resolve_timeframe(self, bars, tf):
        if tf not in bars: return None
        return bars[tf].sort_values("timestamp").reset_index(drop=True)
from __future__ import annotations
import copy
from turtle import setup
import numpy as np
import pandas as pd
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, List
from models.backtesting_models import TradeSignal, PendingSetup, Direction

class ICTStrategy:
    def __init__(
        self,
        *,
        mss_expiry_mins: int = 45,      # Time to wait for a 1m MSS after a 15m sweep
        ote_expiry_mins: int = 90,      # Time to wait for a retracement after MSS
        retracement_min: float = 0.62,   # OTE Start
        retracement_max: float = 0.79,   # OTE End
        funding_threshold: float = 0.0001, # 0.01% hourly
        min_conviction_score: float = 0.6 # Minimum score for a sweep to be considered
    ):
        self.mss_expiry_mins = mss_expiry_mins
        self.ote_expiry_mins = ote_expiry_mins
        self.retracement_min = retracement_min
        self.retracement_max = retracement_max
        self.funding_threshold = funding_threshold
        self.min_conviction_score = min_conviction_score
        
        # State: symbol -> PendingSetup
        self.active_setups: Dict[str, PendingSetup] = {}


    def _session_label(self, ts: pd.Timestamp) -> str:
        h = ts.hour
        if 0 <= h < 8:      return "asia" # Asia:   00:00 – 08:00 UTC
        elif 8 <= h < 13:   return "london" # London: 08:00 – 14:00 UTC
        elif 13 <= h < 21:  return "ny" # NY:     13:00 – 21:00 UTC
        else:               return "after_hours"
        
        
    def _get_last_settled_funding(self, ts: pd.Timestamp, funding_df: pd.DataFrame) -> float:
        """Ensures zero look-ahead bias by only seeing rates settled strictly before now."""
        past_funding = funding_df[funding_df["timestamp"] < ts]
        return float(past_funding.iloc[-1]["funding_rate_relative"])
    
    def _find_mss_level(
        self,
        direction: Direction,
        sweep_ts: pd.Timestamp,
        df_1m: pd.DataFrame,
        look_back_bars: int = 60  # 60 x 1m = last hour of 1m structure
    ) -> Optional[float]:
        
        pre_sweep = df_1m[df_1m["timestamp"] < sweep_ts].tail(look_back_bars)
        
        if direction == Direction.LONG:
            valid_swings = pre_sweep[pre_sweep["is_swing_high"]]
            
            if not valid_swings.empty:
                last_swing_idx = valid_swings.index[-1]
                return float(df_1m.loc[last_swing_idx - 1, "high"])
            
        elif direction == Direction.SHORT:
            valid_swings = pre_sweep[pre_sweep["is_swing_low"]]
            if not valid_swings.empty:
                last_swing_idx = valid_swings.index[-1]
                return float(df_1m.loc[last_swing_idx - 1, "low"])
        return None
    
    def _get_fvg(self, df_1m: pd.DataFrame, idx: int, ltf_atr: float, direction: Direction) -> Optional[dict]:
        """
        Checks the 3-candle sequence ending at 'idx' for an imbalance.
        Prev2 (Left), Prev1 (Middle), Curr (Right)
        """
        if idx < 2: return None
        
        prev2 = df_1m.iloc[idx-2] # Candle 1
        prev1 = df_1m.iloc[idx-1] # Candle 2 (The Displacement Candle)
        curr  = df_1m.iloc[idx]   # Candle 3
        
        if (prev1["high"] - prev1["low"]) < (ltf_atr * 2):
            return None
        
        # Bullish FVG: Candle 3 Low > Candle 1 High 
        if direction == Direction.LONG and curr["low"] > prev2["high"]:
            return {"type": "bullish", "top": curr["low"], "bottom": prev2["high"]}
        
        # Bearish FVG: Candle 3 High < Candle 1 Low
        if direction == Direction.SHORT and curr["high"] < prev2["low"]:
            return {"type": "bearish", "top": prev2["low"], "bottom": curr["high"]}
            
        return None
    
    def _prepare_htf_context(self, df_15m: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates HTF Liquidity Pools (PDH, PDL, hourly rolling extremes).
        """
        df_temp = df_15m.copy().sort_values("timestamp").reset_index(drop=True)
        df_temp["_date"] = df_temp["timestamp"].dt.date
        #df_temp["_week"] = df_temp["timestamp"].dt.to_period("W")
        look_back = 96 # 15m bars so 24h look-back
        
        # Daily levels
        daily_extremes = df_temp.groupby("_date").agg({"high": "max", "low": "min"}).rename(columns={"high": "daily_high", "low": "daily_low"})
        df_temp["pdh"] = df_temp["_date"].map(daily_extremes["daily_high"].shift(1))
        df_temp["pdl"] = df_temp["_date"].map(daily_extremes["daily_low"].shift(1))
        
        block_intervals = {
            "30min": "30min",
            "1h":    "1h",
            "12h":   "12h",
            "24h":   "24h",
            "48h":   "48h",
        }
        for label, freq in block_intervals.items():
            col = f"_{label}_blk"
            df_temp[col] = df_temp["timestamp"].dt.floor(freq)
            blocks = df_temp.groupby(col).agg(b_hi=("high", "max"), b_lo=("low", "min"))
            df_temp[f"prev_{label}_hi"] = df_temp[col].map(blocks["b_hi"].shift(1))
            df_temp[f"prev_{label}_lo"] = df_temp[col].map(blocks["b_lo"].shift(1))
            
        # Session levels
        #df_temp["_session_label"] = df_temp["timestamp"].apply(self._session_label)
        #df_temp["_session_key"] = df_temp["_date"].astype(str) + "_" + df_temp["_session_label"]
        #sessions = (
        #    df_temp.groupby("_session_key")
        #    .agg(
        #        s_hi=("high", "max"), 
        #        s_lo=("low", "min"),
        #        start_ts=("timestamp", "min")
        #    )
        #    .sort_values("start_ts")
        #)
        #df_temp["prev_session_hi"] = df_temp["_session_key"].map(sessions["s_hi"].shift(1))
        #df_temp["prev_session_lo"] = df_temp["_session_key"].map(sessions["s_lo"].shift(1))
        
        # Midnight open
        #midnight_bars = df_temp[df_temp["timestamp"].dt.hour == 0].groupby("_date").first()
        #df_temp["midnight_open"] = df_temp["_date"].map(midnight_bars["open"])  # 00:00 UTC open
        
        # The code is a Python comment that mentions the NY Open time in UTC. It is not performing any
        # specific functionality in terms of code execution, but it is providing information about the
        # NY Open time.
        # The above code is a Python script with comments. It appears to be indicating that the NY
        # Open occurs at 13:00 UTC. The code itself does not contain any executable statements, only
        # comments.
        # NY Open (13:00 UTC)
        #ny_open_bars = df_temp[df_temp["timestamp"].dt.hour == 13].groupby("_date").first()
        #df_temp["ny_open"] = df_temp["_date"].map(ny_open_bars["open"])
        
        # Weekly open
        #weekly_open_bars = df_temp.groupby("_week").first()
        #df_temp["weekly_open"] = df_temp["_week"].map(weekly_open_bars["open"].shift(1))
        
        # Equal highs/lows within the last 24h (96 bars) - with a tolerance to account for minor differences
        tol = 0.0005  # 0.05% tolerance for considering levels as "equal"   
        df_temp["_rolling_hi"] = df_temp["high"].rolling(look_back).max()
        df_temp["eq_hi"] = np.where(
            (df_temp["_rolling_hi"] - df_temp["_rolling_hi"].shift(look_back // 2)).abs() / df_temp["_rolling_hi"] < tol,
            df_temp["_rolling_hi"],
            np.nan
        )
        
        df_temp["_rolling_lo"] = df_temp["low"].rolling(look_back).min()
        df_temp["eq_lo"] = np.where(
            (df_temp["_rolling_lo"] - df_temp["_rolling_lo"].shift(look_back // 2)).abs() / df_temp["_rolling_lo"] < tol,
            df_temp["_rolling_lo"],
            np.nan
        )
            
        # ATR for reliability scoring
        prev_close = df_temp["close"].shift(1)
        tr = pd.concat([
                (df_temp["high"] - df_temp["low"]), 
                (df_temp["high"] - prev_close).abs(), 
                (df_temp["low"] - prev_close).abs()], 
            axis=1).max(axis=1)
        df_temp["atr"] = tr.rolling(look_back).mean()

        return df_temp.ffill()


    def _check_for_sweeps(self, htf_row: pd.Series, ltf_bar: pd.Series) -> Optional[list]:
        """
        Detects if the 15m candle swept a major level with high reliability.
        The sweeps need to be detected the moment 1 1m candle penetrates the HTF level.
        """
        levels = [
            # (name,                   price_level,               side,   weight)
            ("pdh",                    htf_row["pdh"],             "high", 1.0),
            ("pdl",                    htf_row["pdl"],             "low",  1.0),
            ("prev_30min_hi",          htf_row["prev_30min_hi"],    "high", 0.7),
            ("prev_30min_lo",          htf_row["prev_30min_lo"],    "low",  0.7),
            ("prev_1h_hi",             htf_row["prev_1h_hi"],      "high", 0.8),
            ("prev_1h_lo",             htf_row["prev_1h_lo"],      "low",  0.8),
            #("prev_4h_hi",             htf_row["prev_4h_hi"],      "high", 0.8),
            #("prev_4h_lo",             htf_row["prev_4h_lo"],      "low",  0.8),
            ("prev_12h_hi",            htf_row["prev_12h_hi"],     "high", 0.8),
            ("prev_12h_lo",            htf_row["prev_12h_lo"],     "low",  0.8),
            ("prev_24h_hi",            htf_row["prev_24h_hi"],     "high", 0.8),
            ("prev_24h_lo",            htf_row["prev_24h_lo"],     "low",  0.8),
            ("prev_48h_hi",            htf_row["prev_48h_hi"],     "high", 0.8),
            ("prev_48h_lo",            htf_row["prev_48h_lo"],     "low",  0.8),
            #("prev_session_hi",        htf_row["prev_session_hi"], "high", 0.9),
            #("prev_session_lo",        htf_row["prev_session_lo"], "low",  0.9),
            #("midnight_open_high",     htf_row["midnight_open"],   "high", 0.7),
            #("midnight_open_low",      htf_row["midnight_open"],   "low",  0.7),
            #("ny_open_high",           htf_row["ny_open"],         "high", 0.7),
            #("ny_open_low",            htf_row["ny_open"],         "low",  0.7),
            ("eq_hi",                  htf_row["eq_hi"],           "high", 1.2),  # Equal highs = clean liquidity
            ("eq_lo",                  htf_row["eq_lo"],           "low",  1.2),
        ]
        atr = htf_row["atr"]
        if pd.isna(atr) or atr == 0: return None
        
        # best_sweep = None
        current_low = ltf_bar["low"]
        current_high = ltf_bar["high"]
        current_close = ltf_bar["close"]
        sweeps = []        
        for name, price_level, side, weight in levels:
            if pd.isna(price_level): 
                continue
                
            swept = False
            if side == "high" and current_high > price_level and current_close < price_level:
                swept = True
                direction = Direction.SHORT
                wick_excess = current_high - price_level
                reclaim = price_level - current_close
            elif side == "low" and current_low < price_level and current_close > price_level:
                swept = True
                direction = Direction.LONG
                wick_excess = price_level - current_low
                reclaim = current_close - price_level
            else:
                continue
                
            if swept:
                quality = (0.5 * min(wick_excess / atr, 1.0) + 0.5 * min(reclaim / atr, 1.0))
                score = round(min(quality, 1.0), 3)
                assert np.isnan(score) == False, f"Score should not be NaN: {wick_excess=}, {reclaim=}, {atr=}"
                #if best_sweep is None or score > best_sweep["score"]:
                sweep = {"name": name, "dir": direction, "score": score, "price": price_level}
                sweeps.append(sweep)
        
        return sweeps

    # --- 3. Main Signal Loop ---
    def generate_trade_signals(
        self, 
        symbol: str, 
        df_1m: pd.DataFrame, 
        df_15m: pd.DataFrame, 
        funding_df: pd.DataFrame
    ) -> List[TradeSignal]:
        
        signals = []
        htf = self._prepare_htf_context(df_15m)
        
        # Pre-calculate 1m swing points for MSS detection
        df_1m = df_1m.copy().sort_values("timestamp").reset_index(drop=True)
        df_1m['is_swing_high'] = (df_1m['high'].shift(1) > df_1m['high'].shift(2)) & (df_1m['high'].shift(1) > df_1m['high'])
        df_1m['is_swing_low'] = (df_1m['low'].shift(1) < df_1m['low'].shift(2)) & (df_1m['low'].shift(1) < df_1m['low'])
        df_1m['rolling_avg_volume'] = df_1m['volume'].astype(float).rolling(15, min_periods=15).mean()  # 20m rolling average volume for potential use in scoring
        
        prev_close = df_1m["close"].shift(1)
        tr = pd.concat([
                (df_1m["high"] - df_1m["low"]), 
                (df_1m["high"] - prev_close).abs(), 
                (df_1m["low"] - prev_close).abs()], 
            axis=1).max(axis=1)
        df_1m["atr"] = tr.rolling(14).mean()

        for i in range(len(df_1m)):
            ltf_bar = df_1m.iloc[i]
            ts = ltf_bar["timestamp"]
            current_htf = htf[htf["timestamp"] < ts]
            if not current_htf.empty:
                htf_bar = current_htf.iloc[-1]

            # STEP A: Check for new Sweeps
            # is_in_window = (55 > int(ts.minute) >= 40) or ((ts.minute) <= 20)
            # if (ts.minute % 15 == 0) and is_in_window:   
            if True: # Always check for sweeps on every 1m bar, the sweep detection logic will ensure we only get valid sweeps at the right times
                sweeps = self._check_for_sweeps(htf_bar, ltf_bar) 
                if not sweeps:
                    continue
                for sweep in sweeps:
                    mss_level = self._find_mss_level(
                        direction=sweep["dir"],
                        sweep_ts=ts,
                        df_1m=df_1m, # use the 1m dataframe with swing points for MSS level detection
                    )
                    if mss_level is not None:
                        if self.active_setups.get(symbol) is None:
                            self.active_setups[symbol] = []
                        
                        self.active_setups[symbol].append(
                            PendingSetup(
                                name=sweep["name"],
                                direction=sweep["dir"],
                                sweep_timestamp=ts,
                                sweep_price=sweep["price"],
                                reliability_score=sweep["score"],
                                high_at_sweep=ltf_bar["high"],
                                low_at_sweep=ltf_bar["low"],
                                mss_level=mss_level
                            )
                        )

            # STEP B: Manage Active Setup State Machine
            setups = self.active_setups.get(symbol, [])    
            still_active = []
            for setup in setups:
                # Skip any further checks if we're still on the sweep bar (can't have MSS or OTE on the same bar as the sweep)
                if setup.sweep_timestamp == ts:
                    still_active.append(setup)
                    continue

                # 1. Expiry Check
                elapsed_mins = (ts - setup.sweep_timestamp).total_seconds() / 60
                if elapsed_mins > self.ote_expiry_mins:
                    continue
                
                # 2. Wait for MSS (Change of Character)
                if setup.status == "AWAITING_MSS":
                    if elapsed_mins > self.mss_expiry_mins:
                        continue

                    mss_hit = False
                    if setup.direction == Direction.LONG:
                        mss_hit = ltf_bar["close"] > setup.mss_level
                    elif setup.direction == Direction.SHORT:
                        mss_hit = ltf_bar["close"] < setup.mss_level

                    if mss_hit:
                        fvg = self._get_fvg(df_1m, i, ltf_bar["atr"], setup.direction)
                        
                        current_vol = float(ltf_bar["volume"])
                        avg_vol = float(ltf_bar["rolling_avg_volume"])
                        has_displacement = current_vol > (avg_vol * 1.5)
        
                        if fvg is not None and has_displacement: #  
                            setup.status = "AWAITING_OTE"
                            # setup.fvg_zone = fvg # Store this for the entry filter
                            setup.mss_timestamp = ts
                            setup.anchor_high = max(setup.high_at_sweep, ltf_bar["high"])
                            setup.anchor_low = min(setup.low_at_sweep, ltf_bar["low"])
                        else:
                            # Discard setup if there is no FVG or no displacement (Low Conviction)
                            continue
                        
                    still_active.append(setup)
                    
                    
                # 3. Wait for OTE Retreat (The Entry)
                elif setup.status == "AWAITING_OTE":
                                        
                    price_range = setup.anchor_high - setup.anchor_low
                    if price_range <= 0: 
                        continue
                    
                    # How much have we retraced? 0.0 = no retrace, 1.0 = full retrace to sweep
                    if setup.direction == Direction.LONG:
                        retrace = (setup.anchor_high - ltf_bar["close"]) / price_range
                        ote_zone_top = setup.anchor_high - self.retracement_min * price_range
                        ote_zone_bottom = setup.anchor_high - self.retracement_max * price_range
                        anchor_price = setup.low_at_sweep
                        
                        potential_targets = [htf_bar.get("pdh"), htf_bar.get("prev_1h_hi"), htf_bar.get("prev_12h_hi")]
                        valid_targets = [t for t in potential_targets if t and t > ltf_bar["close"]]
                        target_liq = min(valid_targets) if valid_targets else None
                    else:
                        retrace = (ltf_bar["close"] - setup.anchor_low) / price_range
                        ote_zone_bottom= setup.anchor_low + self.retracement_min * price_range
                        ote_zone_top = setup.anchor_low + self.retracement_max * price_range
                        anchor_price = setup.high_at_sweep
                        
                        potential_targets = [htf_bar.get("pdl"), htf_bar.get("prev_1h_lo"), htf_bar.get("prev_12h_lo")]
                        valid_targets = [t for t in potential_targets if t and t < ltf_bar["close"]]
                        target_liq = max(valid_targets) if valid_targets else None # Take the closest one
                    if self.retracement_min <= retrace <= self.retracement_max:
                        
                        last_funding = self._get_last_settled_funding(ts, funding_df)
                        
                        conviction = setup.reliability_score # For now, just use the sweep score as conviction                        
                        funding_favorable = (setup.direction == Direction.LONG and last_funding < 0) or (setup.direction == Direction.SHORT and last_funding > 0)
                        
                        if conviction >= self.min_conviction_score: # and funding_favorable:
                            signals.append(
                                TradeSignal(
                                    symbol=symbol,
                                    timestamp=ts,
                                    direction=setup.direction,
                                    entry=ltf_bar["close"],
                                    conviction=conviction,
                                    metadata={
                                        "sweep_name": setup.name,
                                        "atr": htf_bar["atr"],
                                        "last_funding": last_funding,
                                        "retrace_pct": round(retrace, 3),
                                        "anchor_high": setup.anchor_high,
                                        "anchor_low": setup.anchor_low,
                                        "ote_zone_top": ote_zone_top,
                                        "ote_zone_bottom": ote_zone_bottom,
                                        "ote_timestamp": ts,
                                        "mss_level": setup.mss_level,
                                        "mss_timestamp": setup.mss_timestamp,
                                        "anchor_price": anchor_price, # Use for SL/TP calculations
                                        "target_liq": target_liq, # Use for SL/TP calculations 
                                    }
                                )
                            )
                            continue
                    else:
                        setup.anchor_high = max(setup.high_at_sweep, ltf_bar["high"])
                        setup.anchor_low = min(setup.low_at_sweep, ltf_bar["low"])
                        
                    still_active.append(setup)
                
            self.active_setups[symbol] = still_active
            
        return signals 
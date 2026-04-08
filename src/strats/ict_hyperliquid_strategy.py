from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from models.backtesting_models import Direction, TradeSignal


@dataclass(slots=True)
class _SetupState:
    symbol: str
    direction: Direction
    sweep_time: pd.Timestamp
    trigger_time: pd.Timestamp
    sweep_price: float
    sweep_level_type: str
    reliability_score: float
    confirmed: bool = False
    ote_close_ok: bool = False
    ote_low: float = np.nan
    ote_high: float = np.nan
    ote_pass_time: pd.Timestamp = pd.NaT


class ICTHyperliquidStrategy:
    """
    ICT-specific strategy implementation that converts passed-in OHLCV data
    into TradeSignal objects for an external backtesting engine.

    Expected input shape
    --------------------
    market_bars = {
        "BTC": {"1m": pd.DataFrame, "15m": pd.DataFrame(optional)},
        "ETH": {"1m": pd.DataFrame, "15m": pd.DataFrame(optional)},
    }

    If "15m" is not provided for a symbol, it is derived from "1m".
    """

    _SESSION_LEVELS = ["prev_asia", "prev_london", "prev_ny"]
    _BLOCK_LEVELS = [f"prev_{h}h" for h in [12, 24, 36, 48]]
    _FIXED_LEVELS = [
        ("pdh", "high", "pd_anchor"),
        ("pdl", "low", "pd_anchor"),
        ("midnight_open", "high", "day_start"),
        ("midnight_open", "low", "day_start"),
    ]

    def __init__(
        self,
        *,
        sweep_min_score: float = 61.0,
        dedupe_tol_bps: float = 2.0,
        ote_min: float = 0.61,
        ote_max: float = 0.79,
        setup_timeout_min: float = 120.0,
        post_ote_timeout_min: float = 30.0,
        use_volume_filter: bool = False,
        use_structure_filter: bool = True,
        min_rr: float = 2.0,
    ):
        self.sweep_min_score = float(sweep_min_score)
        self.dedupe_tol_bps = float(dedupe_tol_bps)
        self.ote_min = float(ote_min)
        self.ote_max = float(ote_max)
        self.setup_timeout_min = float(setup_timeout_min)
        self.post_ote_timeout_min = float(post_ote_timeout_min)
        self.use_volume_filter = bool(use_volume_filter)
        self.use_structure_filter = bool(use_structure_filter)
        self.min_rr = float(min_rr)

    def generate_trade_signals(
        self,
        market_bars: dict[str, dict[str, pd.DataFrame]],
    ) -> list[TradeSignal]:
        symbol_contexts = self._build_symbol_contexts(market_bars)
        if not symbol_contexts:
            return []

        ltf_by_symbol: dict[str, dict[pd.Timestamp, pd.Series]] = {}
        for sym, ctx in symbol_contexts.items():
            ltf_by_symbol[sym] = {row["timestamp"]: row for _, row in ctx["df_ltf"].iterrows()}

        sweeps_by_trigger: dict[pd.Timestamp, list[dict[str, Any]]] = {}
        for sym, ctx in symbol_contexts.items():
            for _, row in ctx["sweeps_df"].iterrows():
                key = pd.to_datetime(row["timestamp"], utc=True)
                sweeps_by_trigger.setdefault(key, []).append(
                    {
                        "symbol": sym,
                        "direction": row["direction"],
                        "timestamp": key,
                        "trigger_time": key,
                        "level_price": float(row["level_price"]),
                        "level_type": str(row["level_type"]),
                        "reliability_score": float(row["reliability_score"]),
                    }
                )

        all_timestamps = sorted(
            set(
                ts
                for ctx in symbol_contexts.values()
                for ts in ctx["df_ltf"]["timestamp"]
            )
        )

        signals: list[TradeSignal] = []
        active_setups: list[_SetupState] = []

        for now_ts in all_timestamps:
            active_setups = self._release_setups(now_ts, sweeps_by_trigger, active_setups)
            active_setups = self._confirm_and_gate(now_ts, active_setups, ltf_by_symbol)
            active_setups, new_signals = self._emit_signals(
                now_ts=now_ts,
                setups=active_setups,
                ltf_by_symbol=ltf_by_symbol,
                symbol_contexts=symbol_contexts,
            )
            signals.extend(new_signals)

        return sorted(signals, key=lambda s: (pd.to_datetime(s.timestamp, utc=True), s.symbol))

    def _build_symbol_contexts(
        self,
        market_bars: dict[str, dict[str, pd.DataFrame]],
    ) -> dict[str, dict[str, pd.DataFrame]]:
        contexts: dict[str, dict[str, pd.DataFrame]] = {}

        for symbol, bars in market_bars.items():
            df_1m_raw = bars.get("1m")
            if df_1m_raw is None or df_1m_raw.empty:
                continue

            df_1m = self._normalize_ohlcv(df_1m_raw)
            df_15m = bars.get("15m")
            if df_15m is None or df_15m.empty:
                df_15m = self._resample_ohlcv(df_1m, "15min")
            else:
                df_15m = self._normalize_ohlcv(df_15m)

            df_15m_ctx = self._add_liquidity_pools(df_15m)
            levels_df, sweeps_all = self._detect_sweeps(df_15m_ctx)
            sweeps = sweeps_all.sort_values(["timestamp", "direction", "level_price"]).reset_index(drop=True)
            # sweeps = self._deduplicate_sweeps(sweeps_all, tol_bps=self.dedupe_tol_bps)
            #if not sweeps.empty:
            #    sweeps = sweeps[sweeps["reliability_score"] >= self.sweep_min_score].reset_index(drop=True)

            df_ltf = self._add_ltf_features(
                df=df_1m,
                use_volume_filter=self.use_volume_filter,
                use_structure_filter=self.use_structure_filter,
            )

            contexts[symbol] = {
                "df_1m": df_1m,
                "df_15m": df_15m,
                "df_15m_ctx": df_15m_ctx,
                "df_ltf": df_ltf,
                "levels_df": levels_df,
                "sweeps_df": sweeps,
                "sweeps_all": sweeps_all,
            }

        return contexts

    def _release_setups(
        self,
        now_ts: pd.Timestamp,
        sweeps_by_trigger: dict[pd.Timestamp, list[dict[str, Any]]],
        setups: list[_SetupState],
    ) -> list[_SetupState]:
        for ev in sweeps_by_trigger.get(now_ts, []):
            setups.append(
                _SetupState(
                    symbol=ev["symbol"],
                    direction=Direction(ev["direction"]),
                    sweep_time=ev["timestamp"],
                    trigger_time=ev["trigger_time"],
                    sweep_price=ev["level_price"],
                    sweep_level_type=ev["level_type"],
                    reliability_score=float(ev["reliability_score"]),
                )
            )
        return setups

    def _confirm_and_gate(
        self,
        now_ts: pd.Timestamp,
        setups: list[_SetupState],
        ltf_by_symbol: dict[str, dict[pd.Timestamp, pd.Series]],
    ) -> list[_SetupState]:
        alive: list[_SetupState] = []

        for setup in setups:
            age_min = (now_ts - setup.trigger_time).total_seconds() / 60.0
            if age_min > self.setup_timeout_min:
                continue

            if setup.ote_close_ok and pd.notna(setup.ote_pass_time):
                post_age_min = (now_ts - setup.ote_pass_time).total_seconds() / 60.0
                if post_age_min > self.post_ote_timeout_min:
                    continue

            bar = ltf_by_symbol.get(setup.symbol, {}).get(now_ts)
            if bar is None:
                alive.append(setup)
                continue

            if not setup.confirmed:
                if setup.direction == Direction.SHORT and bool(bar["bearish_shift_ok"]):
                    displacement = setup.sweep_price - float(bar["low"])
                    if displacement > 0:
                        setup.confirmed = True
                        setup.ote_low = float(bar["low"]) + displacement * self.ote_min
                        setup.ote_high = float(bar["low"]) + displacement * self.ote_max
                elif setup.direction == Direction.LONG and bool(bar["bullish_shift_ok"]):
                    displacement = float(bar["high"]) - setup.sweep_price
                    if displacement > 0:
                        setup.confirmed = True
                        setup.ote_low = float(bar["high"]) - displacement * self.ote_max
                        setup.ote_high = float(bar["high"]) - displacement * self.ote_min

            if setup.confirmed and not setup.ote_close_ok:
                if setup.ote_low <= float(bar["close"]) <= setup.ote_high:
                    setup.ote_close_ok = True
                    setup.ote_pass_time = now_ts

            alive.append(setup)

        return alive

    def _emit_signals(
        self,
        now_ts: pd.Timestamp,
        setups: list[_SetupState],
        ltf_by_symbol: dict[str, dict[pd.Timestamp, pd.Series]],
        symbol_contexts: dict[str, dict[str, pd.DataFrame]],
    ) -> tuple[list[_SetupState], list[TradeSignal]]:
        pending: list[_SetupState] = []
        emitted: list[TradeSignal] = []

        for setup in setups:
            if not (setup.confirmed and setup.ote_close_ok):
                pending.append(setup)
                continue

            bar = ltf_by_symbol.get(setup.symbol, {}).get(now_ts)
            if bar is None:
                pending.append(setup)
                continue

            in_zone = float(bar["low"]) <= setup.ote_high and float(bar["high"]) >= setup.ote_low
            is_short = setup.direction == Direction.SHORT
            confluence_ok = bool(bar["bearish_confluence_ok"] if is_short else bar["bullish_confluence_ok"])
            rejection_ok = (
                float(bar["close"]) < float(bar["open"])
                if is_short
                else float(bar["close"]) > float(bar["open"])
            )

            if not (in_zone and confluence_ok and rejection_ok):
                pending.append(setup)
                continue

            entry = float(bar["close"])
            levels_df = symbol_contexts[setup.symbol]["levels_df"]
            tp = self._nearest_key_level(levels_df, now_ts, entry, setup.direction)

            valid_tp = pd.notna(tp) and ((entry > tp) if is_short else (tp > entry))
            if not valid_tp:
                pending.append(setup)
                continue

            sl, rr = self._structural_sl_and_rr(bar, setup, entry, float(tp))
            if pd.isna(sl) or pd.isna(rr) or float(rr) < self.min_rr:
                pending.append(setup)
                continue

            emitted.append(
                TradeSignal(
                    symbol=setup.symbol,
                    timestamp=now_ts,
                    direction=setup.direction,
                    entry=entry,
                    sl=float(sl),
                    tp=float(tp),
                    metadata={
                        "source_sweep_time": setup.sweep_time,
                        "source_level_type": setup.sweep_level_type,
                        "reliability_score": setup.reliability_score,
                        "rr_at_signal": float(rr),
                    },
                )
            )

        return pending, emitted

    @staticmethod
    def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy().sort_values("timestamp").reset_index(drop=True)
        out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
        return out

    @staticmethod
    def _resample_ohlcv(df_1m: pd.DataFrame, rule: str) -> pd.DataFrame:
        return (
            df_1m.resample(rule, on="timestamp")
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            .dropna()
            .reset_index()
        )

    @staticmethod
    def _floor_to_hours(ts: pd.Series, hours: int) -> pd.Series:
        dt = pd.DatetimeIndex(pd.to_datetime(ts, utc=True, errors="coerce"))
        ms_vals = (
            dt.tz_convert("UTC")
            .tz_localize(None)
            .astype("datetime64[ms]")
            .astype("int64")
        )
        block_ms = int(hours) * 3_600_000
        floored_ms = (ms_vals // block_ms) * block_ms
        return pd.Series(
            pd.to_datetime(floored_ms, unit="ms", utc=True, errors="coerce"),
            index=ts.index,
            name=ts.name,
        )

    def _add_liquidity_pools(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy().sort_values("timestamp").reset_index(drop=True)
        out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
        out["_hour"] = out["timestamp"].dt.hour
        out["_date"] = out["timestamp"].dt.date

        out["day_start"] = pd.to_datetime(out["_date"]).dt.tz_localize("UTC")
        midnight_open = (
            out[out["_hour"] == 0]
            .groupby("_date")["open"]
            .first()
            .rename("midnight_open")
        )
        out = out.merge(midnight_open, left_on="_date", right_index=True, how="left")

        sessions = {"asia": (0, 6), "london": (7, 10), "ny": (12, 15)}
        for name, (h_start, h_end) in sessions.items():
            mask = (out["_hour"] >= h_start) & (out["_hour"] < h_end)
            g = out[mask].groupby("_date").agg(
                high_val=("high", "max"),
                low_val=("low", "min"),
                start_ts=("timestamp", "min"),
            )
            g[f"prev_{name}_high"] = g["high_val"].shift(1)
            g[f"prev_{name}_low"] = g["low_val"].shift(1)
            g[f"prev_{name}_anchor"] = g["start_ts"].shift(1)
            out = out.merge(
                g[[f"prev_{name}_high", f"prev_{name}_low", f"prev_{name}_anchor"]],
                left_on="_date",
                right_index=True,
                how="left",
            )
            out[f"prev_{name}_anchor"] = pd.to_datetime(out[f"prev_{name}_anchor"], utc=True)

        for hours in [12, 24, 36, 48]:
            blk = self._floor_to_hours(out["timestamp"], hours)
            block_stats = (
                out.assign(_blk=blk)
                .groupby("_blk", sort=True)
                .agg(
                    high_val=("high", "max"),
                    low_val=("low", "min"),
                    start_ts=("timestamp", "min"),
                )
            )
            prev_stats = block_stats.shift(1)
            out[f"prev_{hours}h_high"] = blk.map(prev_stats["high_val"])
            out[f"prev_{hours}h_low"] = blk.map(prev_stats["low_val"])
            out[f"prev_{hours}h_anchor"] = pd.to_datetime(blk.map(prev_stats["start_ts"]), utc=True)

        daily = out.groupby("_date").agg(
            d_high=("high", "max"),
            d_low=("low", "min"),
            d_start=("timestamp", "min"),
        )
        daily["pdh"] = daily["d_high"].shift(1)
        daily["pdl"] = daily["d_low"].shift(1)
        daily["pd_anchor"] = daily["d_start"].shift(1)
        out = out.merge(daily[["pdh", "pdl", "pd_anchor"]], left_on="_date", right_index=True, how="left")
        out["pd_anchor"] = pd.to_datetime(out["pd_anchor"], utc=True)

        return out.drop(columns=["_hour", "_date"]).ffill()

    @staticmethod
    def _clip01(x: float) -> float:
        return max(0.0, min(1.0, float(x)))

    def _compute_reliability_score(
        self,
        wick_excess_atr: float,
        reclaim_strength_atr: float,
        volume_spike_z: float,
        pre_sweep_touches: int,
        vol_available: bool,
    ) -> float:
        wick = self._clip01(wick_excess_atr / 1.0)
        reclaim = self._clip01(reclaim_strength_atr / 0.6)
        volume = self._clip01((volume_spike_z + 0.5) / 2.5) if vol_available else 0.5
        touch = self._clip01((4.0 - min(float(pre_sweep_touches), 4.0)) / 4.0)
        raw = 0.35 * wick + 0.25 * reclaim + 0.20 * volume + 0.20 * touch
        return float(np.clip(raw * 100.0, 0.0, 100.0))

    def _detect_sweeps(self, df_htf: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        htf = df_htf.copy().sort_values("timestamp").reset_index(drop=True)

        prev_close = htf["close"].shift(1)
        tr = pd.concat(
            [
                (htf["high"] - htf["low"]).abs(),
                (htf["high"] - prev_close).abs(),
                (htf["low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        htf["_atr14"] = tr.rolling(14, min_periods=14).mean()
        htf["_vol_z20"] = (
            (htf["volume"] - htf["volume"].rolling(20, min_periods=20).mean())
            / htf["volume"].rolling(20, min_periods=20).std().replace(0, np.nan)
        )

        def _levels_for_row(row: pd.Series) -> list[tuple[str, Any, str, Any]]:
            entries: list[tuple[str, Any, str, Any]] = []
            for prefix in self._SESSION_LEVELS + self._BLOCK_LEVELS:
                for side in ("high", "low"):
                    entries.append(
                        (
                            f"{prefix}_{side}",
                            row.get(f"{prefix}_{side}"),
                            side,
                            row.get(f"{prefix}_anchor"),
                        )
                    )
            for name, side, anchor_col in self._FIXED_LEVELS:
                entries.append((name, row.get(name), side, row.get(anchor_col)))
            return entries

        levels: list[dict[str, Any]] = []
        sweeps: list[dict[str, Any]] = []
        seen: set[tuple[str, str, float, pd.Timestamp]] = set()

        for i, row in htf.iterrows():
            ts, hi, lo, cl = row["timestamp"], row["high"], row["low"], row["close"]

            for name, price, side, anchor in _levels_for_row(row):
                if pd.isna(price) or pd.isna(anchor):
                    continue
                key = (name, side, float(price), pd.Timestamp(anchor))
                if key in seen:
                    continue
                seen.add(key)
                levels.append(
                    {
                        "level_type": name,
                        "side": side,
                        "price": float(price),
                        "anchor_time": pd.Timestamp(anchor),
                        "created_at": ts,
                        "swept": False,
                        "swept_at": pd.NaT,
                        "sweep_idx": np.nan,
                        "touch_count": 0,
                    }
                )

            atr = row["_atr14"]
            atr_ok = pd.notna(atr) and float(atr) > 0
            vol_z = row["_vol_z20"]

            for lvl in levels:
                p, side = lvl["price"], lvl["side"]
                if side == "high":
                    touched = hi >= p
                    swept = (hi > p) and (cl < p)
                    wick_px = max(0.0, float(hi - p))
                    reclaim_px = max(0.0, float(p - cl))
                    direction = "SHORT"
                else:
                    touched = lo <= p
                    swept = (lo < p) and (cl > p)
                    wick_px = max(0.0, float(p - lo))
                    reclaim_px = max(0.0, float(cl - p))
                    direction = "LONG"

                wick_atr = (wick_px / float(atr)) if atr_ok else np.nan
                reclaim_atr = (reclaim_px / float(atr)) if atr_ok else np.nan

                if swept:
                    lvl.update(swept=True, swept_at=ts, sweep_idx=i)
                    pre_touches = int(lvl["touch_count"])
                    score = self._compute_reliability_score(
                        wick_excess_atr=0.0 if pd.isna(wick_atr) else float(wick_atr),
                        reclaim_strength_atr=0.0 if pd.isna(reclaim_atr) else float(reclaim_atr),
                        volume_spike_z=0.0 if pd.isna(vol_z) else float(vol_z),
                        pre_sweep_touches=pre_touches,
                        vol_available=pd.notna(vol_z),
                    )
                    sweeps.append(
                        {
                            "timestamp": ts,
                            "idx": i,
                            "direction": direction,
                            "level_type": lvl["level_type"],
                            "level_side": side,
                            "level_price": p,
                            "pre_sweep_touches": pre_touches,
                            "wick_excess": float(wick_px),
                            "wick_excess_atr": float(wick_atr) if pd.notna(wick_atr) else np.nan,
                            "reclaim_strength": float(reclaim_px),
                            "reclaim_strength_atr": float(reclaim_atr) if pd.notna(reclaim_atr) else np.nan,
                            "volume_spike_z": float(vol_z) if pd.notna(vol_z) else np.nan,
                            "reliability_score": score,
                        }
                    )
                elif touched:
                    lvl["touch_count"] += 1

        levels_df = pd.DataFrame(levels)
        sweeps_df = pd.DataFrame(sweeps)
        if not levels_df.empty:
            levels_df["swept_at"] = pd.to_datetime(levels_df["swept_at"], utc=True)
        return levels_df, sweeps_df

    @staticmethod
    def _deduplicate_sweeps(sweeps_df: pd.DataFrame, tol_bps: float = 2.0) -> pd.DataFrame:
        if sweeps_df is None or sweeps_df.empty:
            return sweeps_df.copy()

        tol = tol_bps / 10_000.0
        work = sweeps_df.sort_values(["timestamp", "direction", "level_price"]).reset_index(drop=True)
        out_rows: list[pd.Series] = []

        for (_, _), grp in work.groupby(["timestamp", "direction"], sort=False):
            grp = grp.sort_values("level_price").reset_index(drop=True)
            cluster: list[dict[str, Any]] = []

            def _flush(rows: list[dict[str, Any]]) -> None:
                if not rows:
                    return
                cdf = pd.DataFrame(rows)
                rep = cdf.loc[cdf["reliability_score"].idxmax()].copy()
                types = sorted(cdf["level_type"].astype(str).unique().tolist())
                rep["merged_level_types"] = "|".join(types)
                rep["merged_events"] = len(cdf)
                boost = (len(types) - 1) * 5.0
                rep["reliability_score"] = float(np.clip(rep["reliability_score"] + boost, 0.0, 100.0))
                rep["confluence_boost"] = boost
                out_rows.append(rep)

            for _, row in grp.iterrows():
                d = row.to_dict()
                if not cluster:
                    cluster.append(d)
                    continue
                ref = float(cluster[0]["level_price"])
                price_ref = max(abs(ref), abs(float(d["level_price"])), 1e-12)
                if abs(float(d["level_price"]) - ref) <= price_ref * tol:
                    cluster.append(d)
                else:
                    _flush(cluster)
                    cluster = [d]
            _flush(cluster)

        return pd.DataFrame(out_rows).sort_values(["timestamp", "direction", "level_price"]).reset_index(drop=True)

    @staticmethod
    def _add_ltf_features(
        df: pd.DataFrame,
        use_volume_filter: bool = False,
        use_structure_filter: bool = True,
        vol_lookback: int = 50,
        atr_len: int = 14,
        swing_left: int = 3,
        swing_right: int = 3,
        min_break_atr: float = 0.10,
    ) -> pd.DataFrame:
        out = df.copy().sort_values("timestamp").reset_index(drop=True)
        out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)

        prev_close = out["close"].shift(1)
        tr = pd.concat(
            [
                (out["high"] - out["low"]).abs(),
                (out["high"] - prev_close).abs(),
                (out["low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        out["atr"] = tr.rolling(atr_len, min_periods=atr_len).mean()

        out["vol_sma"] = out["volume"].rolling(vol_lookback, min_periods=vol_lookback).mean()
        out["high_participation"] = out["volume"] > out["vol_sma"]

        if use_structure_filter:
            win = swing_left + swing_right + 1
            roll_max = out["high"].rolling(win, min_periods=win).max()
            roll_min = out["low"].rolling(win, min_periods=win).min()
            cand_hi = out["high"].shift(swing_right)
            cand_lo = out["low"].shift(swing_right)

            out["_swing_hi_p"] = np.where(cand_hi >= roll_max, cand_hi, np.nan)
            out["_swing_lo_p"] = np.where(cand_lo <= roll_min, cand_lo, np.nan)
            out["last_swing_high"] = out["_swing_hi_p"].ffill()
            out["last_swing_low"] = out["_swing_lo_p"].ffill()

            up_break = (
                (out["close"] > out["last_swing_high"])
                & (out["close"].shift(1) <= out["last_swing_high"])
                & ((out["close"] - out["last_swing_high"]) >= min_break_atr * out["atr"])
            )
            dn_break = (
                (out["close"] < out["last_swing_low"])
                & (out["close"].shift(1) >= out["last_swing_low"])
                & ((out["last_swing_low"] - out["close"]) >= min_break_atr * out["atr"])
            )

            bull_shift = np.zeros(len(out), bool)
            bear_shift = np.zeros(len(out), bool)
            for i in range(len(out)):
                up = bool(up_break.iat[i]) if pd.notna(up_break.iat[i]) else False
                dn = bool(dn_break.iat[i]) if pd.notna(dn_break.iat[i]) else False
                if up:
                    bull_shift[i] = True
                elif dn:
                    bear_shift[i] = True

            out["bullish_shift"] = bull_shift
            out["bearish_shift"] = bear_shift
            out = out.drop(columns=["_swing_hi_p", "_swing_lo_p"])
        else:
            out["last_swing_high"] = np.nan
            out["last_swing_low"] = np.nan
            out["bullish_shift"] = True
            out["bearish_shift"] = True

        out["bullish_fvg"] = out["low"] > out["high"].shift(2)
        out["bearish_fvg"] = out["high"] < out["low"].shift(2)

        out["bullish_ob"] = (
            (out["close"].shift(1) < out["open"].shift(1))
            & (out["close"] > out["open"])
            & (out["close"] > out["high"].shift(1))
        )
        out["bearish_ob"] = (
            (out["close"].shift(1) > out["open"].shift(1))
            & (out["close"] < out["open"])
            & (out["close"] < out["low"].shift(1))
        )

        out["bullish_confluence"] = out["bullish_fvg"] | out["bullish_ob"]
        out["bearish_confluence"] = out["bearish_fvg"] | out["bearish_ob"]

        vol_gate = out["high_participation"] if use_volume_filter else True
        out["bullish_shift_ok"] = out["bullish_shift"] & vol_gate
        out["bearish_shift_ok"] = out["bearish_shift"] & vol_gate
        out["bullish_confluence_ok"] = out["bullish_confluence"] & vol_gate
        out["bearish_confluence_ok"] = out["bearish_confluence"] & vol_gate

        return out

    @staticmethod
    def _nearest_key_level(
        levels_df: pd.DataFrame,
        now_ts: pd.Timestamp,
        entry: float,
        direction: Direction,
    ) -> float:
        if levels_df.empty:
            return np.nan

        live = levels_df[
            (levels_df["created_at"] <= now_ts)
            & (levels_df["swept_at"].isna() | (levels_df["swept_at"] > now_ts))
        ]
        if live.empty:
            return np.nan

        if direction == Direction.LONG:
            cands = live[(live["side"] == "high") & (live["price"] > entry)]
            return float(cands["price"].min()) if not cands.empty else np.nan

        cands = live[(live["side"] == "low") & (live["price"] < entry)]
        return float(cands["price"].max()) if not cands.empty else np.nan

    @staticmethod
    def _structural_sl_and_rr(
        bar: pd.Series,
        setup: _SetupState,
        entry: float,
        tp: float,
    ) -> tuple[float, float]:
        atr = float(bar["atr"]) if pd.notna(bar.get("atr", np.nan)) else 0.0
        buf = max(atr * 0.20, entry * 0.0005)

        if setup.direction == Direction.SHORT:
            sl = max(float(bar["high"]), setup.sweep_price, setup.ote_high) + buf
            if sl <= entry or tp >= entry:
                return np.nan, np.nan
            return sl, (entry - tp) / (sl - entry)

        sl = min(float(bar["low"]), setup.sweep_price, setup.ote_low) - buf
        if sl >= entry or tp <= entry:
            return np.nan, np.nan
        return sl, (tp - entry) / (entry - sl)

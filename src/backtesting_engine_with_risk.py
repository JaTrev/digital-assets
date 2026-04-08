from __future__ import annotations

import bisect
from typing import Iterable

import pandas as pd
from models.backtesting_models import (
    BacktestResult,
    Direction,
    ExitReason,
    Position,
    RejectedSignal,
    Trade,
    TradeSignal,
)


class BacktestEngine:
    """A strategy-agnostic PnL engine driven by OHLC bars plus external signals."""

    def __init__(
        self,
        *,
        initial_balance: float = 50_000.0,
        leverage: float = 5.0,
        risk_per_trade: float = 0.02,
        maint_margin_rate: float = 0.005,
        min_notional: float = 100.0,
        allow_same_bar_exit: bool = False,
        max_absolute_margin: float | None = None,
    ):
        self.initial_balance = float(initial_balance)
        self.leverage = float(leverage)
        self.risk_per_trade = float(risk_per_trade)
        self.maint_margin_rate = float(maint_margin_rate)
        self.min_notional = float(min_notional)
        self.allow_same_bar_exit = bool(allow_same_bar_exit)
        self.max_absolute_margin = None if max_absolute_margin is None else float(max_absolute_margin)

    def run(
        self,
        bars_by_symbol: dict[str, pd.DataFrame],
        signals: Iterable[TradeSignal],
        *,
        align_signals_to_timeline: bool = True,
    ) -> BacktestResult:
        ltf_by_symbol, all_timestamps = self._build_bar_lookup(bars_by_symbol)
        scheduled = self._schedule_signals(signals, all_timestamps, align_signals_to_timeline)

        balance = float(self.initial_balance)
        positions: list[Position] = []
        trades: list[Trade] = []
        rejected_signals: list[RejectedSignal] = []
        equity_rows = [{"timestamp": all_timestamps[0] if all_timestamps else pd.NaT, "balance": balance}]

        for now_ts in all_timestamps:
            entries = scheduled.get(now_ts, [])
            if entries:
                positions, rejected_new = self._try_open_positions(
                    now_ts=now_ts,
                    signals_now=entries,
                    positions=positions,
                    balance=balance,
                )
                rejected_signals.extend(rejected_new)

            positions, balance, closed_now = self._manage_positions(
                now_ts=now_ts,
                positions=positions,
                ltf_by_symbol=ltf_by_symbol,
                balance=balance,
            )
            if closed_now:
                trades.extend(closed_now)
            equity_rows.append({"timestamp": now_ts, "balance": balance})

        equity_curve = pd.DataFrame(equity_rows).dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")

        return BacktestResult(
            final_balance=balance,
            trades=trades,
            open_positions=positions,
            rejected_signals=rejected_signals,
            equity_curve=equity_curve,
        )

    def _build_bar_lookup(
        self,
        bars_by_symbol: dict[str, pd.DataFrame],
    ) -> tuple[dict[str, dict[pd.Timestamp, pd.Series]], list[pd.Timestamp]]:
        lookup: dict[str, dict[pd.Timestamp, pd.Series]] = {}
        all_ts: list[pd.Timestamp] = []

        for symbol, df in bars_by_symbol.items():
            if df is None or df.empty:
                lookup[symbol] = {}
                continue

            work = df.copy().sort_values("timestamp").reset_index(drop=True)
            work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True)

            sym_lookup = {row["timestamp"]: row for _, row in work.iterrows()}
            lookup[symbol] = sym_lookup
            all_ts.extend(list(work["timestamp"]))

        timeline = sorted(set(all_ts))
        return lookup, timeline

    def _schedule_signals(
        self,
        signals: Iterable[TradeSignal],
        timeline: list[pd.Timestamp],
        align: bool,
    ) -> dict[pd.Timestamp, list[TradeSignal]]:
        out: dict[pd.Timestamp, list[TradeSignal]] = {}
        if not timeline:
            return out

        for sig in signals:
            sig_ts = pd.to_datetime(sig.timestamp, utc=True)
            if align:
                idx = bisect.bisect_left(timeline, sig_ts)
                if idx >= len(timeline):
                    continue
                release_ts = timeline[idx]
            else:
                if sig_ts not in set(timeline):
                    continue
                release_ts = sig_ts

            normalized = TradeSignal(
                symbol=sig.symbol,
                timestamp=sig_ts,
                direction=Direction(sig.direction),
                entry=float(sig.entry),
                sl=float(sig.sl),
                tp=float(sig.tp),
                metadata=dict(sig.metadata),
            )
            out.setdefault(release_ts, []).append(normalized)

        return out

    def _try_open_positions(
        self,
        now_ts: pd.Timestamp,
        signals_now: list[TradeSignal],
        positions: list[Position],
        balance: float,
    ) -> tuple[list[Position], list[RejectedSignal]]:
        equity = balance
        used_margin = sum(p.margin_req for p in positions)
        free_margin = max(0.0, equity - used_margin)
        rejected: list[RejectedSignal] = []

        for sig in signals_now:
            validation_error = self._validate_signal(sig)
            assert validation_error is None, f"Validation error: {validation_error}"

            rr = self._calc_rr(sig)
            sizing = self._size_position(
                entry=sig.entry,
                sl=sig.sl,
                equity=equity,
                free_margin=free_margin,
                direction=Direction(sig.direction),
            )
            if sizing is None:
                rejected.append(RejectedSignal(signal=sig, reason="failed_risk_or_margin_guards"))
                continue

            pos = Position(
                symbol=sig.symbol,
                direction=Direction(sig.direction),
                entry_time=now_ts,
                entry=sig.entry,
                sl=sig.sl,
                tp=sig.tp,
                liq_price=sizing["liq_price"],
                qty=sizing["qty"],
                notional=sizing["notional"],
                margin_req=sizing["margin_req"],
                entry_equity=equity,
                risk_budget=equity * self.risk_per_trade,
                risk_at_sl=sizing["risk_at_sl"],
                rr_at_entry=rr,
                metadata=dict(sig.metadata),
            )
            positions.append(pos)
            free_margin -= sizing["margin_req"]

        return positions, rejected

    def _manage_positions(
        self,
        now_ts: pd.Timestamp,
        positions: list[Position],
        ltf_by_symbol: dict[str, dict[pd.Timestamp, pd.Series]],
        balance: float,
    ) -> tuple[list[Position], float, list[Trade]]:
        remaining: list[Position] = []
        closed: list[Trade] = []

        for pos in positions:
            if not self.allow_same_bar_exit and pos.entry_time == now_ts:
                remaining.append(pos)
                continue

            bar = ltf_by_symbol.get(pos.symbol, {}).get(now_ts)
            if bar is None:
                remaining.append(pos)
                continue

            hi = float(bar["high"])
            lo = float(bar["low"])

            exit_reason, pnl = self._exit_for_bar(pos, hi=hi, lo=lo)
            if exit_reason is None:
                remaining.append(pos)
                continue

            balance += pnl
            close_price = self._close_price_for_reason(pos, exit_reason)
            closed.append(
                Trade(
                    symbol=pos.symbol,
                    direction=pos.direction,
                    entry_time=pos.entry_time,
                    exit_time=now_ts,
                    entry=pos.entry,
                    sl=pos.sl,
                    tp=pos.tp,
                    liq_price=pos.liq_price,
                    qty=pos.qty,
                    notional=pos.notional,
                    margin_req=pos.margin_req,
                    entry_equity=pos.entry_equity,
                    risk_budget=pos.risk_budget,
                    risk_at_sl=pos.risk_at_sl,
                    rr_at_entry=pos.rr_at_entry,
                    exit_reason=exit_reason,
                    close_price=close_price,
                    pnl=pnl,
                    balance=balance,
                    metadata=dict(pos.metadata),
                )
            )

        return remaining, balance, closed

    def _size_position(
        self,
        *,
        entry: float,
        sl: float,
        equity: float,
        free_margin: float,
        direction: Direction,
    ) -> dict[str, float] | None:
        lev = float(self.leverage)        
        sl_dist = abs(entry - sl)
        if sl_dist <= 0:
            return None
        risk_budget = float(equity) * self.risk_per_trade
        qty_by_risk = risk_budget / sl_dist
        
        if self.max_absolute_margin is not None:  
            allowed_margin = min(self.max_absolute_margin, free_margin)
        else:
            allowed_margin = free_margin
        qty_by_margin_cap = (allowed_margin * lev) / entry

        qty = min(qty_by_risk, qty_by_margin_cap)

        if qty <= 0:
            return None

        notional = qty * entry
        margin_req = notional / lev
        risk_at_sl = sl_dist * qty

        # 4. Liquidation Safeguard (5x leverage context)
        if direction == Direction.SHORT:
            liq = entry * (1.0 + 1.0 / lev - self.maint_margin_rate)
            sl_ok = sl < liq
        else:
            liq = entry * (1.0 - 1.0 / lev + self.maint_margin_rate)
            sl_ok = sl > liq

        # Final checks: Minimum trade size and SL safety
        if notional < self.min_notional or not sl_ok:
            return None

        return {
            "qty": qty,
            "notional": notional,
            "margin_req": margin_req,
            "liq_price": liq,
            "risk_at_sl": risk_at_sl,
        }

    def _validate_signal(self, sig: TradeSignal) -> str | None:
        if sig.symbol == "":
            return "empty_symbol"

        direction = Direction(sig.direction)
        if direction == Direction.LONG:
            if not (sig.sl < sig.entry < sig.tp):
                return "invalid_long_levels_expected_sl_lt_entry_lt_tp"
        else:
            if not (sig.tp < sig.entry < sig.sl):
                return "invalid_short_levels_expected_tp_lt_entry_lt_sl"

        return None

    def _calc_rr(self, sig: TradeSignal) -> float:
        if Direction(sig.direction) == Direction.SHORT:
            return (sig.entry - sig.tp) / (sig.sl - sig.entry)
        return (sig.tp - sig.entry) / (sig.entry - sig.sl)

    def _exit_for_bar(self, pos: Position, *, hi: float, lo: float) -> tuple[ExitReason | None, float]:
        if pos.direction == Direction.SHORT:
            hit_liq = hi >= pos.liq_price
            hit_sl = hi >= pos.sl
            hit_tp = lo <= pos.tp
            if hit_liq or hit_sl:
                if hit_liq and hit_sl and pos.liq_price <= pos.sl:
                    return ExitReason.LIQ, -pos.margin_req
                if hit_liq:
                    return ExitReason.LIQ, -pos.margin_req
                return ExitReason.SL, (pos.entry - pos.sl) * pos.qty
            if hit_tp:
                return ExitReason.TP, (pos.entry - pos.tp) * pos.qty
            return None, 0.0

        hit_liq = lo <= pos.liq_price
        hit_sl = lo <= pos.sl
        hit_tp = hi >= pos.tp
        if hit_liq or hit_sl:
            if hit_liq and hit_sl and pos.liq_price >= pos.sl:
                return ExitReason.LIQ, -pos.margin_req
            if hit_liq:
                return ExitReason.LIQ, -pos.margin_req
            return ExitReason.SL, (pos.sl - pos.entry) * pos.qty
        if hit_tp:
            return ExitReason.TP, (pos.tp - pos.entry) * pos.qty
        return None, 0.0

    def _close_price_for_reason(self, pos: Position, reason: ExitReason) -> float:
        if reason == ExitReason.TP:
            return pos.tp
        if reason == ExitReason.SL:
            return pos.sl
        return pos.liq_price

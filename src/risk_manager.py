from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

import pandas as pd
from models.backtesting_models import Direction, RejectedSignal, TradeSignal
from models.risk_manager_models import ExecutionSignal

MMR_CONFIG = {
    "BTC": 0.0125,          # 40x max -> (1/40 * 0.5)
    "ETH": 0.02,            # 25x max -> (1/25 * 0.5)
    "SOL": 0.025,           # 20x max -> (1/20 * 0.5)
    "XRP": 0.025,           # 20x max
    "HYPE": 0.05,           # 10x max -> (1/10 * 0.5)
    "DOGE": 0.05,           # 10x max
    "xyz:SP500": 0.01,      # 50x max -> (1/50 * 0.5)
    "xyz:GOLD": 0.02,       # 25x max
    "xyz:SILVER": 0.02,     # 25x max
    "xyz:CL": 0.025,        # 20x max
    "xyz:BRENTOIL": 0.025,  # 20x max
    "DEFAULT": 0.025        # Safe fallback for unknown alts
}

class RiskManager:
    """Position sizing and liquidation-safety checks.

    This manager sizes using risk_per_trade, optionally capped by max_absolute_margin
    (cash margin per trade), and validates liquidation-vs-stop placement.
    """

    def __init__(
        self,
        *,
        leverage: float = 5.0,
        risk_per_trade: float = 0.02,
        min_notional: float = 100.0,
        max_absolute_margin: float | None = None,
        stop_loss_atr_multiplier: float = 2.0,                  # How many ATRs for SL -> volatility buffer-based SL
        tp_risk_reward_multiplier: float = 2.5,                   # Target Reward-to-Risk (RR)
    ):
        self.leverage = float(leverage)
        self.risk_per_trade = float(risk_per_trade)
        self.min_notional = float(min_notional)
        self.max_absolute_margin = (
            None if max_absolute_margin is None else float(max_absolute_margin)
        )
        self.stop_loss_atr_multiplier = float(stop_loss_atr_multiplier)
        self.tp_risk_reward_multiplier = float(tp_risk_reward_multiplier)

    def size_position(
        self, 
        *,
        symbol: str,
        entry: float,
        sl: float,
        equity: float,
        free_margin: float,
        direction: Direction,
    ) -> dict[str, float] | None:
        
        maintenance_margin_rate = MMR_CONFIG.get(symbol, MMR_CONFIG["DEFAULT"])
        lev = self.leverage
        sl_dist = abs(float(entry) - float(sl))
        if sl_dist <= 0:
            return None

        risk_budget = float(equity) * self.risk_per_trade
        qty_by_risk = risk_budget / sl_dist

        if self.max_absolute_margin is None:
            allowed_margin = max(0.0, free_margin)
        else:
            allowed_margin = min(self.max_absolute_margin, max(0.0, free_margin))

        qty_by_margin_cap = (allowed_margin * lev) / float(entry)
        qty = min(qty_by_risk, qty_by_margin_cap)
        if qty <= 0.0:
            print(f"Symbol {symbol}: Position rejected by margin cap. Allowed margin: {allowed_margin:.2f}, which allows for {qty_by_margin_cap:.4f} qty at entry price {entry:.2f}. Risk-based qty is {qty_by_risk:.4f}. Free margin is {free_margin:.2f}.")
            return None

        notional = qty * float(entry)
        margin_req = notional / lev
        risk_at_sl = sl_dist * qty

        if direction == Direction.SHORT:
            liq = float(entry) * (1.0 + 1.0 / lev) / (1.0 + maintenance_margin_rate)
            sl_ok = float(sl) < liq
        else:
            liq = float(entry) * (1.0 - 1.0 / lev) / (1.0 - maintenance_margin_rate)
            sl_ok = float(sl) > liq

        if notional < self.min_notional or not sl_ok or margin_req > float(free_margin):
            print(f"Symbol {symbol}: Position rejected by notional, SL, or margin. Notional: {notional:.2f}, SL OK: {sl_ok}, Margin Req: {margin_req:.2f}, Free Margin: {free_margin:.2f}")
            return None

        return {
            "qty": qty,
            "notional": notional,
            "margin_req": margin_req,
            "liq_price": liq,
            "risk_at_sl": risk_at_sl,
            "risk_budget": risk_budget,
        }

    def build_execution_signal(
        self,
        *,
        signal: TradeSignal,
        equity: float,
        free_margin: float,
        execution_timestamp: pd.Timestamp | None = None,
    ) -> tuple[ExecutionSignal | None, RejectedSignal | None]:
        
        # Extract average true range (ATR) from strategy metadata
        atr = signal.metadata.get("atr")
        struct_sl = signal.metadata.get("anchor_price") # The high/low of the sweep
        target_liq = signal.metadata.get("target_liq") # The opposing liquidity pool
        entry = float(signal.entry)
        direction = Direction(signal.direction)
        assert atr is not None, f"The following signal is missing ATR in metadata: {signal}"

        # 2. CALCULATE DYNAMIC SL AND TP
        # We calculate these BEFORE sizing so size_position knows the risk distance
        # sl_dist = atr * self.stop_loss_atr_multiplier
        #if direction == Direction.LONG:
        #    calculated_sl = entry - sl_dist
        #    calculated_tp = entry + (sl_dist * self.tp_risk_reward_multiplier)
        #else:
        #    calculated_sl = entry + sl_dist
        #    calculated_tp = entry - (sl_dist * self.tp_risk_reward_multiplier)
                
        # 1. Structural SL + Breathing Room
        # We add 0.1 * ATR so we don't get stopped out by 1 tick of spread/noise
        #breathing_room = atr * 0.1 
        #if direction == Direction.LONG:
        #    calculated_sl = struct_sl - breathing_room
        #else:
        #    calculated_sl = struct_sl + breathing_room
            
        # 2. Structural TP
        # If no target_liq is found in metadata, fall back to mathematical multiplier
        #if target_liq and not pd.isna(target_liq):
        #    calculated_tp = float(target_liq)
        #else:
        #    sl_dist = abs(entry - calculated_sl)
        #    calculated_tp = entry + (sl_dist * self.tp_risk_reward_multiplier) if direction == Direction.LONG else entry - (sl_dist * self.tp_risk_reward_multiplier)

        # 1. CALCULATE TARGET DISTANCE FOR 10% ROE
        # If leverage is 10, this is 0.01 (1%). If leverage is 20, this is 0.005 (0.5%)
        tp_price_change = 0.10 / self.leverage 

        # 2. CALCULATE STOP LOSS DISTANCE FOR 5% ROE (1:2 Risk/Reward)
        # We risk half of what we aim to make.
        sl_price_change = 0.05 / self.leverage

        # 3. APPLY TO ENTRY PRICE
        if direction == Direction.LONG:
            calculated_tp = entry * (1 + tp_price_change)
            calculated_sl = entry * (1 - sl_price_change)
        else:
            calculated_tp = entry * (1 - tp_price_change)
            calculated_sl = entry * (1 + sl_price_change)

        # 3. The RR guard
        risk = abs(entry - calculated_sl)
        reward = abs(calculated_tp - entry)
        rr = reward / risk if risk > 0 else 0
        if rr <= 0.0:
            return None, RejectedSignal(signal=signal, reason=f"RR too low: {rr:.2f}")
        
        # 4. Sizing
        sizing = self.size_position(
            symbol=signal.symbol,
            entry=entry,
            sl=calculated_sl,
            equity=float(equity),
            free_margin=float(free_margin),
            direction=direction,
        )
        
        if sizing is None:
            return None, RejectedSignal(signal=signal, reason="failed_risk_or_margin_guards")

        # 4. Calculate RR for the execution signal
        #rr = abs(entry - calculated_tp) / abs(entry - calculated_sl)

        exec_ts = pd.to_datetime(signal.timestamp)        
        
        out = ExecutionSignal(
            symbol=signal.symbol,
            timestamp=exec_ts,
            direction=direction,
            entry=entry,
            conviction=signal.conviction,
            sl=calculated_sl,
            tp=calculated_tp,
            qty=float(sizing["qty"]),
            notional=float(sizing["notional"]),
            margin_req=float(sizing["margin_req"]),
            liq_price=float(sizing["liq_price"]),
            risk_budget=float(sizing["risk_budget"]),
            risk_at_sl=float(sizing["risk_at_sl"]),
            rr_at_entry=rr,
            metadata=dict(signal.metadata),
        )
        return out, None

    def build_execution_batch(
        self,
        *,
        signals: list[TradeSignal],
        equity: float,
        free_margin: float,
        ) -> tuple[list[ExecutionSignal], list[RejectedSignal]]:
        """
        Processes a batch of raw signals and converts them into validated ExecutionSignals.
        This is the bridge between the Strategy and the Backtest Engine.
        """
        execution_batch: list[ExecutionSignal] = []
        rejected_signals: list[RejectedSignal] = []
        running_free_margin = float(free_margin)

        for signal in signals:
            exec_sig, rejected = self.build_execution_signal(
                signal=signal,
                equity=float(equity),
                free_margin=running_free_margin,
            )

            if rejected:
                rejected_signals.append(rejected)
                continue

            if exec_sig:
                execution_batch.append(exec_sig)
                running_free_margin -= exec_sig.margin_req

        return execution_batch, rejected_signals
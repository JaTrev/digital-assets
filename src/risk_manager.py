from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

import numpy as np
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
        #risk_per_trade: float = 0.02,
        min_notional: float = 100.0,
        max_absolute_margin: float | None = None,
        # stop_loss_atr_multiplier: float = 2.0,                  # How many ATRs for SL -> volatility buffer-based SL
        # tp_risk_reward_multiplier: float = 2.5,                   # Target Reward-to-Risk (RR)
    ):
        self.leverage = float(leverage)
        # self.risk_per_trade = float(risk_per_trade)
        self.min_notional = float(min_notional)
        self.max_absolute_margin = None if max_absolute_margin is None else float(max_absolute_margin)
        # self.stop_loss_atr_multiplier = float(stop_loss_atr_multiplier)
        # self.tp_risk_reward_multiplier = float(tp_risk_reward_multiplier)

    def size_position(
        self, 
        *,
        symbol: str,
        entry: float,
        # sl: float,
        # equity: float,
        free_margin: float,
        direction: Direction,
    ) -> dict[str, float] | None:
        
        maintenance_margin_rate = MMR_CONFIG.get(symbol, MMR_CONFIG["DEFAULT"])
        lev = self.leverage

        if self.max_absolute_margin is None:
            allowed_margin = max(0.0, free_margin)
        else:
            allowed_margin = min(self.max_absolute_margin, max(0.0, free_margin))

        qty_by_margin_cap = (allowed_margin * lev) / float(entry)
        
        # risk_budget = float(equity) * self.risk_per_trade
        # qty_by_risk = risk_budget / sl_dist
        # qty = min(qty_by_risk, qty_by_margin_cap)
        # risk_at_sl = sl_dist * qty
        qty = qty_by_margin_cap
        if qty <= 0.0:
            print(f"Symbol {symbol}: Position rejected. Allowed margin: {allowed_margin:.2f}, which allows for {qty_by_margin_cap:.4f} qty at entry price {entry:.2f}. Free margin is {free_margin:.2f}.")
            return None

        notional = qty * float(entry)
        initial_margin = notional / lev
        maintenance_margin = notional * maintenance_margin_rate
        margin_available = initial_margin - maintenance_margin
        
        side = 1 if direction == Direction.LONG else -1
        # is correct, verified with a long position on ETH
        liq_price = float(entry) - side * margin_available / qty  / (1 - maintenance_margin_rate * side) 

        if notional < self.min_notional:
            print(f"Symbol {symbol}: Position rejected by notional. Notional: {notional:.2f}, Min. Notional: {self.min_notional:.2f}, Initial Margin: {initial_margin:.2f}, Free Margin: {free_margin:.2f}")
            return None

        return {
            "qty": qty,
            "notional": notional,
            "initial_margin": initial_margin,
            "liq_price": liq_price,
        }

    def build_execution_signal(
        self,
        *,
        signal: TradeSignal,
        # equity: float,
        volatility: float | None,
        free_margin: float,
        execution_timestamp: pd.Timestamp | None = None,
    ) -> tuple[ExecutionSignal | None, RejectedSignal | None]:
        
        # Extract average true range (ATR) from strategy metadata
        # atr = signal.metadata.get("atr", None)
        # struct_sl = signal.metadata.get("anchor_price") # The high/low of the sweep
        # target_liq = signal.metadata.get("target_liq") # The opposing liquidity pool
        entry = float(signal.entry)
        direction = Direction(signal.direction)
        # assert atr is not None, f"The following signal is missing ATR in metadata: {signal}"

        # 1. CALCULATE TARGET DISTANCE FOR 20% ROE
        # If leverage is 10, this is 0.02 (2%). If leverage is 20, this is 0.01 (1%)
        # tp_price_change = 0.20 / self.leverage
        #sl_price_change = 0.30 / self.leverage
        
        # Volatility-adjusted TP
        if volatility is None:
            tp_price_change = 0.10 / self.leverage 
        else:
            tp_price_change = max(0.7 * volatility, 0.15 / self.leverage)

        # 2. CALCULATE STOP LOSS DISTANCE FOR 5% ROE (1:2 Risk/Reward)
        # We risk half of what we aim to make.

        # 3. APPLY TO ENTRY PRICE
        if direction == Direction.LONG:
            calculated_tp = entry * (1 + tp_price_change)
            #calculated_sl = entry * (1 - sl_price_change)
        else:
            calculated_tp = entry * (1 - tp_price_change)
            #calculated_sl = entry * (1 + sl_price_change)
        
        # 4. Sizing
        sizing = self.size_position(
            symbol=signal.symbol,
            entry=entry,
            # sl=calculated_sl,
            # equity=float(equity),
            free_margin=float(free_margin),
            direction=direction,
        )
        
        if sizing is None:
            return None, RejectedSignal(signal=signal, reason="failed_risk_or_margin_guards")
        
        # The RR guard
        calculated_sl = sizing["liq_price"]  #TODO: proper sl calculation instead of using liq as a proxy
        risk = abs(entry - calculated_sl)
        reward = abs(calculated_tp - entry)
        rr = reward / risk if risk > 0 else 0
        if rr <= 0.0:
            return None, RejectedSignal(signal=signal, reason=f"RR too low: {rr:.2f}")

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
            margin_req=float(sizing["initial_margin"]),
            liq_price=float(sizing["liq_price"]),
            risk_budget=None,#float(sizing["risk_budget"]),
            risk_at_sl=None, #float(sizing["risk_at_sl"]),
            rr_at_entry=rr,
            metadata=dict(signal.metadata),
        )
        return out, None

    def build_execution_batch(
        self,
        *,
        ts: pd.Timestamp,
        signals: list[TradeSignal],
        df_1m: dict[str, pd.DataFrame],
        # equity: float,
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
            
            start_lookback_t = ts - pd.Timedelta(hours=8)
            pre_trade_bars = df_1m[signal.symbol].loc[start_lookback_t : ts]
            if len(pre_trade_bars) < 60: # Ensure we have at least 1 hour of data to be meaningful
                volatility_8h = None
            else:
                log_returns = np.log(pre_trade_bars["close"] / pre_trade_bars["close"].shift(1))
                volatility_8h = log_returns.std() * np.sqrt(60*8)
            exec_sig, rejected = self.build_execution_signal(
                signal=signal,
                # equity=float(equity),
                volatility=volatility_8h,
                free_margin=running_free_margin,
            )

            if rejected:
                rejected_signals.append(rejected)
                continue

            if exec_sig:
                execution_batch.append(exec_sig)
                running_free_margin -= exec_sig.margin_req

        return execution_batch, rejected_signals
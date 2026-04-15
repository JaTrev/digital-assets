from __future__ import annotations
from dataclasses import asdict
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
    AccountState,
)
from risk_manager import ExecutionSignal


class BacktestEngine:
    """Backtesting engine that can execute executions signals into trades and positions."""

    def __init__(self, allow_same_bar_exit: bool = False):
        self.allow_same_bar_exit = bool(allow_same_bar_exit)
        self.maker_fee = 0.015 / 100  # 0.015%
        self.taker_fee = 0.045 / 100  # 0.045%
        
    def _get_exit_fee_rate(self, reason: ExitReason) -> float:
        if reason == ExitReason.TP:
            return self.taker_fee # Assuming TP is taker execution
        return self.taker_fee
        
    def update_account(self, account: AccountState, current_bars: dict[str, pd.Series]) -> AccountState:
        still_open_positions = []
        for pos in account.open_positions:
            bar = current_bars.get(pos.symbol)
            if bar is None:
                still_open_positions.append(pos)
                continue
            
            minute_duration = (pd.to_datetime(bar.name, utc=True) - pos.entry_time).total_seconds() / 60  # Duration in minutes
            exit_reason, gross_pnl = self._check_exit(
                pos, 
                high=float(bar["high"]), 
                low=float(bar["low"]), 
                close=float(bar["close"]),
                minute_duration=minute_duration,
            )
            
            if exit_reason is not None:
                # Exit fee calculation
                exit_price = self._get_exit_price(pos, exit_reason)
                fee_rate = self._get_exit_fee_rate(exit_reason)
                exit_fee = (exit_price * pos.qty) * fee_rate
                
                # Realize the PnL into the balance
                net_pnl = gross_pnl - exit_fee
                account.balance += net_pnl
                
                # Create the trade record
                trade = Trade(
                    **asdict(pos), 
                    exit_reason=exit_reason,
                    exit_time=pd.to_datetime(bar.name, utc=True),
                    close_price=self._get_exit_price(pos, exit_reason),
                    pnl_gross=gross_pnl,
                    pnl_net=net_pnl,
                    balance=account.balance,
                )
                account.closed_trades.append(trade)
            else:
                still_open_positions.append(pos)
                
        account.open_positions = still_open_positions
        return account
    
    def try_fill_execution(self, account: AccountState, signal: ExecutionSignal, bar: pd.Series) -> Position | None:

        lo = float(bar["low"])
        hi = float(bar["high"])
        bar_ts = pd.to_datetime(bar.name, utc=True)
        
        is_long = signal.direction == Direction.LONG
        hit_sl = (is_long and lo <= signal.sl) or (not is_long and hi >= signal.sl)
        hit_lq = (is_long and lo <= signal.liq_price) or (not is_long and hi >= signal.liq_price)
        hit_tp = (is_long and hi >= signal.tp) or (not is_long and lo <= signal.tp)
                
        # assert pd.to_datetime(bar.name, utc=True) != pd.to_datetime(signal.timestamp, utc=True), f"Bar timestamp {bar_ts} matches signal timestamp {signal.timestamp}"        
        
        if hit_sl or hit_lq:
            return None  # Reject the signal if SL or LIQ would have been hit on the entry bar
        
        if not self.allow_same_bar_exit and hit_tp:
            return None  # Reject the signal if TP would have been hit on the entry bar (unless allow_same_bar_exit is True)
        
        if lo <= signal.entry <= hi:
            entry_fee = (signal.entry * signal.qty) * self.taker_fee
            account.balance -= entry_fee
            
            return Position(
                symbol=signal.symbol,
                direction=Direction(signal.direction),
                entry_time=pd.to_datetime(bar.name, utc=True),
                entry=signal.entry,
                conviction=signal.conviction,
                sl=signal.sl,
                tp=signal.tp,
                liq_price=signal.liq_price,
                qty=signal.qty,
                notional=signal.notional,
                margin_req=signal.margin_req,
                entry_equity=None, # Placeholder: Set by AccountState when position is opened
                risk_budget=signal.risk_budget,
                risk_at_sl=signal.risk_at_sl,
                rr_at_entry=signal.rr_at_entry,
                metadata=signal.metadata,
                max_minute_duration=2*60,  # Placeholder: max duration of 2 hours
            )
        return None

    def _check_exit(self, pos: Position, high: float, low: float, close: float, minute_duration: float) -> tuple[ExitReason | None, float]:
        """ 
        Core logic for SL/TP/LIQ checks for an open position given a new high/low/close price. 
        Returns the exit reason and PnL if exiting, or (None, 0.0) if still open.
        """
        if pos.direction == Direction.SHORT:
            if high >= pos.liq_price: return ExitReason.LIQ, -pos.margin_req
            elif high >= pos.sl: return ExitReason.SL, (pos.entry - pos.sl) * pos.qty
            elif close <= pos.tp: return ExitReason.TP, (pos.entry - pos.tp) * pos.qty
            #elif minute_duration >= pos.max_minute_duration and close <= pos.entry: return ExitReason.DURATION, (pos.entry - close) * pos.qty
        else:
            assert pos.direction == Direction.LONG
            if low <= pos.liq_price: return ExitReason.LIQ, -pos.margin_req
            elif low <= pos.sl: return ExitReason.SL, (pos.sl - pos.entry) * pos.qty
            elif close >= pos.tp: return ExitReason.TP, (pos.tp - pos.entry) * pos.qty
            #elif minute_duration >= pos.max_minute_duration and close >= pos.entry: return ExitReason.DURATION, (close - pos.entry) * pos.qty
        return None, 0.0
    
    def apply_funding_rate(self, account: AccountState, funding_rates: dict[str, float], current_prices: dict[str, float]) -> AccountState:
        """ 
        Applies funding payments to the account balance based on the open positions and current funding rates. 
        """
        for pos in account.open_positions:
            qty = float(pos.qty)
            assert qty > 0.0, f"Position quantity should be positive. Got {qty} for position {pos}"
            
            close_price = float(current_prices.get(pos.symbol))
            funding_rate = float(funding_rates.get(pos.symbol))
                        
            # Calculate the funding payment
            print(f"Applying funding for {pos.symbol}: qty={qty}, close_price={close_price}, funding_rate={funding_rate}")
            payment = qty * close_price * funding_rate
            if pos.direction == Direction.LONG:
                payment = -payment  # Longs pay if funding_rate > 0, receive if funding_rate < 0
            else:
                payment = payment  # Shorts receive if funding_rate > 0, pay if funding_rate < 0
            account.balance += payment
            
            # Record the funding payment in the position metadata for tracking
            pos.metadata["cumulative_funding_payment"] = pos.metadata.get("cumulative_funding_payment", 0.0) + payment
            
        return account
    
    def _get_exit_price(self, pos: Position, reason: ExitReason) -> float:
        """ 
        Given a position and exit reason, return the price at which the position would have been closed. 
        """
        if reason == ExitReason.TP:
            return pos.tp
        if reason == ExitReason.SL:
            return pos.sl
        return pos.liq_price
    
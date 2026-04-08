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
            
            exit_reason, gross_pnl = self._check_exit(pos, hi=float(bar["high"]), lo=float(bar["low"]))
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
        hit_tp = (is_long and hi >= signal.tp) or (not is_long and lo <= signal.tp)
        
        #is_in_window = (55 > int(bar_ts.minute) > 30) or ((bar_ts.minute) <= 20)
        #if not is_in_window:
        #    return None  # Reject if not in the allowed execution window
        
        assert pd.to_datetime(bar.name, utc=True) != pd.to_datetime(signal.timestamp, utc=True), f"Bar timestamp {bar_ts} matches signal timestamp {signal.timestamp}"        
        if hit_sl:
            return None  # Reject the signal if SL would have been hit on the entry bar
        
        if not self.allow_same_bar_exit and hit_tp:
            return None  # Reject the signal if TP would have been hit on the entry bar (unless allow_same_bar_exit is True)
        
            
        if lo <= signal.entry <= hi:
            entry_fee = (signal.entry * signal.qty) * self.taker_fee
            account.balance -= entry_fee
            return Position(
                symbol=signal.symbol,
                direction=Direction(signal.direction),
                entry_time=pd.to_datetime(bar.name, utc=True), # pd.to_datetime(signal.timestamp)# TODO: or?
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
            )
        return None

    
    def _check_exit(self, pos: Position, hi: float, lo: float) -> tuple[ExitReason | None, float]:
        """ 
        Core logic for SL/TP/LIQ checks for an open position given a new hi/lo price. 
        Returns the exit reason and PnL if exiting, or (None, 0.0) if still open.
        """
        if pos.direction == Direction.SHORT:
            if hi >= pos.liq_price: return ExitReason.LIQ, -pos.margin_req
            if hi >= pos.sl: return ExitReason.SL, (pos.entry - pos.sl) * pos.qty
            if lo <= pos.tp: return ExitReason.TP, (pos.entry - pos.tp) * pos.qty
            
        else:
            assert pos.direction == Direction.LONG
            if lo <= pos.liq_price: return ExitReason.LIQ, -pos.margin_req
            if lo <= pos.sl: return ExitReason.SL, (pos.sl - pos.entry) * pos.qty
            if hi >= pos.tp: return ExitReason.TP, (pos.tp - pos.entry) * pos.qty
        return None, 0.0
    
    def _get_exit_price(self, pos: Position, reason: ExitReason) -> float:
        """ 
        Given a position and exit reason, return the price at which the position would have been closed. 
        """
        if reason == ExitReason.TP:
            return pos.tp
        if reason == ExitReason.SL:
            return pos.sl
        return pos.liq_price
    
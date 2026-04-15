from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Optional

import pandas as pd


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class ExitReason(str, Enum):
    TP = "TP"
    SL = "SL"
    LIQ = "LIQ"
    DURATION = "DURATION"

@dataclass(slots=True)
class TradeSignal:
    symbol: str
    timestamp: pd.Timestamp
    direction: Direction | str
    entry: float
    conviction: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Position:
    symbol: str
    direction: Direction
    entry_time: pd.Timestamp
    entry: float 
    conviction: float
    sl: float
    tp: float
    liq_price: float
    qty: float
    notional: float
    margin_req: float
    entry_equity: float
    risk_budget: float
    risk_at_sl: float
    rr_at_entry: float
    max_minute_duration: int # in minutes
    metadata: dict[str, Any] = field(default_factory=dict)
    
@dataclass
class PendingSetup:
    name: str
    direction: Direction
    sweep_timestamp: pd.Timestamp
    sweep_price: float
    reliability_score: float
    high_at_sweep: float
    low_at_sweep: float
    status: str = "AWAITING_MSS"  # Transitions: AWAITING_MSS -> AWAITING_FV-> AWAITING_OTE
    mss_timestamp: Optional[pd.Timestamp] = None
    anchor_high: Optional[float] = None
    anchor_low: Optional[float] = None
    mss_level: Optional[float] = None 
    fvg_timestamp: Optional[pd.Timestamp] = None
    fvg_high: Optional[float] = None
    fvg_low: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    

@dataclass(slots=True)
class Trade:
    symbol: str
    direction: Direction
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry: float
    conviction: float
    sl: float
    tp: float
    liq_price: float
    qty: float
    notional: float
    margin_req: float
    entry_equity: float
    risk_budget: float
    risk_at_sl: float
    rr_at_entry: float
    exit_reason: ExitReason
    close_price: float
    pnl_gross: float
    pnl_net: float
    balance: float
    max_minute_duration: int # in minutes
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RejectedSignal:
    signal: TradeSignal
    reason: str


@dataclass(slots=True)
class BacktestResult:
    final_balance: float
    trades: list[Trade]
    open_positions: list[Position]
    rejected_signals: list[RejectedSignal]
    equity_curve: pd.DataFrame

    def trades_df(self) -> pd.DataFrame:
        return pd.DataFrame([asdict(t) for t in self.trades])

    def rejected_signals_df(self) -> pd.DataFrame:
        rows = []
        for r in self.rejected_signals:
            rows.append(
                {
                    "reason": r.reason,
                    **asdict(r.signal),
                }
            )
        return pd.DataFrame(rows)


@dataclass
class AccountState:
    balance: float
    open_positions: list[Position] = field(default_factory=list)
    closed_trades: list[Trade] = field(default_factory=list)

    def get_equity(self, current_prices: dict[str, float]) -> float:
        unrealized_pnl = 0.0
        for pos in self.open_positions:
            price = current_prices.get(pos.symbol)
            if price is None: continue
            side = 1 if pos.direction == Direction.LONG else -1
            unrealized_pnl += (price - pos.entry) * pos.qty * side
        return self.balance + unrealized_pnl

    def get_free_margin(self, equity: float) -> float:
        locked_margin = sum(pos.margin_req for pos in self.open_positions)
        return equity - locked_margin
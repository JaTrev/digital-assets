from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd
from models.backtesting_models import Direction

@dataclass(slots=True)
class ExecutionSignal:
    """Pre-sized trade intent consumed by BacktestEngineV2.

    This object is produced by RiskManager in a separate step after strategy
    signal generation and before execution backtesting.
    """

    symbol: str
    timestamp: pd.Timestamp
    direction: Direction | str
    entry: float
    conviction: float
    sl: float
    tp: float
    qty: float
    notional: float
    margin_req: float
    liq_price: float
    risk_budget: float
    risk_at_sl: float
    rr_at_entry: float
    metadata: dict = field(default_factory=dict) 
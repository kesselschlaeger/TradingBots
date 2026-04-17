"""Kern-Datenmodelle: Bar, Signal, OrderRequest, Trade, Position."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Literal, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Bar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None


# ── Feature Vector & Signal-Hierarchie ──────────────────────────────────

@dataclass(frozen=True)
class FeatureVector:
    """Einheitlicher Feature-Vektor für ML-Filter – shared über alle Signaltypen."""
    sma_diff: float = 0.0       # (SMA_short - SMA_long) / SMA_long
    adx: float = 0.0
    atr_pct: float = 0.0        # ATR / Close
    rsi: float = 0.0
    macd_hist: float = 0.0
    z_score: float = 0.0        # Pair-Trading: Spread Z-Score; Einzelsymbol: 0.0
    volume_ratio: float = 1.0   # Volume / Vol_SMA


@dataclass(frozen=True)
class BaseSignal:
    """Gemeinsame Basis für Signal und PairSignal. Wird vom MLFilter konsumiert."""
    strategy: str
    symbol: str
    features: FeatureVector = field(default_factory=FeatureVector)
    timestamp: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class Signal(BaseSignal):
    """Einzelsymbol-Signal (ORB, OBB, Botti Trend/MR)."""
    direction: int = 0              # +1 long, -1 short, 0 flat/exit
    strength: float = 0.0           # 0.0–1.0
    stop_price: float = 0.0
    target_price: Optional[float] = None
    metadata: dict = field(default_factory=dict)

    @property
    def strategy_id(self) -> str:
        """Alias für Abwärtskompatibilität."""
        return self.strategy


@dataclass(frozen=True)
class PairSignal(BaseSignal):
    """Signal für Pair-/Multi-Symbol-Strategien."""
    long_symbol: str = ""
    short_symbol: str = ""
    z_score: float = 0.0
    action: Literal["ENTER", "EXIT", "HOLD"] = "HOLD"
    qty_pct: float = 0.05       # % of equity pro Seite
    metadata: dict = field(default_factory=dict)


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class OrderRequest:
    symbol: str
    side: OrderSide
    qty: int
    order_type: str = "market"
    limit_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    time_in_force: str = "day"
    client_order_id: Optional[str] = None


@dataclass(frozen=True)
class ExecutionResult:
    order_id: str
    qty: int
    order_type: str = "market"
    time_in_force: str = "day"


@dataclass(frozen=True)
class CloseExecution:
    symbol: str
    qty: float
    fill_price: float
    side: str
    order_id: str = ""
    realized_pnl: Optional[float] = None
    fees: Optional[float] = None


@dataclass
class Position:
    symbol: str
    qty: float
    side: str                   # "long" | "short"
    entry_price: float
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    strategy_id: Optional[str] = None
    opened_at: datetime = field(default_factory=_utcnow)


@dataclass
class Trade:
    symbol: str
    side: str                   # "BUY" | "SELL" | "SHORT" | "COVER"
    qty: float
    price: float
    timestamp: datetime = field(default_factory=_utcnow)
    pnl: float = 0.0
    strategy_id: str = ""
    reason: str = ""
    order_id: str = ""
    fees: float = 0.0
    metadata: dict = field(default_factory=dict)

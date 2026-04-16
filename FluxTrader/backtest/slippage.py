"""SlippageModel – Fixed + Percentage Slippage für Backtest-Engine."""
from __future__ import annotations

from dataclasses import dataclass

from core.models import OrderSide


@dataclass
class SlippageModel:
    """Modelliert Ausführungspreis vs. Referenzpreis.

    fixed_cents: absoluter Slippage in $ pro Aktie (z. B. 0.01 = 1c).
    percentage: zusätzlicher prozentualer Slippage (0.0005 = 5 bps).
    Beide werden additiv angewendet, gegen die Trade-Richtung.
    """

    fixed_cents: float = 0.01
    percentage: float = 0.0005

    def apply(self, price: float, side: OrderSide) -> float:
        slip = self.fixed_cents + price * self.percentage
        if side == OrderSide.BUY:
            return price + slip
        return price - slip


@dataclass
class CommissionModel:
    """Per-Share-Commission analog Alpaca/IBKR-Tier."""

    per_share: float = 0.0
    minimum: float = 0.0
    maximum_pct: float = 0.01

    def calculate(self, qty: float, price: float) -> float:
        c = max(self.per_share * qty, self.minimum)
        cap = self.maximum_pct * qty * price
        return min(c, cap) if cap > 0 else c

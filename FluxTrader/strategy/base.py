"""BaseStrategy – Broker-agnostisch, unit-testbar.

Kontrakt:
  - on_bar(bar) -> list[Signal]
  - Kein I/O, keine Broker-Aufrufe, keine Netzwerk-Calls.
  - Cross-Symbol-Kontext wird via injiziertem MarketContextService gelesen,
    niemals vom Caller vor jeder on_bar-Invokation manuell gesetzt.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

from core.context import MarketContextService, get_context_service
from core.models import Bar, Signal

# Maximale Bars im Strategie-Buffer (genügt für ATR(14), Volume_MA(20),
# OBB lookback(50) + Puffer).  Verhindert unbegrenztes Wachstum.
_DEFAULT_MAX_BARS = 2000


class BaseStrategy(ABC):
    """Pure-Function-Strategie.

    State besteht aus self.bars (rolling window). Alles weitere kommt
    über den injizierten Context-Service.
    """

    def __init__(self, config: dict,
                 context: Optional[MarketContextService] = None):
        self.config = config or {}
        self._context = context
        max_bars = int(self.config.get("max_bars_buffer", _DEFAULT_MAX_BARS))
        self.bars: deque[Bar] = deque(maxlen=max_bars)

    # ── Public API ──────────────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Strategie-ID, identisch zum @register-Namen."""

    def on_bar(self, bar: Bar) -> list[Signal]:
        self.bars.append(bar)
        # Bar auch in Shared-Context spiegeln (für Strategien, die
        # cross-symbol lesen wollen).
        self.context.push_bar(bar)
        if not self._is_ready():
            return []
        return self._generate_signals(bar)

    @abstractmethod
    def _generate_signals(self, bar: Bar) -> list[Signal]:
        """Kernlogik. Implementiert von Subklassen."""

    # ── State ──────────────────────────────────────────────────────────

    def _is_ready(self) -> bool:
        return len(self.bars) >= int(self.config.get("min_bars", 5))

    def reset(self) -> None:
        """EOD-Reset und Backtest-Re-Run."""
        self.bars.clear()

    # ── DI ─────────────────────────────────────────────────────────────

    @property
    def context(self) -> MarketContextService:
        if self._context is None:
            self._context = get_context_service()
        return self._context

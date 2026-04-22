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
from typing import Callable, Optional

from core.context import MarketContext, MarketContextService, get_context_service
from core.models import Bar, PairSignal, Signal


# Sink-Signatur: (symbol, status_code, reason) -> None.
# Der LiveRunner bindet dies an HealthState.set_symbol_status (sync).
# Strategien rufen es via self._record_status(...) in _generate_signals().
StatusSink = Callable[[str, str, str], None]

# Maximale Bars im Strategie-Buffer (genügt für ATR(14), Volume_MA(20),
# OBB lookback(50) + Puffer).  Verhindert unbegrenztes Wachstum.
_DEFAULT_MAX_BARS = 2000


class BaseStrategy(ABC):
    """Pure-Function-Strategie.

    State besteht aus self.bars (rolling window). Alles weitere kommt
    über den injizierten Context-Service.

    Context-Zugriff via Constructor-DI (nicht via set_context vor jedem Bar):
    Der Runner aktualisiert den MarketContextService kontinuierlich
    (set_now, set_spy_df, update_account) – Strategien lesen ihn lesend
    über self.context. PairStrategy verwendet stattdessen einen expliziten
    frozen MarketContext-Snapshot als Parameter – das ist das strengere
    Muster, wurde aber nur für neuen Code eingeführt. BaseStrategy bleibt
    beim Service-Zugriff, um Abwärtskompatibilität zu ORB/OBB/Botti zu
    wahren. Writer-Methoden des Service dürfen in _generate_signals()
    niemals aufgerufen werden.
    """

    def __init__(self, config: dict,
                 context: Optional[MarketContextService] = None):
        self.config = config or {}
        self._context = context or get_context_service()
        max_bars = int(self.config.get("max_bars_buffer", _DEFAULT_MAX_BARS))
        self.bars: deque[Bar] = deque(maxlen=max_bars)
        self._status_sink: Optional[StatusSink] = None

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

    def warmup_bar(self, bar: Bar) -> None:
        """Puffer befüllen, ohne Signale zu erzeugen.

        Der LiveRunner spielt damit die Session-Historie (heutige +
        Vortagesbars) in den Buffer, bevor Live-Bars eintreffen. Ohne
        diesen Warmup könnten Strategien, die auf Period-Levels
        basieren (z. B. ORB: Opening-Range 09:30–09:50 ET), nie die
        nötigen Bars sehen, wenn der Runner nach Session-Open startet.
        """
        self.bars.append(bar)
        self.context.push_bar(bar)

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

    # ── Status-Reporting (Null-Object-Pattern) ─────────────────────────

    def set_status_sink(self, sink: Optional[StatusSink]) -> None:
        """Bindet einen Sink, über den die Strategie pro-Symbol-Status
        meldet. Ohne Sink ist _record_status ein No-Op (Backtest/Tests)."""
        self._status_sink = sink

    def _record_status(self, symbol: str, code: str,
                       reason: str = "") -> None:
        sink = self._status_sink
        if sink is None:
            return
        try:
            sink(symbol, code, reason)
        except Exception:  # noqa: BLE001
            pass


# ─────────────────────────── Pair-Strategy ABC ──────────────────────────────


class PairStrategy(ABC):
    """ABC für Pair-/Multi-Symbol-Strategien.

    _generate_pair_signal erhält zwei synchrone Bars + Context-Snapshot.
    Kein Broker-Import. Kein I/O.
    """

    def __init__(self, config: dict,
                 context: Optional[MarketContextService] = None):
        self.config = config or {}
        self._context = context or get_context_service()
        self._status_sink: Optional[StatusSink] = None

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def symbol_a(self) -> str: ...

    @property
    @abstractmethod
    def symbol_b(self) -> str: ...

    @abstractmethod
    def _generate_pair_signal(
        self,
        bar_a: Bar,
        bar_b: Bar,
        snapshot: MarketContext,
    ) -> PairSignal: ...

    def reset(self) -> None:
        """EOD-Reset."""

    @property
    def context(self) -> MarketContextService:
        if self._context is None:
            self._context = get_context_service()
        return self._context

    # ── Status-Reporting (Null-Object-Pattern) ─────────────────────────

    def set_status_sink(self, sink: Optional[StatusSink]) -> None:
        """Bindet einen Sink für pro-Paar-Status. Ohne Sink No-Op."""
        self._status_sink = sink

    def _record_status(self, key: str, code: str,
                       reason: str = "") -> None:
        sink = self._status_sink
        if sink is None:
            return
        try:
            sink(key, code, reason)
        except Exception:  # noqa: BLE001
            pass

    @property
    def pair_key(self) -> str:
        """Kanonischer Key für das Pair im Status-Monitoring, z. B. 'SPY/QQQ'."""
        return f"{self.symbol_a}/{self.symbol_b}"

"""Shared MarketContextService (DI-Container).

Strategien greifen über den Service auf Cross-Symbol-Kontext zu, der
nicht in einem einzelnen Bar steckt: SPY-Historie für Trendfilter,
VIX/VIX3M für Regime, aktueller Account-Drawdown, gehaltene Positionen.

Der Runner/Backtest-Engine ist Owner des Services und füttert ihn vor
jeder Bar-Iteration. Strategien konsumieren lesend.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Deque, Optional

import pandas as pd

from core.models import Bar


@dataclass
class AccountState:
    equity: float = 0.0
    cash: float = 0.0
    buying_power: float = 0.0
    peak_equity: float = 0.0
    initial_capital: float = 10_000.0

    @property
    def drawdown(self) -> float:
        peak = max(self.peak_equity, self.initial_capital)
        if peak <= 0 or self.equity <= 0:
            return 0.0
        return max(0.0, (peak - self.equity) / peak)

    def update_equity(self, equity: float) -> None:
        self.equity = equity
        if equity > self.peak_equity:
            self.peak_equity = equity


@dataclass
class MarketContext:
    """Snapshot, wie ihn eine Strategie lesen darf."""
    now: Optional[datetime] = None
    account: AccountState = field(default_factory=AccountState)
    spy_df: Optional[pd.DataFrame] = None
    vix_spot: Optional[float] = None
    vix_3m: Optional[float] = None
    open_symbols: list[str] = field(default_factory=list)
    reserved_groups: set[str] = field(default_factory=set)


class MarketContextService:
    """DI-Container. Ein Exemplar je laufender Session (Live oder Backtest).

    Komponenten registrieren sich nicht explizit – sie rufen lesend
    `snapshot()` oder die getter ab. Writer-Methoden werden vom
    Runner/Backtest-Engine bedient, nie von Strategien.
    """

    def __init__(self, initial_capital: float = 10_000.0,
                 bar_buffer: int = 500):
        self._now: Optional[datetime] = None
        self._account = AccountState(initial_capital=initial_capital)
        self._spy_df: Optional[pd.DataFrame] = None
        self._vix_spot: Optional[float] = None
        self._vix_3m: Optional[float] = None
        self._open_symbols: list[str] = []
        self._reserved_groups: set[str] = set()
        self._bar_buffer = bar_buffer
        # Pro Symbol ein Deque der letzten Bars (für Strategien, die nicht
        # selbst buffern wollen).
        self._bars: dict[str, Deque[Bar]] = defaultdict(
            lambda: deque(maxlen=self._bar_buffer)
        )
        # Precomputed indicator DataFrames (Backtest-Optimierung).
        # Engine berechnet Indikatoren einmal vorab und stellt sie
        # Strategien per Reader zur Verfügung.
        self._indicator_frames: dict[str, pd.DataFrame] = {}
        self._bar_cursors: dict[str, int] = {}

    # ── Reader ──────────────────────────────────────────────────────────

    def snapshot(self) -> MarketContext:
        return MarketContext(
            now=self._now,
            account=AccountState(
                equity=self._account.equity,
                cash=self._account.cash,
                buying_power=self._account.buying_power,
                peak_equity=self._account.peak_equity,
                initial_capital=self._account.initial_capital,
            ),
            spy_df=self._spy_df,
            vix_spot=self._vix_spot,
            vix_3m=self._vix_3m,
            open_symbols=list(self._open_symbols),
            reserved_groups=set(self._reserved_groups),
        )

    def bars(self, symbol: str) -> list[Bar]:
        return list(self._bars.get(symbol, ()))

    @property
    def account(self) -> AccountState:
        return self._account

    @property
    def spy_df(self) -> Optional[pd.DataFrame]:
        return self._spy_df

    @property
    def vix(self) -> tuple[Optional[float], Optional[float]]:
        return self._vix_spot, self._vix_3m

    @property
    def open_symbols(self) -> list[str]:
        return list(self._open_symbols)

    @property
    def reserved_groups(self) -> set[str]:
        return set(self._reserved_groups)

    @property
    def now(self) -> Optional[datetime]:
        return self._now

    # ── Writer (nur Runner / Backtest-Engine) ──────────────────────────

    def set_now(self, ts: datetime) -> None:
        self._now = ts

    def update_account(self, equity: float, cash: float = 0.0,
                       buying_power: float = 0.0) -> None:
        self._account.cash = cash
        self._account.buying_power = buying_power
        self._account.update_equity(equity)

    def set_spy_df(self, df: Optional[pd.DataFrame]) -> None:
        self._spy_df = df

    def set_vix(self, spot: Optional[float], vix_3m: Optional[float]) -> None:
        self._vix_spot = spot
        self._vix_3m = vix_3m

    def set_open_symbols(self, symbols: list[str]) -> None:
        self._open_symbols = list(symbols)

    def reserve_group(self, group: str) -> None:
        if group:
            self._reserved_groups.add(group)

    def clear_reserved_groups(self) -> None:
        self._reserved_groups.clear()

    def set_indicator_frames(self, frames: dict[str, pd.DataFrame]) -> None:
        """Vorberechnete Indikator-DataFrames speichern (einmal pro Backtest)."""
        self._indicator_frames = frames

    def set_bar_cursor(self, symbol: str, idx: int) -> None:
        """Aktuelle Position im precomputed DataFrame setzen."""
        self._bar_cursors[symbol] = idx

    # ── Reader (Precomputed) ──────────────────────────────────────────

    def indicator_frame(self, symbol: str) -> Optional[pd.DataFrame]:
        """Liefert den vorberechneten Indikator-DataFrame für ein Symbol."""
        return self._indicator_frames.get(symbol)

    def bar_cursor(self, symbol: str) -> int:
        """Aktuelle Position im precomputed DataFrame. -1 wenn nicht gesetzt."""
        return self._bar_cursors.get(symbol, -1)

    def push_bar(self, bar: Bar) -> None:
        self._bars[bar.symbol].append(bar)

    def reset_bars(self, symbol: Optional[str] = None) -> None:
        if symbol is None:
            self._bars.clear()
        else:
            self._bars.pop(symbol, None)


# ─────────────────────────── Singleton-Zugriff ─────────────────────────────

_default_service: Optional[MarketContextService] = None


def get_context_service() -> MarketContextService:
    """Liefert den Prozess-weiten Default-Service. Falls keiner gesetzt
    wurde, wird ein Service mit Default-Parametern erzeugt.

    Für Tests/Backtests explizit einen eigenen Service via
    ``set_context_service`` injecten.
    """
    global _default_service
    if _default_service is None:
        _default_service = MarketContextService()
    return _default_service


def set_context_service(service: MarketContextService) -> None:
    global _default_service
    _default_service = service


def reset_context_service() -> None:
    global _default_service
    _default_service = None

"""Zentrales Exit-/Trailing-/EOD-Management.

Verantwortung:
  - R-basierte Exit-Levels (Stop / Target) werden aus Signal.metadata
    berechnet und als Bracket-Order an den Broker gegeben.
  - Trailing-Stop: ab trail_after_r wird Stop auf (entry + trail_distance_r)
    nachgezogen. Nur relevant für Broker, die ``modify_stop`` unterstützen
    (IBKR). Alpaca managed serverseitig.
  - EOD-Close: wenn now >= eod_close_time, werden alle Positionen geflattet.

Strategien emittieren nur Entry-Signale. Exits entstehen entweder über
die Bracket-Order oder über diesen Manager.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Optional

from core.filters import to_et_time
from core.logging import get_logger
from core.models import Position

log = get_logger(__name__)


@dataclass
class ManagedTrade:
    """Run-time-Tracking einer offenen Position mit R-Metriken."""
    symbol: str
    side: str                  # "long" | "short"
    entry: float
    stop: float
    target: Optional[float]
    qty: float
    strategy_id: str
    opened_at: datetime
    highest: float = 0.0       # für Long-Trailing
    lowest: float = 0.0        # für Short-Trailing
    current_stop: float = 0.0
    trailed: bool = False
    metadata: dict = field(default_factory=dict)

    def r_distance(self) -> float:
        return abs(self.entry - self.stop)

    def unrealized_r(self, price: float) -> float:
        r = self.r_distance()
        if r <= 0:
            return 0.0
        if self.side == "long":
            return (price - self.entry) / r
        return (self.entry - price) / r


class TradeManager:
    """Verwaltet offene Positionen und berechnet Exit-Wünsche.

    Der Runner fragt bei jedem Tick ``on_price`` und ``on_tick`` und erhält
    eine Liste von Aktionen: Stop-Modify oder Close-All-Positions.
    """

    def __init__(self,
                 trail_after_r: float = 1.0,
                 trail_distance_r: float = 0.6,
                 use_trailing: bool = False,
                 eod_close_time: Optional[time] = None,
                 market_close: time = time(16, 0)):
        self.trail_after_r = trail_after_r
        self.trail_distance_r = trail_distance_r
        self.use_trailing = use_trailing
        self.eod_close_time = eod_close_time
        self.market_close = market_close
        self._trades: dict[str, ManagedTrade] = {}

    # ── Register / Remove ─────────────────────────────────────────────

    def register(self, trade: ManagedTrade) -> None:
        self._trades[trade.symbol] = trade
        if trade.side == "long":
            trade.highest = trade.entry
        else:
            trade.lowest = trade.entry
        trade.current_stop = trade.stop
        log.info("trade.register", symbol=trade.symbol, side=trade.side,
                 entry=trade.entry, stop=trade.stop, target=trade.target)

    def forget(self, symbol: str) -> None:
        self._trades.pop(symbol, None)

    def reconcile_with_broker(self, broker_positions: dict[str, Position]) -> None:
        """Entfernt Trades, die der Broker nicht mehr kennt (z.B. durch
        serverseitigen SL/TP-Fill)."""
        alive = set(broker_positions.keys())
        stale = [s for s in self._trades if s not in alive]
        for s in stale:
            log.info("trade.stale_remove", symbol=s)
            self._trades.pop(s, None)

    def get(self, symbol: str) -> Optional[ManagedTrade]:
        return self._trades.get(symbol)

    def all_symbols(self) -> list[str]:
        return list(self._trades.keys())

    # ── Trailing ──────────────────────────────────────────────────────

    def on_price(self, symbol: str, price: float) -> Optional[float]:
        """Update Hochs/Tiefs und gib neuen Stop zurück, wenn Trailing greift.

        Return None wenn keine Änderung. Sonst: neuer Stop-Preis.
        """
        if not self.use_trailing:
            return None
        trade = self._trades.get(symbol)
        if not trade:
            return None

        r = trade.r_distance()
        if r <= 0:
            return None

        if trade.side == "long":
            if price > trade.highest:
                trade.highest = price
            favorable_r = (trade.highest - trade.entry) / r
            if favorable_r >= self.trail_after_r:
                new_stop = trade.highest - self.trail_distance_r * r
                if new_stop > trade.current_stop:
                    trade.current_stop = new_stop
                    trade.trailed = True
                    return new_stop
        else:
            if price < trade.lowest or trade.lowest == 0:
                trade.lowest = price
            favorable_r = (trade.entry - trade.lowest) / r
            if favorable_r >= self.trail_after_r:
                new_stop = trade.lowest + self.trail_distance_r * r
                if new_stop < trade.current_stop or trade.current_stop == 0:
                    trade.current_stop = new_stop
                    trade.trailed = True
                    return new_stop
        return None

    # ── Exit-Checks für Bar-basierte Engines (Backtest) ───────────────

    def check_bar_exit(self, symbol: str,
                       high: float, low: float,
                       close: float) -> Optional[tuple[str, float]]:
        """Prüft, ob SL/TP im aktuellen Bar gerissen wurde.

        Return: (reason, exit_price) oder None.
        Für Long: SL zuerst wenn Low <= stop, sonst TP wenn High >= target.
        Für Short: SL zuerst wenn High >= stop, sonst TP wenn Low <= target.
        """
        trade = self._trades.get(symbol)
        if not trade:
            return None

        if trade.side == "long":
            if low <= trade.current_stop:
                return "STOP", trade.current_stop
            if trade.target is not None and high >= trade.target:
                return "TARGET", trade.target
        else:
            if high >= trade.current_stop:
                return "STOP", trade.current_stop
            if trade.target is not None and low <= trade.target:
                return "TARGET", trade.target
        return None

    # ── EOD-Check ─────────────────────────────────────────────────────

    def should_eod_close(self, now: datetime) -> bool:
        if self.eod_close_time is None:
            return False
        t = to_et_time(now)
        return self.eod_close_time <= t < self.market_close

    def reset(self) -> None:
        self._trades.clear()

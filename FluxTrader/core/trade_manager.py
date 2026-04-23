"""Zentrales Exit-/Trailing-/EOD-Management + Trade-Persistenz.

Verantwortung:
  - R-basierte Exit-Levels (Stop / Target) werden aus Signal.metadata
    berechnet und als Bracket-Order an den Broker gegeben.
  - Trailing-Stop: ab trail_after_r wird Stop auf (entry + trail_distance_r)
    nachgezogen. Nur relevant für Broker, die ``modify_stop`` unterstützen
    (IBKR). Alpaca managed serverseitig.
  - EOD-Close: wenn now >= eod_close_time, werden alle Positionen geflattet.
  - Persistenz: Bei ``register_and_persist`` wird ein offener Trade in die
    zentrale SQLite angelegt, bei ``close_trade`` aktualisiert. Das ist
    die einzige Stelle, an der Core in die DB schreibt – Strategien
    bleiben I/O-frei (CLAUDE.md Invariante).

Strategien emittieren nur Entry-Signale. Exits entstehen entweder über
die Bracket-Order oder über diesen Manager.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, time, timezone
from typing import Any, Optional, TYPE_CHECKING

from core.filters import to_et_time
from core.logging import get_logger
from core.models import Position

if TYPE_CHECKING:  # pragma: no cover
    from core.models import BaseSignal
    from live.state import PersistentState

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
                 market_close: time = time(16, 0),
                 state: Optional["PersistentState"] = None,
                 log_registers: bool = True):
        self.trail_after_r = trail_after_r
        self.trail_distance_r = trail_distance_r
        self.use_trailing = use_trailing
        self.eod_close_time = eod_close_time
        self.market_close = market_close
        self.state = state
        self.log_registers = log_registers
        self._trades: dict[str, ManagedTrade] = {}

    # ── Register / Remove ─────────────────────────────────────────────

    def register(self, trade: ManagedTrade) -> None:
        self._trades[trade.symbol] = trade
        if trade.side == "long":
            trade.highest = trade.entry
        else:
            trade.lowest = trade.entry
        trade.current_stop = trade.stop
        if self.log_registers:
            log.info("trade.register", symbol=trade.symbol, side=trade.side,
                     entry=trade.entry, stop=trade.stop, target=trade.target)

    async def register_and_persist(
        self,
        trade: ManagedTrade,
        signal: Optional["BaseSignal"] = None,
        bot_name: Optional[str] = None,
        broker_order_id: Optional[str] = None,
        order_reference: Optional[str] = None,
    ) -> Optional[int]:
        """Registriert + persistiert den Trade in der zentralen DB.

        Extrahiert MIT-Qty-Factor, EV-Estimate, Signal-Strength und
        Feature-Vector aus ``signal``/``trade.metadata`` – entscheidend
        für probabilistische Auswertungen (Kelly, MIT-Overlay, EV).
        Gibt die neue Trade-ID zurück (als ``trade.metadata['trade_id']``
        hinterlegt), damit ``close_trade`` denselben Datensatz aktua-
        lisiert.

        Nutzt ``open_trade_atomic`` für atomare Trade+Position+Group
        Persistierung in einer einzigen DB-Transaktion."""
        self.register(trade)
        if self.state is None:
            return None

        meta = trade.metadata or {}
        sig_meta: dict[str, Any] = {}
        sig_features_json: Optional[str] = None
        strength: Optional[float] = None
        if signal is not None:
            sig_meta = dict(getattr(signal, "metadata", {}) or {})
            strength = getattr(signal, "strength", None)
            feats = getattr(signal, "features", None)
            sig_features_json = _dump_features(feats)

        mit_qty_factor = (
            _first_float(meta, "qty_factor", "mit_qty_factor")
            or _first_float(sig_meta, "qty_factor", "mit_qty_factor")
        )
        ev_estimate = (
            _first_float(meta, "ev_estimate", "expected_value", "ev")
            or _first_float(sig_meta, "ev_estimate", "expected_value", "ev")
        )
        group_name = meta.get("reserve_group") or sig_meta.get("reserve_group")
        reason = meta.get("reason") or sig_meta.get("reason")
        entry_signal = (
            sig_meta.get("entry_signal")
            or sig_meta.get("signal")
            or ("LONG" if trade.side == "long" else "SHORT")
        )

        # Reservierungsdaten für MIT-Independence
        reserve_group_name = group_name
        reserve_day = None
        if reserve_group_name:
            from datetime import date as _date
            reserve_day = trade.opened_at.date() if hasattr(trade.opened_at, "date") else _date.today()

        try:
            trade_id = await self.state.open_trade_atomic(
                strategy=trade.strategy_id,
                bot_name=bot_name,
                symbol=trade.symbol,
                side=trade.side,
                entry_ts=trade.opened_at,
                entry_price=trade.entry,
                qty=trade.qty,
                stop_price=trade.stop,
                signal_strength=strength,
                mit_qty_factor=mit_qty_factor,
                ev_estimate=ev_estimate,
                group_name=group_name,
                features_json=sig_features_json,
                reason=reason,
                current_price=trade.entry,
                entry_signal=str(entry_signal),
                broker_order_id=(str(broker_order_id) if broker_order_id else None),
                order_reference=(str(order_reference) if order_reference else None),
                reserve_group_name=reserve_group_name,
                reserve_day=reserve_day,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("trade.persist_open_failed",
                        symbol=trade.symbol, error=str(e))
            return None

        trade.metadata["trade_id"] = trade_id
        return trade_id

    def forget(self, symbol: str) -> None:
        self._trades.pop(symbol, None)

    async def close_trade(
        self,
        symbol: str,
        *,
        exit_price: float,
        exit_ts: Optional[datetime] = None,
        pnl: Optional[float] = None,
        reason: Optional[str] = None,
        tracked: Optional[ManagedTrade] = None,
    ) -> None:
        """Schließt einen Trade in DB (``trades``) + entfernt offene
        Position aus ``positions`` und entfernt ihn aus dem In-Memory-
        TradeManager.

        Nutzt ``close_trade_atomic`` für atomare Trade-Close + Position-
        DELETE in einer einzigen DB-Transaktion.

        ``tracked`` kann explizit übergeben werden, falls der Trade
        zuvor bereits aus dem In-Memory-State entfernt wurde
        (z.B. durch ``reconcile_with_broker``)."""
        tracked = tracked or self._trades.get(symbol)
        ts = exit_ts or datetime.now(timezone.utc)
        trade_id: Optional[int] = None
        strategy: Optional[str] = None
        pnl_pct: Optional[float] = None
        if tracked is not None:
            trade_id = tracked.metadata.get("trade_id")
            strategy = tracked.strategy_id
            if tracked.entry > 0 and pnl is not None and tracked.qty > 0:
                pnl_pct = (pnl / (tracked.entry * tracked.qty)) * 100.0

        if self.state is not None:
            try:
                await self.state.close_trade_atomic(
                    trade_id=trade_id,
                    strategy=strategy,
                    symbol=symbol,
                    exit_ts=ts,
                    exit_price=float(exit_price),
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    reason=reason,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("trade.persist_close_failed",
                            symbol=symbol, error=str(e))

        self.forget(symbol)

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


# ── Helpers ───────────────────────────────────────────────────────────

def _first_float(d: dict[str, Any], *keys: str) -> Optional[float]:
    for k in keys:
        v = d.get(k) if d else None
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _dump_features(feats: Any) -> Optional[str]:
    """Serialisiert einen FeatureVector (dataclass oder pydantic) zu JSON."""
    if feats is None:
        return None
    try:
        if hasattr(feats, "model_dump"):  # Pydantic v2
            return json.dumps(feats.model_dump(), default=str)
        if is_dataclass(feats):
            return json.dumps(asdict(feats), default=str)
        if isinstance(feats, dict):
            return json.dumps(feats, default=str)
    except (TypeError, ValueError):
        return None
    return None

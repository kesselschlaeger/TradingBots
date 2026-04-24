"""Anomalie-Detektion fuer den Live-Runner.

Regelbasierte Checks auf Signal-, Order- und Trade-Streams. Der
AnomalyDetector *blockiert* standardmaessig keine Orders – er emittiert
Warnungen ueber ``TelegramNotifier`` und ``log.warning``. Ausnahme:
``duplicate_hard_block=True`` – dann blockiert ``check_signal`` das
erneute Signal (Return-Wert enthaelt ``severity=CRITICAL`` und Aufrufer
ueberprueft das).

Alle fuenf Checks sind einzeln ueber ``AnomalyConfig.enabled_checks``
deaktivierbar.
"""
from __future__ import annotations

import math
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from core.config import AnomalyConfig, AppConfig
from core.filters import is_market_hours
from core.logging import get_logger
from core.models import (
    AlertLevel,
    AnomalyEvent,
    BaseSignal,
    OrderRequest,
    Trade,
)
from live.notifier import TelegramNotifier
from live.state import PersistentState

log = get_logger(__name__)


class AnomalyDetector:
    """Regelbasierte Anomalie-Erkennung auf Trade-/Signal-Stream.

    Methoden liefern eine Liste gefeuerter Events zurueck (leere Liste =
    alles i.O.). Alerts werden parallel versandt. Bei
    ``duplicate_hard_block=True`` ist das erste Tuple-Element
    ``blocked=True``.
    """

    def __init__(self, notifier: TelegramNotifier,
                 state: PersistentState,
                 cfg: AppConfig,
                 bot_name: str = "") -> None:
        self.notifier = notifier
        self.state = state
        self.cfg = cfg
        self.acfg: AnomalyConfig = cfg.anomaly
        self._bot_name: str = bot_name

        # Duplicate-Cache: (strategy, symbol, action) -> timestamp
        self._dup_cache: dict[tuple[str, str, str], datetime] = {}
        # PnL-Spike: rollendes Fenster der letzten N realisierten PnLs
        self._pnl_window: deque[float] = deque(maxlen=max(5, int(self.acfg.pnl_lookback_trades)))
        # Signal-Flood: Sliding-Window pro Strategie
        self._signal_ts: dict[str, deque[datetime]] = {}

    # ── Hilfsfunktionen ──────────────────────────────────────────────

    def _enabled(self, key: str) -> bool:
        return bool(self.acfg.enabled_checks.get(key, True))

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def _emit(self, event: AnomalyEvent) -> None:
        if not event.bot_name and self._bot_name:
            event.bot_name = self._bot_name
        log.warning("anomaly.detected", check=event.check_name,
                    severity=event.severity.value, symbol=event.symbol,
                    strategy=event.strategy, message=event.message)
        try:
            await self.state.log_anomaly(event)
        except Exception as e:  # noqa: BLE001
            log.warning("anomaly.persist_failed", error=str(e))
        try:
            await self.notifier.alert(
                level=event.severity,
                event="anomaly",
                rate_limit_key=(event.symbol or event.check_name),
                check=event.check_name,
                message=event.message,
                emoji="🚨" if event.severity == AlertLevel.CRITICAL else "⚠️",
            )
        except Exception as e:  # noqa: BLE001
            log.warning("anomaly.notifier_failed", error=str(e))

    # ── Check 1: DuplicateTradeGuard ─────────────────────────────────

    async def check_signal(self, signal: BaseSignal) -> list[AnomalyEvent]:
        events: list[AnomalyEvent] = []
        now = self._now()

        # Signal-Flood (Check 5)
        if self._enabled("signal_flood"):
            q = self._signal_ts.setdefault(signal.strategy, deque())
            q.append(now)
            window_start = now - timedelta(hours=1)
            while q and q[0] < window_start:
                q.popleft()
            if len(q) > self.acfg.max_signals_per_hour:
                ev = AnomalyEvent(
                    timestamp=now, check_name="signal_flood",
                    severity=AlertLevel.WARNING,
                    symbol=signal.symbol, strategy=signal.strategy,
                    message=(
                        f"{len(q)} Signale in der letzten Stunde "
                        f"(Limit {self.acfg.max_signals_per_hour})"
                    ),
                    context={"count": len(q)},
                )
                events.append(ev)
                await self._emit(ev)

        # DuplicateTrade (Check 1) – action aus metadata oder direction
        if self._enabled("duplicate_trade"):
            action = self._extract_action(signal)
            key = (signal.strategy, signal.symbol, action)
            window = timedelta(minutes=self.acfg.duplicate_window_minutes)
            last = self._dup_cache.get(key)
            if last is not None and (now - last) < window:
                severity = (AlertLevel.CRITICAL if self.acfg.duplicate_hard_block
                            else AlertLevel.WARNING)
                ev = AnomalyEvent(
                    timestamp=now, check_name="duplicate_trade",
                    severity=severity,
                    symbol=signal.symbol, strategy=signal.strategy,
                    message=(
                        f"Duplicate {action} innerhalb "
                        f"{self.acfg.duplicate_window_minutes} Min"
                    ),
                    context={
                        "last_ts": last.isoformat(),
                        "hard_block": self.acfg.duplicate_hard_block,
                    },
                )
                events.append(ev)
                await self._emit(ev)
            else:
                self._dup_cache[key] = now

        return events

    @staticmethod
    def _extract_action(signal: BaseSignal) -> str:
        action = getattr(signal, "action", None)
        if action:
            return str(action)
        direction = getattr(signal, "direction", 0)
        if direction > 0:
            return "LONG"
        if direction < 0:
            return "SHORT"
        return "FLAT"

    def should_block(self, events: list[AnomalyEvent]) -> bool:
        """True wenn unter den Events ein hard-block-relevanter ist."""
        if not self.acfg.duplicate_hard_block:
            return False
        return any(e.check_name == "duplicate_trade"
                   and e.severity == AlertLevel.CRITICAL
                   for e in events)

    # ── Check 2: OversizedOrderGuard ─────────────────────────────────

    async def check_order(self, order: OrderRequest, result: dict) -> list[AnomalyEvent]:
        if not self._enabled("oversized_order"):
            return []
        events: list[AnomalyEvent] = []
        now = self._now()
        price = float(result.get("fill_price") or result.get("price") or 0.0)
        equity = float(result.get("equity") or 0.0)
        avg_daily_volume = float(result.get("avg_daily_volume") or 0.0)

        order_value = float(order.qty) * price
        if equity > 0 and price > 0:
            pct = order_value / equity
            if pct > self.acfg.max_single_order_pct:
                ev = AnomalyEvent(
                    timestamp=now, check_name="oversized_order",
                    severity=AlertLevel.WARNING,
                    symbol=order.symbol,
                    message=(
                        f"Ordervolumen ${order_value:,.0f} = {pct*100:.1f}% "
                        f"des Equity (Limit {self.acfg.max_single_order_pct*100:.0f}%)"
                    ),
                    context={"order_value": order_value, "equity": equity,
                             "pct": pct},
                )
                events.append(ev)
                await self._emit(ev)

        if avg_daily_volume > 0:
            vpct = float(order.qty) / avg_daily_volume
            if vpct > self.acfg.max_volume_pct:
                ev = AnomalyEvent(
                    timestamp=now, check_name="oversized_order",
                    severity=AlertLevel.WARNING,
                    symbol=order.symbol,
                    message=(
                        f"Order = {vpct*100:.2f}% des Tagesvolumens "
                        f"(Limit {self.acfg.max_volume_pct*100:.2f}%)"
                    ),
                    context={"volume_pct": vpct},
                )
                events.append(ev)
                await self._emit(ev)
        return events

    # ── Check 3: PnLSpikeDetector ────────────────────────────────────

    async def check_trade_result(self, trade: Trade) -> list[AnomalyEvent]:
        if not self._enabled("pnl_spike"):
            self._pnl_window.append(float(trade.pnl))
            return []
        events: list[AnomalyEvent] = []
        now = self._now()
        pnl = float(trade.pnl)

        if len(self._pnl_window) >= 5:
            mean = sum(self._pnl_window) / len(self._pnl_window)
            var = sum((x - mean) ** 2 for x in self._pnl_window) / len(self._pnl_window)
            std = math.sqrt(var)
            if std > 0:
                z = abs(pnl - mean) / std
                if z > self.acfg.pnl_spike_sigma:
                    ev = AnomalyEvent(
                        timestamp=now, check_name="pnl_spike",
                        severity=AlertLevel.WARNING,
                        symbol=trade.symbol, strategy=trade.strategy_id,
                        message=(
                            f"PnL ${pnl:+.2f} weicht {z:.1f}σ vom Mittel "
                            f"(${mean:+.2f}, σ={std:.2f}) ab"
                        ),
                        context={"z": z, "mean": mean, "std": std, "pnl": pnl},
                    )
                    events.append(ev)
                    await self._emit(ev)

        self._pnl_window.append(pnl)
        return events

    # ── Check 4: ConnectivityWatchdog ────────────────────────────────

    async def check_heartbeat(self, strategy: str,
                              last_bar_ts: Optional[datetime]) -> list[AnomalyEvent]:
        if not self._enabled("connectivity"):
            return []
        events: list[AnomalyEvent] = []
        now = self._now()
        if not is_market_hours(now):
            return []
        if last_bar_ts is None:
            return []
        if last_bar_ts.tzinfo is None:
            last_bar_ts = last_bar_ts.replace(tzinfo=timezone.utc)
        gap_min = (now - last_bar_ts).total_seconds() / 60.0
        if gap_min > self.acfg.bar_gap_minutes:
            ev = AnomalyEvent(
                timestamp=now, check_name="connectivity",
                severity=AlertLevel.CRITICAL,
                strategy=strategy,
                message=(
                    f"Kein Bar fuer {gap_min:.1f} Min "
                    f"(Limit {self.acfg.bar_gap_minutes} Min)"
                ),
                context={"gap_minutes": gap_min,
                         "last_bar_ts": last_bar_ts.isoformat()},
            )
            events.append(ev)
            log.error("anomaly.connectivity", strategy=strategy,
                      gap_minutes=gap_min)
            await self._emit(ev)
        return events

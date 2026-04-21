"""Health-Check- und Status-Endpunkt fuer den Live-Runner.

Minimaler async HTTP-Server via ``aiohttp`` (mit Fallback auf stdlib
``http.server`` wenn aiohttp nicht installiert ist). Kein Import von
``execution/`` – der HealthState wird ausschliesslich vom Runner bzw. den
Broker-Adaptern gefuettert (Writer/Reader-Trennung analog zu
``MarketContextService``).

Endpunkte:
  GET /health         Liveness (immer 200 solange der Task laeuft)
  GET /ready          Readiness (Broker connected + letzter Bar < 10 Min)
  GET /status         Vollstaendiger JSON-Snapshot
  GET /metrics/text   Prometheus-Text (delegiert an MetricsCollector)
"""
from __future__ import annotations

import asyncio
import json
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, Optional

from core.logging import get_logger
from core.filters import is_market_hours

log = get_logger(__name__)

try:
    from aiohttp import web  # type: ignore
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    AIOHTTP_AVAILABLE = False


_READY_BAR_MAX_AGE_S = 600          # letzter Bar muss juenger als 10 Min sein
_STATUS_OK_BAR_LAG_MS = 5_000
_STATUS_DEGRADED_BAR_LAG_MS = 30_000


class HealthState:
    """Zentraler Zustandsspeicher fuer Health-/Status-Daten.

    Writer-Methoden werden vom LiveRunner / PairEngine / BrokerPort-Adapter
    aufgerufen. Reader-Methoden sind idempotent, nicht-blockierend und
    koennen von HTTP-Handlern sowie Collector/Detector gelesen werden.
    """

    def __init__(self,
                 on_ready_alert: Optional[Callable[..., Coroutine]] = None,
                 bar_max_age_seconds: int = _READY_BAR_MAX_AGE_S) -> None:
        self._start_ts = _time.time()
        self._lock = asyncio.Lock()
        self._bar_max_age_seconds = max(1, int(bar_max_age_seconds))

        self._broker_connected: bool = False
        self._broker_adapter: str = ""
        self._last_order_ms: Optional[float] = None

        self._last_bar_ts: dict[str, datetime] = {}
        self._last_bar_lag_ms: dict[str, float] = {}
        self._signals_today: dict[str, int] = {}
        self._signals_filtered_today: dict[str, int] = {}

        # Pro-Symbol-Status: {strategy: {symbol: {code, reason, ts}}}
        # Synchroner Writer (siehe set_symbol_status) – wird von Strategien
        # innerhalb des Event-Loops aufgerufen, kein Lock nötig.
        self._symbol_status: dict[str, dict[str, dict[str, Any]]] = {}

        self._equity: float = 0.0
        self._cash: float = 0.0
        self._open_positions: int = 0
        self._drawdown_pct: float = 0.0
        self._peak_equity: float = 0.0

        self._circuit_breaker: bool = False
        self._on_ready_alert = on_ready_alert  # Callback für Alerts
        self._last_alert_ts: Optional[float] = None
        self._alert_cooldown_s = 60  # Verhindert Alert-Spam

    # ── Writer ────────────────────────────────────────────────────────

    async def set_last_bar(self, strategy: str, ts: datetime,
                           lag_ms: float) -> None:
        async with self._lock:
            self._last_bar_ts[strategy] = _ensure_aware(ts)
            self._last_bar_lag_ms[strategy] = float(lag_ms)

    async def set_broker_status(self, connected: bool, adapter: str,
                                last_order_ms: Optional[float] = None) -> None:
        async with self._lock:
            self._broker_connected = bool(connected)
            self._broker_adapter = str(adapter)
            if last_order_ms is not None:
                self._last_order_ms = float(last_order_ms)

    async def record_signal(self, strategy: str, filtered: bool) -> None:
        async with self._lock:
            if filtered:
                self._signals_filtered_today[strategy] = \
                    self._signals_filtered_today.get(strategy, 0) + 1
            else:
                self._signals_today[strategy] = \
                    self._signals_today.get(strategy, 0) + 1

    async def reset_daily_counters(self) -> None:
        async with self._lock:
            self._signals_today.clear()
            self._signals_filtered_today.clear()

    async def update_portfolio(self, equity: float, cash: float,
                               drawdown_pct: float,
                               open_positions: Optional[int] = None,
                               peak_equity: Optional[float] = None) -> None:
        async with self._lock:
            self._equity = float(equity)
            self._cash = float(cash)
            self._drawdown_pct = float(drawdown_pct)
            if open_positions is not None:
                self._open_positions = int(open_positions)
            if peak_equity is not None:
                self._peak_equity = float(peak_equity)

    async def set_circuit_breaker(self, active: bool) -> None:
        async with self._lock:
            self._circuit_breaker = bool(active)

    def set_symbol_status(self, strategy: str, symbol: str,
                          code: str, reason: str = "",
                          ts: Optional[datetime] = None) -> None:
        """Synchroner Writer für Pro-Symbol-Status.

        Strategien rufen dies synchron aus _generate_signals() auf.
        Da der LiveRunner single-threaded im Event-Loop laeuft, ist kein
        Lock noetig.
        """
        bucket = self._symbol_status.setdefault(strategy, {})
        bucket[symbol.upper()] = {
            "code": str(code),
            "reason": str(reason or ""),
            "ts": _ensure_aware(ts or datetime.now(timezone.utc)).isoformat(),
        }

    # ── Reader (nicht async – immer Momentaufnahme) ───────────────────

    def snapshot(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        strategies: list[dict[str, Any]] = []
        all_names = (set(self._last_bar_ts)
                     | set(self._signals_today)
                     | set(self._signals_filtered_today)
                     | set(self._symbol_status))
        for name in sorted(all_names):
            ts = self._last_bar_ts.get(name)
            strategies.append({
                "name": name,
                "last_bar_ts": ts.isoformat() if ts else None,
                "last_bar_lag_ms": self._last_bar_lag_ms.get(name),
                "signals_today": self._signals_today.get(name, 0),
                "signals_filtered_today":
                    self._signals_filtered_today.get(name, 0),
                "symbol_status": self._symbol_status.get(name, {}),
            })

        return {
            "timestamp": now.isoformat(),
            "status": self.overall_status(),
            "uptime_seconds": int(_time.time() - self._start_ts),
            "broker": {
                "connected": self._broker_connected,
                "last_order_ms": self._last_order_ms,
                "adapter": self._broker_adapter,
            },
            "strategies": strategies,
            "portfolio": {
                "equity": self._equity,
                "cash": self._cash,
                "open_positions": self._open_positions,
                "drawdown_pct": self._drawdown_pct,
                "peak_equity": self._peak_equity,
            },
            "circuit_breaker_active": self._circuit_breaker,
        }

    def is_ready(self) -> bool:
        """Readiness Check: Broker + Bars aktuell (außerhalb Handelszeiten locker).

        Außerhalb Handelszeiten: nur Broker-Check (Bars dürfen alt sein).
        Innerhalb Handelszeiten: Broker + Bars < 10 Min.
        """
        if not self._broker_connected:
            return False
        if not self._last_bar_ts:
            return True  # noch keine Bars, aber Broker connected

        now = datetime.now(timezone.utc)
        # Außerhalb der Handelszeiten: akzeptiere alte Bars
        if not is_market_hours(now):
            return True  # Broker OK, Bars können alt sein außerhalb Handelszeiten

        # Innerhalb Handelszeiten: Bars müssen frisch sein
        for ts in self._last_bar_ts.values():
            if (now - _ensure_aware(ts)).total_seconds() > self._bar_max_age_seconds:
                return False
        return True

    def should_alert_on_not_ready(self) -> bool:
        """True wenn 503 während Handelszeiten (Fehler, nicht Normal-Zustand).

        Nutze dies um Telegram-Alerts zu triggern.
        """
        if not is_market_hours(datetime.now(timezone.utc)):
            return False  # Außerhalb Handelszeiten: kein Alert
        if self._broker_connected and self._last_bar_ts:
            now = datetime.now(timezone.utc)
            for ts in self._last_bar_ts.values():
                if (now - _ensure_aware(ts)).total_seconds() > self._bar_max_age_seconds:
                    return True  # Bars zu alt während Handelszeiten → Alert
        return False

    def overall_status(self) -> str:
        if self._circuit_breaker or not self._broker_connected:
            return "critical"
        # Während Handelszeiten: 503 (is_ready=False) → critical
        if is_market_hours(datetime.now(timezone.utc)) and not self.is_ready():
            return "critical"
        # Bar-Lag bewerten (nur wenn Bars vorhanden)
        if self._last_bar_lag_ms:
            worst = max(self._last_bar_lag_ms.values())
            if worst > _STATUS_DEGRADED_BAR_LAG_MS:
                return "degraded"
            if worst > _STATUS_OK_BAR_LAG_MS:
                return "degraded"
        return "ok"


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ── HTTP-Server (aiohttp) ─────────────────────────────────────────────

async def start_health_server(health_state: HealthState,
                              metrics_collector: Optional[Any] = None,
                              port: int = 8090,
                              host: str = "0.0.0.0") -> Optional[Any]:
    """Startet den aiohttp-Server und gibt das Runner-Handle zurueck.

    Bei fehlendem aiohttp wird nur gewarnt und ``None`` zurueckgegeben.
    """
    if not AIOHTTP_AVAILABLE:
        log.warning("health.disabled",
                    reason="aiohttp missing – pip install aiohttp")
        return None

    async def _health(_req: "web.Request") -> "web.Response":
        return web.json_response({"status": "alive"})

    async def _ready(_req: "web.Request") -> "web.Response":
        ok = health_state.is_ready()
        payload = {"ready": ok, "status": health_state.overall_status()}
        return web.json_response(payload, status=200 if ok else 503)

    async def _status(_req: "web.Request") -> "web.Response":
        return web.json_response(health_state.snapshot())

    async def _metrics_text(_req: "web.Request") -> "web.Response":
        if metrics_collector is None:
            return web.Response(text="# metrics disabled\n",
                                content_type="text/plain")
        try:
            text = metrics_collector.generate_text()
        except Exception as e:  # noqa: BLE001
            log.warning("health.metrics_error", error=str(e))
            return web.Response(status=500, text=f"# error: {e}\n",
                                content_type="text/plain")
        return web.Response(text=text, content_type="text/plain")

    app = web.Application()
    app.router.add_get("/health", _health)
    app.router.add_get("/ready", _ready)
    app.router.add_get("/status", _status)
    app.router.add_get("/metrics/text", _metrics_text)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    log.info("health.started", host=host, port=port)
    return runner


async def snapshot_json(health_state: HealthState) -> str:
    """Hilfsfunktion fuer Tests ohne HTTP-Stack."""
    return json.dumps(health_state.snapshot(), default=str)

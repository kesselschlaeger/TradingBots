"""Health-Check- und Status-Endpunkt fuer den Live-Runner.

Minimaler async HTTP-Server via ``aiohttp`` (mit Fallback auf stdlib
``http.server`` wenn aiohttp nicht installiert ist). Kein Import von
``execution/`` – der HealthState wird ausschliesslich vom Runner bzw. den
Broker-Adaptern gefuettert (Writer/Reader-Trennung analog zu
``MarketContextService``).

Portfolio-Daten (Equity, Drawdown, Positions, PnL) werden seit dem
state.py-Refactoring ausschliesslich per ``get_health_snapshot()`` aus
SQLite gelesen (≤5s TTL-Cache). HealthState ist damit ein reiner
Read-Cache fuer Portfolio-Metriken. Nur Bar-Lag, Broker-Status und
Circuit-Breaker kommen weiterhin als In-Memory-Writer vom LiveRunner.

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
from contextlib import suppress
from datetime import datetime, time, timedelta, timezone
from typing import Any, Callable, Coroutine, Optional, TYPE_CHECKING

from core.logging import get_logger
from core.filters import (
    is_after_entry_cutoff,
    is_after_eod_close,
    is_before_premarket,
    is_within_trade_window,
    timeframe_to_seconds,
    to_et,
)

if TYPE_CHECKING:  # pragma: no cover
    from live.state import PersistentState

log = get_logger(__name__)

try:
    from aiohttp import web  # type: ignore
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    AIOHTTP_AVAILABLE = False


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
                 strategy_config: Optional[dict[str, Any]] = None,
                 monitoring_config: Optional[Any] = None,
                 persistent_state: Optional["PersistentState"] = None,
                 cache_ttl_seconds: float = 5.0,
                 bot_name: str = "",
                 data_timeframe: Optional[str] = None) -> None:
        self._start_ts = _time.time()
        self._lock = asyncio.Lock()
        self._strategy_cfg = strategy_config or {}
        self._monitoring_cfg = monitoring_config

        self._watchdog_interval_s = max(1, self._monitoring_int("watchdog_interval_s", 15))
        # Auto-Ableitung: Wenn weder strategy_cfg noch monitoring_cfg einen
        # expliziten Wert (>0) setzen, berechnen wir den Bar-Takt aus dem
        # Strategie-/Data-Timeframe. Vermeidet Fehlalarme wie "Kein Bar für
        # 15 Min" bei Daily-Strategien, wo 300s-Defaults unsinnig sind.
        tf_fallback = (
            str(self._strategy_cfg.get("timeframe")
                or data_timeframe
                or "5Min")
        )
        explicit_bar_tf = (
            self._strategy_cfg.get("bar_timeframe_seconds")
            or self._monitoring_int("bar_timeframe_seconds", 0)
        )
        self._bar_timeframe_seconds = max(
            1,
            int(explicit_bar_tf) if explicit_bar_tf and int(explicit_bar_tf) > 0
            else int(timeframe_to_seconds(tf_fallback)),
        )
        self._provider_poll_interval_s = max(
            0,
            int(self._strategy_cfg.get("provider_poll_interval_s")
                or self._monitoring_int("provider_poll_interval_s", 30)),
        )
        explicit_stale = (
            self._strategy_cfg.get("stale_tolerance_s")
            or self._monitoring_int("stale_tolerance_s", 0)
        )
        if explicit_stale and int(explicit_stale) > 0:
            self._stale_tolerance_s = int(explicit_stale)
        else:
            # 25 % des Bar-Intervalls, mindestens 60 s. Bei Daily-Bars (86400 s)
            # resultiert das in 21600 s = 6 h – vernünftig als Toleranz.
            self._stale_tolerance_s = max(60, int(self._bar_timeframe_seconds * 0.25))
        self._grace_period_s = max(0, self._monitoring_int("grace_period_s", 90))

        self._broker_connected: bool = False
        self._broker_adapter: str = ""
        self._last_order_ms: Optional[float] = None

        self._last_bar_ts: dict[str, datetime] = {}
        self._last_watchdog_ts: dict[str, datetime] = {}
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
        self._bot_name: str = bot_name

        # Read-Cache: Portfolio-Daten werden aus SQLite gelesen (TTL-Cache).
        self._persistent_state = persistent_state
        self._cache_ttl = max(0.1, float(cache_ttl_seconds))
        self._db_cache: dict[tuple[str, str], dict[str, Any]] = {}  # (bot_name, strategy) → snapshot
        self._db_cache_ts: dict[tuple[str, str], float] = {}  # (bot_name, strategy) → monotonic ts

    # ── DB-Cache (Read-Only) ──────────────────────────────────────────

    async def refresh_from_db(self, strategy: str) -> Optional[dict[str, Any]]:
        """Liest den Health-Snapshot aus SQLite (≤TTL-Cache).

        Gibt den gecachten/frischen Snapshot zurück oder None falls kein
        PersistentState konfiguriert ist.
        """
        if self._persistent_state is None:
            return None
        cache_key = (self._bot_name, strategy)
        now = _time.monotonic()
        cached_ts = self._db_cache_ts.get(cache_key, 0.0)
        if (now - cached_ts) < self._cache_ttl:
            return self._db_cache.get(cache_key)

        try:
            snap = await self._persistent_state.get_health_snapshot(
                self._bot_name, strategy
            )
        except Exception as e:  # noqa: BLE001
            log.warning("health.db_refresh_failed", strategy=strategy,
                        bot_name=self._bot_name, error=str(e))
            return self._db_cache.get(cache_key)

        self._db_cache[cache_key] = snap
        self._db_cache_ts[cache_key] = now

        # Portfolio-Felder aus DB-Cache in In-Memory-State übernehmen
        async with self._lock:
            if snap.get("equity") is not None:
                self._equity = float(snap["equity"])
            if snap.get("cash") is not None:
                self._cash = float(snap["cash"])
            if snap.get("drawdown_pct") is not None:
                self._drawdown_pct = float(snap["drawdown_pct"])
            if snap.get("peak_equity") is not None:
                self._peak_equity = float(snap["peak_equity"])
            if snap.get("open_positions") is not None:
                self._open_positions = int(snap["open_positions"])
        return snap

    # ── Writer ────────────────────────────────────────────────────────

    async def set_last_bar(self, strategy: str, ts: datetime,
                           lag_ms: float) -> None:
        async with self._lock:
            self._last_bar_ts[strategy] = _ensure_aware(ts)
            self._last_bar_lag_ms[strategy] = float(lag_ms)

    async def set_watchdog(self, strategy: str, ts: datetime) -> None:
        async with self._lock:
            self._last_watchdog_ts[strategy] = _ensure_aware(ts)

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
        """Update Portfolio-Daten.

        Wenn ein PersistentState konfiguriert ist, ist dies ein No-Op
        (Portfolio-Daten kommen dann via refresh_from_db aus SQLite).
        Ohne PersistentState bleibt das alte Verhalten erhalten.
        """
        if self._persistent_state is not None:
            return  # Portfolio kommt aus DB-Cache
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

    def is_broker_connected(self) -> bool:
        return self._broker_connected

    def is_circuit_breaker_active(self) -> bool:
        return self._circuit_breaker

    def get_symbol_status(self, strategy: str) -> dict[str, Any]:
        return dict(self._symbol_status.get(strategy, {}))

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
                     | set(self._last_watchdog_ts)
                     | set(self._signals_today)
                     | set(self._signals_filtered_today)
                     | set(self._symbol_status))
        for name in sorted(all_names):
            ts = self._last_bar_ts.get(name)
            wd = self._last_watchdog_ts.get(name)
            now = datetime.now(timezone.utc)
            expected = self.next_expected_bar_at(ts)
            phase = self.trade_window_phase(now)
            in_window = phase == "in_window"
            process_alive = self._is_process_alive(name, now)
            data_flowing = self._is_data_flowing(name, now, in_window, expected)
            strategies.append({
                "name": name,
                "bot_name": self._bot_name,
                "last_bar_ts": ts.isoformat() if ts else None,
                "last_watchdog_ts": wd.isoformat() if wd else None,
                "next_expected_bar_at": expected.isoformat() if expected else None,
                "seconds_to_next_bar": self._seconds_to_next_bar(expected, now),
                "trade_window": self._trade_window_payload(phase),
                "in_trade_window": in_window,
                "process_alive": process_alive,
                "data_flowing": data_flowing,
                "overall_state": self.overall_state(name, now=now),
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
        """Readiness ist die zentrale Auswertung aus overall_state()."""
        if not self._broker_connected:
            return False
        now = datetime.now(timezone.utc)
        for name in self._last_bar_ts:
            if self.overall_state(name, now=now) in {
                "PROCESS_DEAD", "DATA_STALE", "CIRCUIT_BREAK",
            }:
                return False
        return True

    def should_alert_on_not_ready(self) -> bool:
        now = datetime.now(timezone.utc)
        for name in self._last_bar_ts:
            if self.overall_state(name, now=now) in {"PROCESS_DEAD", "DATA_STALE"}:
                return True
        return False

    def overall_status(self) -> str:
        if self._circuit_breaker or not self._broker_connected:
            return "critical"
        now = datetime.now(timezone.utc)
        for name in self._last_bar_ts:
            state = self.overall_state(name, now=now)
            if state in {"PROCESS_DEAD", "DATA_STALE", "CIRCUIT_BREAK"}:
                return "critical"
            if state == "IDLE_OUT_OF_WINDOW":
                continue
        # Bar-Lag bewerten (nur wenn Bars vorhanden)
        if self._last_bar_lag_ms:
            worst = max(self._last_bar_lag_ms.values())
            if worst > _STATUS_DEGRADED_BAR_LAG_MS:
                return "degraded"
            if worst > _STATUS_OK_BAR_LAG_MS:
                return "degraded"
        return "ok"

    def next_expected_bar_at(self, last_bar_ts: Optional[datetime]) -> Optional[datetime]:
        if last_bar_ts is None:
            return None
        return _ensure_aware(last_bar_ts) + timedelta(
            seconds=(
                self._bar_timeframe_seconds
                + self._provider_poll_interval_s
                + self._stale_tolerance_s
            )
        )

    def trade_window_phase(self, now: datetime) -> str:
        now_utc = _ensure_aware(now)
        if is_before_premarket(self._strategy_cfg, now_utc):
            return "before_premarket"
        if is_after_eod_close(self._strategy_cfg, now_utc):
            return "after_eod"
        if is_after_entry_cutoff(self._strategy_cfg, now_utc):
            return "after_cutoff"

        now_et = to_et(now_utc)
        open_t = self._cfg_time("market_open_time", default=(9, 30))
        open_dt = now_et.replace(
            hour=open_t.hour,
            minute=open_t.minute,
            second=0,
            microsecond=0,
        )
        cutoff_t = self._cfg_time("entry_cutoff_time", default=(15, 0))
        window_minutes = max(
            1,
            int((cutoff_t.hour * 60 + cutoff_t.minute) - (open_t.hour * 60 + open_t.minute)),
        )
        if is_within_trade_window(now_et, open_dt, window_minutes=window_minutes):
            return "in_window"
        return "out_of_window"

    def overall_state(self, strategy: str, now: Optional[datetime] = None) -> str:
        cur = _ensure_aware(now or datetime.now(timezone.utc))
        if self._circuit_breaker:
            return "CIRCUIT_BREAK"

        phase = self.trade_window_phase(cur)
        in_window = (phase == "in_window") or self._phase_alert_enabled(phase)
        expected = self.next_expected_bar_at(self._last_bar_ts.get(strategy))
        process_alive = self._is_process_alive(strategy, cur)
        data_flowing = self._is_data_flowing(strategy, cur, in_window, expected)

        if not process_alive:
            return "PROCESS_DEAD"
        if not in_window:
            return "IDLE_OUT_OF_WINDOW"
        if not data_flowing:
            return "DATA_STALE"
        return "OK"

    def _is_process_alive(self, strategy: str, now: datetime) -> bool:
        wd = self._last_watchdog_ts.get(strategy)
        if wd is None:
            return True
        return (now - wd).total_seconds() < (3 * self._watchdog_interval_s)

    def _is_data_flowing(self, strategy: str, now: datetime,
                         in_window: bool,
                         expected: Optional[datetime]) -> bool:
        if not in_window:
            return True
        if expected is None:
            return False
        return now <= (expected + timedelta(seconds=self._grace_period_s))

    @staticmethod
    def _seconds_to_next_bar(expected: Optional[datetime], now: datetime) -> Optional[int]:
        if expected is None:
            return None
        return int((expected - now).total_seconds())

    def _trade_window_payload(self, phase: str) -> dict[str, str]:
        open_t = self._cfg_time("market_open_time", default=(9, 30))
        cutoff_t = self._cfg_time("entry_cutoff_time", default=(15, 0))
        return {
            "start": f"{open_t.hour:02d}:{open_t.minute:02d}",
            "end": f"{cutoff_t.hour:02d}:{cutoff_t.minute:02d}",
            "phase": phase,
        }

    def _monitoring_int(self, key: str, default: int) -> int:
        cfg = self._monitoring_cfg
        if cfg is None:
            return int(default)
        try:
            if isinstance(cfg, dict):
                return int(cfg.get(key, default))
            return int(getattr(cfg, key, default))
        except Exception:
            return int(default)

    def _phase_alert_enabled(self, phase: str) -> bool:
        phases = getattr(self._monitoring_cfg, "trade_window_phases", None)
        if phases is None:
            return False
        mapping = {
            "before_premarket": "premarket_alert",
            "after_cutoff": "after_cutoff_alert",
            "after_eod": "after_eod_alert",
        }
        key = mapping.get(phase)
        if key is None:
            return False
        try:
            return bool(getattr(phases, key, False))
        except Exception:
            return False

    def _cfg_time(self, key: str, default: tuple[int, int]) -> time:
        raw = self._strategy_cfg.get(key)
        if raw is None:
            return datetime(2000, 1, 1, default[0], default[1]).time()
        if hasattr(raw, "hour") and hasattr(raw, "minute"):
            return raw
        if isinstance(raw, str) and ":" in raw:
            hh, mm = raw.split(":", 1)
            return datetime(2000, 1, 1, int(hh), int(mm)).time()
        return datetime(2000, 1, 1, default[0], default[1]).time()


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ── HTTP-Server (aiohttp) ─────────────────────────────────────────────

async def start_health_server(health_state: HealthState,
                              metrics_collector: Optional[Any] = None,
                              port: int = 8090,
                              host: str = "0.0.0.0",
                              fallback_ports: tuple[int, ...] = ()) -> Optional[Any]:
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

    ports_to_try = (int(port),) + tuple(int(p) for p in fallback_ports)
    for current_port in ports_to_try:
        runner = web.AppRunner(app, access_log=None)
        try:
            await runner.setup()
            site = web.TCPSite(runner, host=host, port=current_port)
            await site.start()
            log.info("health.started", host=host, port=current_port)
            return runner
        except OSError as e:
            log.warning("health.port_in_use", port=current_port, error=str(e))
            with suppress(Exception):
                await runner.cleanup()

    log.error("health.no_port_available", tried=ports_to_try)
    return None


async def snapshot_json(health_state: HealthState) -> str:
    """Hilfsfunktion fuer Tests ohne HTTP-Stack."""
    return json.dumps(health_state.snapshot(), default=str)

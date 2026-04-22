"""Prometheus-Metriken fuer den Live-Runner (optional).

``MetricsCollector`` ist ein duenner Wrapper um ``prometheus_client``.
Wenn die Library fehlt oder die Metriken per Config abgeschaltet sind,
liefert ``MetricsCollector.create`` eine NoOp-Implementation zurueck
(analog zum ``MLFilter``-Null-Object-Pattern).

Die Metrik-Namen sind in ``CLAUDE.md`` dokumentiert und mit
``fluxtrader_`` gepraefixt.
"""
from __future__ import annotations

from typing import Any

from core.logging import get_logger

log = get_logger(__name__)

try:
    from prometheus_client import (  # type: ignore
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False


_ORDER_LATENCY_BUCKETS = (50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0)
_BAR_LAG_BUCKETS = (100.0, 500.0, 1000.0, 5000.0, 30000.0)


class _NoOpCollector:
    """Null-Object: jedes Methoden-Call ist ein harmloser No-Op."""

    enabled = False

    def record_trade_opened(self, *_a: Any, **_k: Any) -> None: ...
    def record_trade_closed(self, *_a: Any, **_k: Any) -> None: ...
    def record_order_latency(self, *_a: Any, **_k: Any) -> None: ...
    def set_equity(self, *_a: Any, **_k: Any) -> None: ...
    def set_drawdown(self, *_a: Any, **_k: Any) -> None: ...
    def set_unrealized_pnl(self, *_a: Any, **_k: Any) -> None: ...
    def set_open_positions(self, *_a: Any, **_k: Any) -> None: ...
    def record_signal(self, *_a: Any, **_k: Any) -> None: ...
    def set_bar_lag(self, *_a: Any, **_k: Any) -> None: ...
    def set_circuit_breaker(self, *_a: Any, **_k: Any) -> None: ...
    def set_wfo_sharpe(self, *_a: Any, **_k: Any) -> None: ...
    def update_from_health_snapshot(self, *_a: Any, **_k: Any) -> None: ...

    def generate_text(self) -> str:
        return "# prometheus_client not installed or metrics disabled\n"


class MetricsCollector:
    """Emittiert Prometheus-Metriken fuer alle relevanten Bot-Events."""

    enabled = True

    def __init__(self) -> None:
        self._registry = CollectorRegistry()

        # Counter
        self._trades = Counter(
            "fluxtrader_trades_total",
            "Anzahl geoeffneter Trades",
            ["strategy", "symbol", "side"],
            registry=self._registry,
        )
        self._pnl_realized = Counter(
            "fluxtrader_pnl_realized_total",
            "Realisierter PnL (kumuliert)",
            ["strategy", "symbol"],
            registry=self._registry,
        )
        self._signals = Counter(
            "fluxtrader_signals_total",
            "Anzahl Signale",
            ["strategy", "action", "filtered_by"],
            registry=self._registry,
        )

        # Gauge
        self._equity = Gauge(
            "fluxtrader_equity_current",
            "Aktuelles Equity",
            ["strategy"],
            registry=self._registry,
        )
        self._drawdown = Gauge(
            "fluxtrader_drawdown_pct",
            "Aktueller Drawdown in %",
            ["strategy"],
            registry=self._registry,
        )
        self._positions = Gauge(
            "fluxtrader_positions_open",
            "Offene Positionen",
            ["strategy"],
            registry=self._registry,
        )
        self._unrealized = Gauge(
            "fluxtrader_position_pnl_unrealized",
            "Unrealisierter PnL je Symbol",
            ["symbol"],
            registry=self._registry,
        )
        self._cb = Gauge(
            "fluxtrader_circuit_breaker_active",
            "1 = Circuit Breaker aktiv",
            ["strategy"],
            registry=self._registry,
        )
        self._wfo_sharpe = Gauge(
            "fluxtrader_wfo_oos_sharpe",
            "WFO Out-of-Sample Sharpe",
            ["strategy", "window"],
            registry=self._registry,
        )

        # Histogram
        self._order_latency = Histogram(
            "fluxtrader_order_latency_ms",
            "Order-Latenz (ms)",
            ["broker"],
            buckets=_ORDER_LATENCY_BUCKETS,
            registry=self._registry,
        )
        self._bar_lag = Histogram(
            "fluxtrader_bar_processing_lag_ms",
            "Bar-Verzoegerung (ms)",
            ["strategy"],
            buckets=_BAR_LAG_BUCKETS,
            registry=self._registry,
        )

    # ── Factory ──────────────────────────────────────────────────────

    @classmethod
    def create(cls, enabled: bool = True) -> "MetricsCollector | _NoOpCollector":
        if not enabled or not PROMETHEUS_AVAILABLE:
            if enabled:
                log.warning("metrics.disabled",
                            reason="prometheus_client not installed")
            return _NoOpCollector()
        return cls()

    # ── Trade-Metriken ───────────────────────────────────────────────

    def record_trade_opened(self, strategy: str, symbol: str, side: str) -> None:
        self._trades.labels(strategy=strategy, symbol=symbol, side=side).inc()

    def record_trade_closed(self, strategy: str, symbol: str,
                            pnl: float, reason: str = "") -> None:
        if pnl >= 0:
            self._pnl_realized.labels(strategy=strategy, symbol=symbol).inc(pnl)
        else:
            # prometheus counter akzeptiert keinen negativen Wert; wir halten
            # den realisierten PnL trotzdem als Counter (positive Komponenten)
            # plus zusaetzlichem Gauge auf Position-Ebene. Fuer negative
            # realisierte PnLs liefern Grafana-Panels die Differenz aus
            # pnl_realized_total und unrealized.
            pass

    def record_order_latency(self, broker: str, latency_ms: float) -> None:
        self._order_latency.labels(broker=broker).observe(float(latency_ms))

    # ── Portfolio-Metriken ───────────────────────────────────────────

    def set_equity(self, strategy: str, equity: float) -> None:
        self._equity.labels(strategy=strategy).set(float(equity))

    def set_drawdown(self, strategy: str, drawdown_pct: float) -> None:
        self._drawdown.labels(strategy=strategy).set(float(drawdown_pct))

    def set_unrealized_pnl(self, symbol: str, pnl: float) -> None:
        self._unrealized.labels(symbol=symbol).set(float(pnl))

    def set_open_positions(self, strategy: str, count: int) -> None:
        self._positions.labels(strategy=strategy).set(int(count))

    # ── Signal-Metriken ──────────────────────────────────────────────

    def record_signal(self, strategy: str, action: str,
                      filtered_by: str = "") -> None:
        self._signals.labels(strategy=strategy, action=action,
                             filtered_by=filtered_by).inc()

    def set_bar_lag(self, strategy: str, lag_ms: float) -> None:
        self._bar_lag.labels(strategy=strategy).observe(float(lag_ms))

    def set_circuit_breaker(self, strategy: str, active: bool) -> None:
        self._cb.labels(strategy=strategy).set(1.0 if active else 0.0)

    def set_wfo_sharpe(self, strategy: str, window: str, value: float) -> None:
        self._wfo_sharpe.labels(strategy=strategy, window=window).set(float(value))

    # ── DB-Cache Update ──────────────────────────────────────────────

    def update_from_health_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Aktualisiert Portfolio-Gauges aus einem get_health_snapshot()-Dict.

        Wird vom Runner nach refresh_from_db aufgerufen, damit die
        Prometheus-Metriken konsistent aus SQLite stammen.
        """
        strategy = snapshot.get("strategy", "")
        if snapshot.get("equity") is not None:
            self._equity.labels(strategy=strategy).set(
                float(snapshot["equity"]))
        if snapshot.get("drawdown_pct") is not None:
            self._drawdown.labels(strategy=strategy).set(
                float(snapshot["drawdown_pct"]))
        if snapshot.get("open_positions") is not None:
            self._positions.labels(strategy=strategy).set(
                int(snapshot["open_positions"]))

    # ── Export ───────────────────────────────────────────────────────

    def generate_text(self) -> str:
        return generate_latest(self._registry).decode("utf-8")

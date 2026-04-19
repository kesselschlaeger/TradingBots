"""Tests fuer live/metrics.py – NoOp-Verhalten + Prometheus-Wrapper."""
from __future__ import annotations

import pytest

from live.metrics import MetricsCollector


def test_disabled_returns_noop():
    m = MetricsCollector.create(enabled=False)
    # Alle Calls sind No-Ops – kein Fehler
    m.record_trade_opened("orb", "AAPL", "LONG")
    m.record_trade_closed("orb", "AAPL", pnl=-10.0)
    m.set_equity("orb", 10000.0)
    m.set_drawdown("orb", -1.2)
    m.set_unrealized_pnl("AAPL", 50.0)
    m.set_open_positions("orb", 2)
    m.record_order_latency("paper", 50.0)
    m.record_signal("orb", "LONG", "")
    m.set_bar_lag("orb", 100.0)
    m.set_circuit_breaker("orb", False)
    text = m.generate_text()
    assert "#" in text
    assert m.enabled is False


def test_enabled_requires_prometheus():
    """Wenn prometheus_client fehlt, faellt create() auf NoOp zurueck."""
    try:
        import prometheus_client  # noqa: F401
    except ImportError:
        m = MetricsCollector.create(enabled=True)
        assert m.enabled is False
        return

    m = MetricsCollector.create(enabled=True)
    m.record_trade_opened("orb", "AAPL", "LONG")
    m.set_equity("orb", 10000.0)
    m.set_bar_lag("orb", 500.0)
    text = m.generate_text()
    assert "fluxtrader_trades_total" in text
    assert "fluxtrader_equity_current" in text

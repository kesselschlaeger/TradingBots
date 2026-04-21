"""Tests fuer live/health.py – HealthState Writer/Reader-Trennung."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from live.health import HealthState


@pytest.mark.asyncio
async def test_initial_state_is_critical_without_broker():
    hs = HealthState()
    assert hs.is_ready() is False
    assert hs.overall_status() == "critical"


@pytest.mark.asyncio
async def test_broker_connected_without_bars_is_ready():
    hs = HealthState()
    await hs.set_broker_status(connected=True, adapter="paper")
    assert hs.is_ready() is True


@pytest.mark.asyncio
async def test_stale_bar_marks_unready(monkeypatch):
    # is_ready akzeptiert alte Bars außerhalb der Handelszeiten, damit
    # der Runner nachts/am Wochenende nicht als "not ready" gilt. Dieser
    # Test verifiziert das Verhalten während der Handelszeiten, also
    # muss is_market_hours() hier True liefern.
    import live.health as health_mod
    monkeypatch.setattr(health_mod, "is_market_hours", lambda _dt: True)
    hs = HealthState()
    await hs.set_broker_status(connected=True, adapter="paper")
    old = datetime.now(timezone.utc) - timedelta(minutes=20)
    await hs.set_last_bar("orb", old, lag_ms=1_200_000.0)
    assert hs.is_ready() is False


@pytest.mark.asyncio
async def test_stale_bar_ok_outside_market_hours(monkeypatch):
    import live.health as health_mod
    monkeypatch.setattr(health_mod, "is_market_hours", lambda _dt: False)
    hs = HealthState()
    await hs.set_broker_status(connected=True, adapter="paper")
    old = datetime.now(timezone.utc) - timedelta(minutes=20)
    await hs.set_last_bar("orb", old, lag_ms=1_200_000.0)
    assert hs.is_ready() is True


@pytest.mark.asyncio
async def test_bar_lag_thresholds_degrade_status():
    hs = HealthState()
    await hs.set_broker_status(connected=True, adapter="paper")
    recent = datetime.now(timezone.utc)
    await hs.set_last_bar("orb", recent, lag_ms=15_000.0)  # degraded
    assert hs.overall_status() == "degraded"
    await hs.set_last_bar("orb", recent, lag_ms=500.0)
    assert hs.overall_status() == "ok"


@pytest.mark.asyncio
async def test_signal_counters_reset():
    hs = HealthState()
    await hs.record_signal("orb", filtered=False)
    await hs.record_signal("orb", filtered=True)
    snap = hs.snapshot()
    strats = {s["name"]: s for s in snap["strategies"]}
    assert strats["orb"]["signals_today"] == 1
    assert strats["orb"]["signals_filtered_today"] == 1
    await hs.reset_daily_counters()
    snap = hs.snapshot()
    strats = {s["name"]: s for s in snap["strategies"]}
    assert strats.get("orb", {"signals_today": 0})["signals_today"] == 0


@pytest.mark.asyncio
async def test_circuit_breaker_forces_critical():
    hs = HealthState()
    await hs.set_broker_status(connected=True, adapter="paper")
    await hs.set_circuit_breaker(True)
    assert hs.overall_status() == "critical"


@pytest.mark.asyncio
async def test_symbol_status_round_trip():
    """set_symbol_status (sync) erscheint im Snapshot."""
    hs = HealthState()
    from datetime import timezone
    ts = datetime(2026, 4, 21, 14, 0, tzinfo=timezone.utc)
    hs.set_symbol_status("orb", "AAPL", "WAIT_BREAKOUT",
                         "Preis 150.00 in [149..151]", ts)
    hs.set_symbol_status("orb", "NVDA", "GAP_BLOCK", "gap 3.50%")

    snap = hs.snapshot()
    strats = {s["name"]: s for s in snap["strategies"]}
    sym = strats["orb"]["symbol_status"]
    assert sym["AAPL"]["code"] == "WAIT_BREAKOUT"
    assert "149" in sym["AAPL"]["reason"]
    assert sym["NVDA"]["code"] == "GAP_BLOCK"


@pytest.mark.asyncio
async def test_symbol_status_overwrite():
    """Neueres set_symbol_status überschreibt den alten Eintrag."""
    hs = HealthState()
    hs.set_symbol_status("orb", "SPY", "WAIT_ORB", "ORB-Periode")
    hs.set_symbol_status("orb", "SPY", "SIGNAL", "ORB Breakout: 420 > 419")

    snap = hs.snapshot()
    strats = {s["name"]: s for s in snap["strategies"]}
    assert strats["orb"]["symbol_status"]["SPY"]["code"] == "SIGNAL"


@pytest.mark.asyncio
async def test_strategy_status_sink_wires_to_health_state():
    """LiveRunner-Muster: Sink verbindet BaseStrategy mit HealthState."""
    from strategy.orb import ORBStrategy

    hs = HealthState()
    strat = ORBStrategy({})
    strat.set_status_sink(
        lambda sym, code, reason:
            hs.set_symbol_status("orb", sym, code, reason)
    )
    strat._record_status("AAPL", "TEST_CODE", "detail")

    snap = hs.snapshot()
    strats = {s["name"]: s for s in snap["strategies"]}
    assert strats["orb"]["symbol_status"]["AAPL"]["code"] == "TEST_CODE"


@pytest.mark.asyncio
async def test_snapshot_contains_all_sections():
    hs = HealthState()
    await hs.set_broker_status(connected=True, adapter="alpaca", last_order_ms=120.0)
    await hs.update_portfolio(equity=10000.0, cash=5000.0,
                              drawdown_pct=-1.5, open_positions=2, peak_equity=10100.0)
    snap = hs.snapshot()
    assert snap["broker"]["adapter"] == "alpaca"
    assert snap["portfolio"]["equity"] == 10000.0
    assert snap["portfolio"]["open_positions"] == 2
    assert "strategies" in snap
    assert snap["uptime_seconds"] >= 0

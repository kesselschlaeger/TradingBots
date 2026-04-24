"""Tests fuer live/health.py mit neuer Overall-State-Logik."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from live.health import HealthState


@pytest.mark.asyncio
async def test_next_expected_bar_at_dynamic_formula():
    hs = HealthState(
        strategy_config={
            "bar_timeframe_seconds": 300,
            "provider_poll_interval_s": 30,
            "stale_tolerance_s": 60,
        },
    )
    last = datetime(2026, 4, 24, 14, 0, tzinfo=timezone.utc)
    expected = hs.next_expected_bar_at(last)
    assert expected == datetime(2026, 4, 24, 14, 6, 30, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_overall_state_ok_in_trade_window_when_fresh_and_alive():
    hs = HealthState(
        strategy_config={
            "market_open_time": "09:30",
            "entry_cutoff_time": "15:00",
            "eod_close_time": "15:27",
        },
        monitoring_config={
            "watchdog_interval_s": 15,
            "grace_period_s": 90,
            "bar_timeframe_seconds": 300,
            "provider_poll_interval_s": 30,
            "stale_tolerance_s": 60,
        },
    )
    now = datetime(2026, 4, 24, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
    await hs.set_broker_status(True, "paper")
    await hs.set_watchdog("orb", now - timedelta(seconds=10))
    await hs.set_last_bar("orb", now - timedelta(minutes=1), lag_ms=500.0)

    assert hs.overall_state("orb", now=now) == "OK"


@pytest.mark.asyncio
async def test_overall_state_idle_outside_window():
    hs = HealthState(
        strategy_config={
            "market_open_time": "09:30",
            "entry_cutoff_time": "15:00",
            "eod_close_time": "15:27",
        },
    )
    now = datetime(2026, 4, 24, 20, 0, tzinfo=timezone.utc)  # 16:00 ET
    await hs.set_broker_status(True, "paper")
    await hs.set_watchdog("orb", now - timedelta(seconds=10))
    await hs.set_last_bar("orb", now - timedelta(minutes=20), lag_ms=1000.0)

    assert hs.overall_state("orb", now=now) == "IDLE_OUT_OF_WINDOW"


@pytest.mark.asyncio
async def test_overall_state_data_stale_in_window():
    hs = HealthState(
        strategy_config={
            "market_open_time": "09:30",
            "entry_cutoff_time": "15:00",
            "eod_close_time": "15:27",
            "bar_timeframe_seconds": 300,
            "provider_poll_interval_s": 30,
            "stale_tolerance_s": 60,
        },
        monitoring_config={
            "watchdog_interval_s": 15,
            "grace_period_s": 90,
        },
    )
    now = datetime(2026, 4, 24, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
    await hs.set_broker_status(True, "paper")
    await hs.set_watchdog("orb", now - timedelta(seconds=5))
    await hs.set_last_bar("orb", now - timedelta(minutes=20), lag_ms=1_200_000.0)

    assert hs.overall_state("orb", now=now) == "DATA_STALE"


@pytest.mark.asyncio
async def test_overall_state_process_dead():
    hs = HealthState(
        strategy_config={
            "market_open_time": "09:30",
            "entry_cutoff_time": "15:00",
            "eod_close_time": "15:27",
        },
        monitoring_config={"watchdog_interval_s": 15},
    )
    now = datetime(2026, 4, 24, 14, 0, tzinfo=timezone.utc)
    await hs.set_broker_status(True, "paper")
    await hs.set_watchdog("orb", now - timedelta(seconds=60))
    await hs.set_last_bar("orb", now - timedelta(minutes=1), lag_ms=500.0)

    assert hs.overall_state("orb", now=now) == "PROCESS_DEAD"


@pytest.mark.asyncio
async def test_overall_state_circuit_break_priority():
    hs = HealthState(strategy_config={"entry_cutoff_time": "15:00"})
    now = datetime(2026, 4, 24, 14, 0, tzinfo=timezone.utc)
    await hs.set_broker_status(True, "paper")
    await hs.set_watchdog("orb", now - timedelta(seconds=5))
    await hs.set_last_bar("orb", now - timedelta(minutes=1), lag_ms=500.0)
    await hs.set_circuit_breaker(True)

    assert hs.overall_state("orb", now=now) == "CIRCUIT_BREAK"

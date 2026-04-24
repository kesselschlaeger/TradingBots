"""Tests fuer live/anomaly.py – alle fuenf Checks mit synthetischen Events."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.config import AppConfig, StrategyConfig
from core.models import (
    AlertLevel,
    FeatureVector,
    OrderRequest,
    OrderSide,
    Signal,
    Trade,
)
from live.anomaly import AnomalyDetector
from live.notifier import TelegramNotifier


def _mk_cfg() -> AppConfig:
    cfg = AppConfig(strategy=StrategyConfig(name="orb", symbols=["AAPL"]))
    # Engere Grenzen fuer deterministische Tests
    cfg.anomaly.duplicate_window_minutes = 5
    cfg.anomaly.max_single_order_pct = 0.25
    cfg.anomaly.max_volume_pct = 0.01
    cfg.anomaly.pnl_spike_sigma = 2.0
    cfg.anomaly.pnl_lookback_trades = 10
    cfg.anomaly.bar_gap_minutes = 10
    cfg.anomaly.max_signals_per_hour = 3
    return cfg


class _StubState:
    """State-Stub – der AnomalyDetector ruft aktuell keine Methoden darauf auf."""
    pass


def _mk_state(tmp_path: Path) -> _StubState:
    return _StubState()


def _mk_notifier() -> TelegramNotifier:
    # enabled=False -> send() ist No-Op, aber alert() laeuft durch den Limiter
    return TelegramNotifier(enabled=False, bot_name="test")


def _mk_signal(symbol: str = "AAPL", direction: int = 1) -> Signal:
    return Signal(
        strategy="orb", symbol=symbol, direction=direction,
        features=FeatureVector(),
        timestamp=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_duplicate_trade_guard_warns(tmp_path):
    cfg = _mk_cfg()
    det = AnomalyDetector(_mk_notifier(), _mk_state(tmp_path), cfg)
    sig = _mk_signal()
    events1 = await det.check_signal(sig)
    assert all(e.check_name != "duplicate_trade" for e in events1)
    events2 = await det.check_signal(_mk_signal())
    assert any(e.check_name == "duplicate_trade" for e in events2)
    assert det.should_block(events2) is False


@pytest.mark.asyncio
async def test_duplicate_hard_block(tmp_path):
    cfg = _mk_cfg()
    cfg.anomaly.duplicate_hard_block = True
    det = AnomalyDetector(_mk_notifier(), _mk_state(tmp_path), cfg)
    await det.check_signal(_mk_signal())
    events = await det.check_signal(_mk_signal())
    assert det.should_block(events) is True


@pytest.mark.asyncio
async def test_oversized_order_pct_equity(tmp_path):
    cfg = _mk_cfg()
    det = AnomalyDetector(_mk_notifier(), _mk_state(tmp_path), cfg)
    req = OrderRequest(symbol="AAPL", side=OrderSide.BUY, qty=100)
    result = {"fill_price": 100.0, "equity": 10_000.0}   # 100*100=10k = 100%
    events = await det.check_order(req, result)
    assert any(e.check_name == "oversized_order" for e in events)


@pytest.mark.asyncio
async def test_oversized_order_volume_pct(tmp_path):
    cfg = _mk_cfg()
    det = AnomalyDetector(_mk_notifier(), _mk_state(tmp_path), cfg)
    req = OrderRequest(symbol="AAPL", side=OrderSide.BUY, qty=100_000)
    result = {"fill_price": 1.0, "equity": 10_000_000.0,
              "avg_daily_volume": 1_000_000.0}  # 10% des Tagesvolumens
    events = await det.check_order(req, result)
    assert any("Tagesvolumens" in e.message for e in events)


@pytest.mark.asyncio
async def test_pnl_spike_detector(tmp_path):
    cfg = _mk_cfg()
    det = AnomalyDetector(_mk_notifier(), _mk_state(tmp_path), cfg)
    # Fensterfuellen mit leicht variierenden PnLs (std > 0)
    for pnl in [4.0, 5.5, 3.0, 6.0, 4.5, 5.0, 4.8, 5.2, 4.2, 5.8]:
        await det.check_trade_result(Trade(
            symbol="AAPL", side="BUY", qty=1, price=100.0, pnl=pnl,
            strategy_id="orb",
        ))
    # Spike: 100$ weit ausserhalb >2σ
    events = await det.check_trade_result(Trade(
        symbol="AAPL", side="BUY", qty=1, price=100.0, pnl=100.0,
        strategy_id="orb",
    ))
    assert any(e.check_name == "pnl_spike" for e in events)


@pytest.mark.skip(reason="check_heartbeat removed — connectivity now handled by health state machine")
@pytest.mark.asyncio
async def test_connectivity_no_alert_outside_market_hours(tmp_path, monkeypatch):
    cfg = _mk_cfg()
    det = AnomalyDetector(_mk_notifier(), _mk_state(tmp_path), cfg)
    import live.anomaly as am
    monkeypatch.setattr(am, "is_market_hours", lambda _dt: False)
    stale = datetime.now(timezone.utc) - timedelta(minutes=30)
    events = await det.check_heartbeat("orb", stale)
    assert events == []


@pytest.mark.skip(reason="check_heartbeat removed — connectivity now handled by health state machine")
@pytest.mark.asyncio
async def test_connectivity_alerts_inside_market_hours(tmp_path, monkeypatch):
    cfg = _mk_cfg()
    det = AnomalyDetector(_mk_notifier(), _mk_state(tmp_path), cfg)
    import live.anomaly as am
    monkeypatch.setattr(am, "is_market_hours", lambda _dt: True)
    stale = datetime.now(timezone.utc) - timedelta(minutes=30)
    events = await det.check_heartbeat("orb", stale)
    assert any(e.check_name == "connectivity" for e in events)
    assert any(e.severity == AlertLevel.CRITICAL for e in events)


@pytest.mark.asyncio
async def test_signal_flood_detector(tmp_path):
    cfg = _mk_cfg()   # max_signals_per_hour=3
    det = AnomalyDetector(_mk_notifier(), _mk_state(tmp_path), cfg)
    # 5 Signale mit unterschiedlichen Symbolen (keine Duplicates)
    fired = False
    for i in range(5):
        events = await det.check_signal(_mk_signal(symbol=f"SYM{i}"))
        if any(e.check_name == "signal_flood" for e in events):
            fired = True
    assert fired is True


@pytest.mark.asyncio
async def test_disabled_check_is_noop(tmp_path):
    cfg = _mk_cfg()
    cfg.anomaly.enabled_checks["duplicate_trade"] = False
    det = AnomalyDetector(_mk_notifier(), _mk_state(tmp_path), cfg)
    await det.check_signal(_mk_signal())
    events = await det.check_signal(_mk_signal())
    assert all(e.check_name != "duplicate_trade" for e in events)

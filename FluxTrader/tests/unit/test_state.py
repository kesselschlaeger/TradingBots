"""Unit-Tests für live/state.py – sauberes Schema, (bot_name, strategy) als Composite-Key."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.models import AlertLevel, AnomalyEvent
from live.state import PersistentState

BOT = "orb_spy_paper"
STRAT = "orb"
BOT2 = "botti_nq_live"
STRAT2 = "botti"


@pytest.fixture()
async def state(tmp_path: Path) -> PersistentState:
    s = PersistentState(tmp_path / "test.db")
    await s.ensure_schema()
    return s


# ── Schema ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ensure_schema_idempotent(state: PersistentState):
    await state.ensure_schema()
    await state.ensure_schema()


# ── Cooldowns ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cooldowns_composite_key(state: PersistentState):
    now = datetime.now(timezone.utc)
    until = now + timedelta(hours=1)

    await state.set_cooldown(bot_name=BOT, strategy=STRAT, symbol="AAPL", until=until)
    await state.set_cooldown(bot_name=BOT2, strategy=STRAT2, symbol="AAPL", until=until)

    assert await state.is_in_cooldown(bot_name=BOT, strategy=STRAT, symbol="AAPL", now=now)
    assert await state.is_in_cooldown(bot_name=BOT2, strategy=STRAT2, symbol="AAPL", now=now)
    assert not await state.is_in_cooldown(bot_name=BOT, strategy=STRAT, symbol="MSFT", now=now)

    future = now + timedelta(hours=2)
    cleared = await state.clear_expired_cooldowns(BOT, STRAT, future)
    assert cleared == 1

    orb_cools = await state.get_cooldowns(BOT, STRAT)
    botti_cools = await state.get_cooldowns(BOT2, STRAT2)
    assert len(orb_cools) == 0
    assert len(botti_cools) == 1


# ── Account-Peak ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_account_peak_composite_key(state: PersistentState):
    peak_orb = await state.update_peak_equity(BOT, STRAT, 50000.0)
    peak_botti = await state.update_peak_equity(BOT2, STRAT2, 75000.0)

    assert peak_orb == 50000.0
    assert peak_botti == 75000.0
    assert await state.get_peak_equity(BOT, STRAT) == 50000.0
    assert await state.get_peak_equity(BOT2, STRAT2) == 75000.0

    peak_orb2 = await state.update_peak_equity(BOT, STRAT, 60000.0)
    assert peak_orb2 == 60000.0
    assert await state.get_peak_equity(BOT2, STRAT2) == 75000.0


# ── Trade Lifecycle ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_open_trade_atomic_creates_trade_and_position(state: PersistentState):
    now = datetime.now(timezone.utc)
    trade_id = await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT,
        symbol="AAPL", side="long",
        entry_ts=now, entry_price=150.0, qty=10.0,
        stop_price=145.0, current_price=150.0,
    )
    assert trade_id > 0

    trades = await state.get_trades(bot_name=BOT, strategy=STRAT)
    assert len(trades) == 1
    assert trades[0]["id"] == trade_id
    assert trades[0]["symbol"] == "AAPL"
    assert trades[0]["created_at"] is not None

    positions = await state.get_open_positions(BOT, STRAT)
    assert len(positions) == 1
    assert positions[0]["trade_id"] == trade_id
    assert positions[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_open_trade_atomic_with_reserve_group(state: PersistentState):
    today = date.today()
    trade_id = await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT,
        symbol="AAPL", side="long",
        entry_ts=datetime.now(timezone.utc),
        entry_price=150.0, qty=10.0,
        reserve_group_name="TECH", reserve_day=today,
    )
    assert trade_id > 0

    groups = await state.reserved_groups(BOT, STRAT, today)
    assert "TECH" in groups


@pytest.mark.asyncio
async def test_close_trade_atomic_by_id(state: PersistentState):
    now = datetime.now(timezone.utc)
    trade_id = await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT,
        symbol="AAPL", side="long",
        entry_ts=now, entry_price=150.0, qty=10.0, current_price=150.0,
    )
    assert len(await state.get_open_positions(BOT, STRAT)) == 1

    await state.close_trade_atomic(
        bot_name=BOT, strategy=STRAT,
        trade_id=trade_id,
        exit_ts=now, exit_price=155.0,
        pnl=50.0, pnl_pct=3.33, reason="target",
    )

    assert len(await state.get_open_positions(BOT, STRAT)) == 0
    trades = await state.get_trades(bot_name=BOT, strategy=STRAT, only_closed=True)
    assert len(trades) == 1
    assert trades[0]["exit_price"] == 155.0
    assert trades[0]["pnl"] == 50.0


@pytest.mark.asyncio
async def test_close_trade_atomic_by_symbol(state: PersistentState):
    now = datetime.now(timezone.utc)
    await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT,
        symbol="MSFT", side="long",
        entry_ts=now, entry_price=300.0, qty=5.0,
    )
    await state.close_trade_atomic(
        bot_name=BOT, strategy=STRAT,
        symbol="MSFT",
        exit_ts=now, exit_price=310.0, pnl=50.0,
    )
    assert len(await state.get_open_positions(BOT, STRAT)) == 0


@pytest.mark.asyncio
async def test_close_trade_atomic_no_match_is_noop(state: PersistentState):
    await state.close_trade_atomic(
        bot_name=BOT, strategy=STRAT,
        trade_id=9999,
        exit_ts=datetime.now(timezone.utc), exit_price=100.0,
    )


# ── Daily PnL / Trades ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_daily_pnl_from_trades(state: PersistentState):
    now = datetime.now(timezone.utc)
    today = now.date()

    t1 = await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT, symbol="AAPL", side="long",
        entry_ts=now, entry_price=150.0, qty=10.0,
    )
    await state.close_trade_atomic(
        bot_name=BOT, strategy=STRAT, trade_id=t1,
        exit_ts=now, exit_price=155.0, pnl=50.0,
    )
    t2 = await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT, symbol="MSFT", side="long",
        entry_ts=now, entry_price=300.0, qty=5.0,
    )
    await state.close_trade_atomic(
        bot_name=BOT, strategy=STRAT, trade_id=t2,
        exit_ts=now, exit_price=295.0, pnl=-25.0,
    )

    pnl = await state.daily_pnl(BOT, STRAT, today)
    assert pnl == pytest.approx(25.0)


@pytest.mark.asyncio
async def test_trades_today_counts_by_symbol(state: PersistentState):
    now = datetime.now(timezone.utc)
    today = now.date()

    for _ in range(3):
        tid = await state.open_trade_atomic(
            bot_name=BOT, strategy=STRAT, symbol="AAPL", side="long",
            entry_ts=now, entry_price=150.0, qty=10.0,
        )
        await state.close_trade_atomic(
            bot_name=BOT, strategy=STRAT, trade_id=tid,
            exit_ts=now, exit_price=155.0, pnl=50.0,
        )
    tid = await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT, symbol="MSFT", side="long",
        entry_ts=now, entry_price=300.0, qty=5.0,
    )
    await state.close_trade_atomic(
        bot_name=BOT, strategy=STRAT, trade_id=tid,
        exit_ts=now, exit_price=305.0, pnl=25.0,
    )

    by_sym = await state.trades_today(BOT, STRAT, today)
    assert by_sym == {"AAPL": 3, "MSFT": 1}


# ── Equity + Positions ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_save_equity_snapshot_has_created_at(state: PersistentState):
    now = datetime.now(timezone.utc)
    await state.save_equity_snapshot(
        bot_name=BOT, strategy=STRAT, ts=now,
        equity=100000.0, cash=50000.0,
    )
    curve = await state.get_latest_equity_curve(BOT, STRAT, limit=1)
    assert len(curve) == 1
    assert curve[0]["created_at"] is not None


@pytest.mark.asyncio
async def test_update_or_create_position_upserts(state: PersistentState):
    await state.update_or_create_position(
        bot_name=BOT, strategy=STRAT, symbol="AAPL",
        side="long", entry_price=150.0, qty=10.0,
    )
    positions = await state.get_open_positions(BOT, STRAT)
    assert len(positions) == 1

    await state.update_or_create_position(
        bot_name=BOT, strategy=STRAT, symbol="AAPL",
        current_price=155.0, unrealized_pnl=50.0,
    )
    positions = await state.get_open_positions(BOT, STRAT)
    assert len(positions) == 1
    assert float(positions[0]["current_price"]) == 155.0


@pytest.mark.asyncio
async def test_remove_position(state: PersistentState):
    await state.update_or_create_position(
        bot_name=BOT, strategy=STRAT, symbol="AAPL",
        side="long", entry_price=150.0, qty=10.0,
    )
    await state.remove_position(bot_name=BOT, strategy=STRAT, symbol="AAPL")
    assert len(await state.get_open_positions(BOT, STRAT)) == 0


# ── Signals ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_save_signal_returns_id(state: PersistentState):
    now = datetime.now(timezone.utc)
    sig_id = await state.save_signal(
        bot_name=BOT, strategy=STRAT,
        ts=now, symbol="AAPL", action="LONG", strength=0.8,
    )
    assert sig_id > 0
    signals = await state.get_signals(bot_name=BOT, strategy=STRAT)
    assert len(signals) == 1
    assert signals[0]["created_at"] is not None


# ── Reserved Groups ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_reserve_group_and_reset_day(state: PersistentState):
    today = date.today()
    await state.reserve_group(bot_name=BOT, strategy=STRAT, group="TECH", day=today)
    await state.reserve_group(bot_name=BOT, strategy=STRAT, group="FINANCE", day=today)

    groups = await state.reserved_groups(BOT, STRAT, today)
    assert "TECH" in groups
    assert "FINANCE" in groups

    await state.reset_day(BOT, STRAT, today)
    groups = await state.reserved_groups(BOT, STRAT, today)
    assert len(groups) == 0


# ── Heartbeat ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_upsert_bot_heartbeat(state: PersistentState):
    now = datetime.now(timezone.utc)
    await state.upsert_bot_heartbeat(
        bot_name=BOT, strategy=STRAT,
        last_bar_ts=now, last_bar_lag_ms=42.5,
        broker_connected=True, broker_adapter="alpaca",
        symbol_status={"AAPL": "ok"},
    )
    heartbeats = await state.get_bot_heartbeats()
    assert len(heartbeats) == 1
    hb = heartbeats[0]
    assert hb["bot_name"] == BOT
    assert hb["strategy"] == STRAT
    assert hb["broker_connected"] is True
    assert hb["symbol_status"] == {"AAPL": "ok"}


@pytest.mark.asyncio
async def test_get_bot_heartbeats_active_only(state: PersistentState):
    now = datetime.now(timezone.utc)
    await state.upsert_bot_heartbeat(
        bot_name=BOT, strategy=STRAT,
        last_bar_ts=now, broker_connected=True, broker_adapter="alpaca",
    )
    active = await state.get_bot_heartbeats(active_only=True, active_threshold_seconds=180)
    assert len(active) == 1


# ── Anomalien ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_log_anomaly_and_get(state: PersistentState):
    older = datetime.now(timezone.utc) - timedelta(minutes=10)
    newer = datetime.now(timezone.utc) - timedelta(minutes=1)

    await state.log_anomaly(AnomalyEvent(
        timestamp=older, strategy=STRAT, bot_name=BOT,
        check_name="duplicate_trade", severity=AlertLevel.WARNING,
        symbol="AAPL", message="Older warning",
    ))
    await state.log_anomaly(AnomalyEvent(
        timestamp=newer, strategy=STRAT, bot_name=BOT,
        check_name="signal_flood", severity=AlertLevel.CRITICAL,
        symbol="MSFT", message="Newer critical",
    ))

    items = await state.get_anomalies(bot_name=BOT, strategy=STRAT, limit=10)
    assert len(items) == 2
    assert items[0]["check_name"] == "signal_flood"
    assert items[1]["check_name"] == "duplicate_trade"


# ── Health Snapshot ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_health_snapshot(state: PersistentState):
    now = datetime.now(timezone.utc)

    await state.save_equity_snapshot(
        bot_name=BOT, strategy=STRAT, ts=now,
        equity=100000.0, cash=50000.0, drawdown_pct=-2.5, peak_equity=102500.0,
    )
    tid = await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT, symbol="AAPL", side="long",
        entry_ts=now, entry_price=150.0, qty=10.0,
    )
    await state.close_trade_atomic(
        bot_name=BOT, strategy=STRAT, trade_id=tid,
        exit_ts=now, exit_price=155.0, pnl=50.0,
    )
    await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT, symbol="MSFT", side="long",
        entry_ts=now, entry_price=300.0, qty=5.0,
    )
    await state.save_signal(
        bot_name=BOT, strategy=STRAT, ts=now, symbol="AAPL", action="BUY",
    )

    snap = await state.get_health_snapshot(BOT, STRAT)
    assert snap["bot_name"] == BOT
    assert snap["strategy"] == STRAT
    assert snap["equity"] == 100000.0
    assert snap["cash"] == 50000.0
    assert snap["open_positions"] == 1
    assert snap["trades_today"] == 1
    assert snap["pnl_today"] == 50.0
    assert snap["signals_today"] == 1
    assert snap["anomalies_last_hour"] == 0


# ── get_bot_instances ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_bot_instances(state: PersistentState):
    now = datetime.now(timezone.utc)
    await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT, symbol="AAPL", side="long",
        entry_ts=now, entry_price=150.0, qty=10.0,
    )
    await state.upsert_bot_heartbeat(
        bot_name=BOT2, strategy=STRAT2,
        broker_connected=False, broker_adapter="paper",
    )

    instances = await state.get_bot_instances()
    keys = {(i["bot_name"], i["strategy"]) for i in instances}
    assert (BOT, STRAT) in keys
    assert (BOT2, STRAT2) in keys


# ── clear_expired_cooldowns single DELETE ─────────────────────────────

@pytest.mark.asyncio
async def test_clear_expired_cooldowns_single_delete(state: PersistentState):
    now = datetime.now(timezone.utc)
    past = now - timedelta(hours=1)
    future = now + timedelta(hours=1)

    await state.set_cooldown(bot_name=BOT, strategy=STRAT, symbol="AAPL", until=past)
    await state.set_cooldown(bot_name=BOT, strategy=STRAT, symbol="MSFT", until=future)

    deleted = await state.clear_expired_cooldowns(BOT, STRAT, now)
    assert deleted == 1

    remaining = await state.get_cooldowns(BOT, STRAT)
    assert "AAPL" not in remaining
    assert "MSFT" in remaining


# ── Rollback-Simulation ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_open_trade_atomic_rollback_on_error(tmp_path: Path):
    """Crash nach Trade-INSERT → weder Trade noch Position in DB."""
    state = PersistentState(tmp_path / "crash_test.db")
    await state.ensure_schema()

    original_conn = state._conn
    call_count = 0

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def failing_conn():
        async with original_conn() as conn:
            original_execute = conn.execute

            async def patched_execute(sql, params=None):
                nonlocal call_count
                call_count += 1
                if call_count >= 3 and "INSERT INTO positions" in sql:
                    raise RuntimeError("Simulated crash after trade INSERT")
                if params:
                    return await original_execute(sql, params)
                return await original_execute(sql)

            conn.execute = patched_execute
            yield conn

    state._conn = failing_conn

    with pytest.raises(RuntimeError, match="Simulated crash"):
        await state.open_trade_atomic(
            bot_name=BOT, strategy=STRAT,
            symbol="AAPL", side="long",
            entry_ts=datetime.now(timezone.utc),
            entry_price=150.0, qty=10.0,
        )

    state._conn = original_conn

    trades = await state.get_trades(bot_name=BOT, strategy=STRAT)
    positions = await state.get_open_positions(BOT, STRAT)
    assert len(trades) == 0
    assert len(positions) == 0

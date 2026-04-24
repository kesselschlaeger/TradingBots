"""Unit-Tests für live/state.py – Refactored Schema + Atomic Operations.

Jeder Test bekommt eine frische PersistentState-Instanz (kein Singleton).
"""
from __future__ import annotations

import asyncio
import warnings
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.models import AlertLevel, AnomalyEvent
from live.state import PersistentState


@pytest.fixture()
async def state(tmp_path: Path) -> PersistentState:
    """Frischer PersistentState mit temporärer DB pro Test."""
    s = PersistentState(tmp_path / "test.db")
    await s.ensure_schema()
    return s


# ── Schema + Migrations ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ensure_schema_idempotent(state: PersistentState):
    """Schema-Aufruf darf beliebig oft wiederholt werden."""
    await state.ensure_schema()
    await state.ensure_schema()


@pytest.mark.asyncio
async def test_cooldowns_strategy_aware(state: PersistentState):
    """Cooldowns sind (symbol, strategy)-scoped."""
    now = datetime.now(timezone.utc)
    until = now + timedelta(hours=1)

    await state.set_cooldown("AAPL", until, strategy="orb")
    await state.set_cooldown("AAPL", until, strategy="botti")

    assert await state.is_in_cooldown("AAPL", now, strategy="orb")
    assert await state.is_in_cooldown("AAPL", now, strategy="botti")
    assert not await state.is_in_cooldown("AAPL", now, strategy="obb")

    # ORB-Cooldown löschen, Botti bleibt
    future = now + timedelta(hours=2)
    cleared = await state.clear_expired_cooldowns(future, strategy="orb")
    assert cleared == 1
    orb_cools = await state.get_cooldowns(strategy="orb")
    botti_cools = await state.get_cooldowns(strategy="botti")
    assert len(orb_cools) == 0
    assert len(botti_cools) == 1


@pytest.mark.asyncio
async def test_cooldowns_default_strategy(state: PersistentState):
    """Ohne strategy-Parameter greift Default ''."""
    now = datetime.now(timezone.utc)
    until = now + timedelta(hours=1)

    await state.set_cooldown("MSFT", until)
    assert await state.is_in_cooldown("MSFT", now)
    cools = await state.get_cooldowns()
    assert "MSFT" in cools


@pytest.mark.asyncio
async def test_account_peak_strategy_aware(state: PersistentState):
    """Peak Equity ist pro Strategie getrennt."""
    peak_orb = await state.update_peak_equity(50000.0, strategy="orb")
    peak_botti = await state.update_peak_equity(75000.0, strategy="botti")

    assert peak_orb == 50000.0
    assert peak_botti == 75000.0
    assert await state.get_peak_equity(strategy="orb") == 50000.0
    assert await state.get_peak_equity(strategy="botti") == 75000.0

    # Update ORB höher
    peak_orb2 = await state.update_peak_equity(60000.0, strategy="orb")
    assert peak_orb2 == 60000.0
    # Botti unverändert
    assert await state.get_peak_equity(strategy="botti") == 75000.0


@pytest.mark.asyncio
async def test_account_peak_default_strategy(state: PersistentState):
    """Ohne strategy-Parameter greift Default ''."""
    peak = await state.update_peak_equity(100000.0)
    assert peak == 100000.0
    assert await state.get_peak_equity() == 100000.0


# ── Positions trade_id FK ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_position_with_trade_id(state: PersistentState):
    """Position kann mit trade_id FK angelegt werden."""
    trade_id = await state.save_trade(
        strategy="orb", symbol="AAPL", side="long",
        entry_ts=datetime.now(timezone.utc), entry_price=150.0, qty=10.0,
    )
    await state.update_or_create_position(
        strategy="orb", symbol="AAPL", side="long",
        entry_price=150.0, qty=10.0, trade_id=trade_id,
    )
    positions = await state.get_open_positions(strategy="orb")
    assert len(positions) == 1
    assert positions[0]["trade_id"] == trade_id


# ── Daily deprecated → trades-basiert ────────────────────────────────

@pytest.mark.asyncio
async def test_update_daily_record_deprecated(state: PersistentState):
    """update_daily_record ist deprecated und ein no-op."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        await state.update_daily_record(
            day=date.today(), strategy="orb", pnl_delta=100.0, symbol="AAPL",
        )
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)


@pytest.mark.asyncio
async def test_add_trade_deprecated(state: PersistentState):
    """add_trade ist deprecated und ein no-op."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        await state.add_trade(day=date.today(), symbol="AAPL",
                              pnl=50.0, strategy="orb")
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)


@pytest.mark.asyncio
async def test_daily_pnl_from_trades(state: PersistentState):
    """daily_pnl berechnet PnL aus der trades-Tabelle."""
    now = datetime.now(timezone.utc)
    today = now.date()

    # Trade öffnen und schließen
    trade_id = await state.save_trade(
        strategy="orb", symbol="AAPL", side="long",
        entry_ts=now, entry_price=150.0, qty=10.0,
    )
    await state.close_trade(
        trade_id=trade_id, exit_ts=now, exit_price=155.0,
        pnl=50.0, reason="target",
    )

    trade_id2 = await state.save_trade(
        strategy="orb", symbol="MSFT", side="long",
        entry_ts=now, entry_price=300.0, qty=5.0,
    )
    await state.close_trade(
        trade_id=trade_id2, exit_ts=now, exit_price=295.0,
        pnl=-25.0, reason="stop",
    )

    pnl = await state.daily_pnl(today, strategy="orb")
    assert pnl == pytest.approx(25.0)

    # Aggregiert über alle Strategien
    pnl_all = await state.daily_pnl(today)
    assert pnl_all == pytest.approx(25.0)


@pytest.mark.asyncio
async def test_trades_today_from_trades(state: PersistentState):
    """trades_today zählt Trades pro Symbol aus der trades-Tabelle."""
    now = datetime.now(timezone.utc)
    today = now.date()

    for _ in range(3):
        tid = await state.save_trade(
            strategy="orb", symbol="AAPL", side="long",
            entry_ts=now, entry_price=150.0, qty=10.0,
        )
        await state.close_trade(
            trade_id=tid, exit_ts=now, exit_price=155.0, pnl=50.0,
        )

    tid = await state.save_trade(
        strategy="orb", symbol="MSFT", side="long",
        entry_ts=now, entry_price=300.0, qty=5.0,
    )
    await state.close_trade(
        trade_id=tid, exit_ts=now, exit_price=305.0, pnl=25.0,
    )

    by_sym = await state.trades_today(today, strategy="orb")
    assert by_sym == {"AAPL": 3, "MSFT": 1}


# ── open_trade_atomic ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_open_trade_atomic_creates_trade_and_position(
    state: PersistentState,
):
    """Atomic open erstellt Trade + Position in einer Transaktion."""
    trade_id = await state.open_trade_atomic(
        strategy="orb",
        symbol="AAPL",
        side="long",
        entry_ts=datetime.now(timezone.utc),
        entry_price=150.0,
        qty=10.0,
        stop_price=145.0,
        current_price=150.0,
    )
    assert trade_id > 0

    # Trade existiert
    trades = await state.get_trades(strategy="orb")
    assert len(trades) == 1
    assert trades[0]["id"] == trade_id
    assert trades[0]["symbol"] == "AAPL"

    # Position existiert mit trade_id FK
    positions = await state.get_open_positions(strategy="orb")
    assert len(positions) == 1
    assert positions[0]["trade_id"] == trade_id
    assert positions[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_open_trade_atomic_with_reserve_group(
    state: PersistentState,
):
    """Atomic open reserviert Gruppe in gleicher Transaktion."""
    today = date.today()
    trade_id = await state.open_trade_atomic(
        strategy="orb",
        symbol="AAPL",
        side="long",
        entry_ts=datetime.now(timezone.utc),
        entry_price=150.0,
        qty=10.0,
        reserve_group_name="TECH",
        reserve_day=today,
    )
    assert trade_id > 0

    groups = await state.reserved_groups(today, strategy="orb")
    assert "TECH" in groups


# ── close_trade_atomic ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_close_trade_atomic_closes_and_removes_position(
    state: PersistentState,
):
    """Atomic close schließt Trade und entfernt Position."""
    trade_id = await state.open_trade_atomic(
        strategy="orb",
        symbol="AAPL",
        side="long",
        entry_ts=datetime.now(timezone.utc),
        entry_price=150.0,
        qty=10.0,
        current_price=150.0,
    )
    assert len(await state.get_open_positions(strategy="orb")) == 1

    await state.close_trade_atomic(
        trade_id=trade_id,
        exit_ts=datetime.now(timezone.utc),
        exit_price=155.0,
        pnl=50.0,
        pnl_pct=3.33,
        reason="target",
    )

    # Position gelöscht
    positions = await state.get_open_positions(strategy="orb")
    assert len(positions) == 0

    # Trade geschlossen
    trades = await state.get_trades(strategy="orb", only_closed=True)
    assert len(trades) == 1
    assert trades[0]["exit_price"] == 155.0
    assert trades[0]["pnl"] == 50.0


@pytest.mark.asyncio
async def test_close_trade_atomic_by_strategy_symbol(
    state: PersistentState,
):
    """Atomic close funktioniert auch ohne trade_id."""
    await state.open_trade_atomic(
        strategy="orb",
        symbol="MSFT",
        side="long",
        entry_ts=datetime.now(timezone.utc),
        entry_price=300.0,
        qty=5.0,
    )

    await state.close_trade_atomic(
        strategy="orb",
        symbol="MSFT",
        exit_ts=datetime.now(timezone.utc),
        exit_price=310.0,
        pnl=50.0,
    )

    positions = await state.get_open_positions(strategy="orb")
    assert len(positions) == 0


@pytest.mark.asyncio
async def test_close_trade_atomic_no_match(state: PersistentState):
    """Atomic close mit nicht-existentem Trade ist harmlos."""
    await state.close_trade_atomic(
        trade_id=9999,
        exit_ts=datetime.now(timezone.utc),
        exit_price=100.0,
    )
    # Kein Fehler, einfach no-op


# ── get_health_snapshot ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_health_snapshot(state: PersistentState):
    """Health-Snapshot liefert aggregierte Monitoring-Daten."""
    now = datetime.now(timezone.utc)
    today = now.date()

    # Equity-Snapshot
    await state.save_equity_snapshot(
        strategy="orb", ts=now, equity=100000.0, cash=50000.0,
        drawdown_pct=-2.5, peak_equity=102500.0,
    )

    # Ein Trade öffnen + schließen
    tid = await state.open_trade_atomic(
        strategy="orb", symbol="AAPL", side="long",
        entry_ts=now, entry_price=150.0, qty=10.0,
    )
    await state.close_trade_atomic(
        trade_id=tid, exit_ts=now, exit_price=155.0, pnl=50.0,
    )

    # Ein offener Trade
    await state.open_trade_atomic(
        strategy="orb", symbol="MSFT", side="long",
        entry_ts=now, entry_price=300.0, qty=5.0,
    )

    # Signal + Anomaly
    await state.save_signal(strategy="orb", symbol="AAPL",
                            ts=now, action="BUY")

    snap = await state.get_health_snapshot("orb")
    assert snap["strategy"] == "orb"
    assert snap["equity"] == 100000.0
    assert snap["cash"] == 50000.0
    assert snap["drawdown_pct"] == -2.5
    assert snap["peak_equity"] == 102500.0
    assert snap["open_positions"] == 1  # MSFT
    assert snap["trades_today"] == 1   # AAPL geschlossen
    assert snap["pnl_today"] == 50.0
    assert snap["signals_today"] == 1
    assert snap["signals_filtered_today"] == 0
    assert snap["anomalies_last_hour"] == 0


@pytest.mark.asyncio
async def test_get_anomalies_returns_latest_first(state: PersistentState):
    """Anomalie-Reader liefert Dashboard-Daten in absteigender Zeitfolge."""
    older = datetime.now(timezone.utc) - timedelta(minutes=10)
    newer = datetime.now(timezone.utc) - timedelta(minutes=1)

    await state.log_anomaly(AnomalyEvent(
        timestamp=older,
        strategy="orb",
        check_name="duplicate_trade",
        severity=AlertLevel.WARNING,
        symbol="AAPL",
        message="Older warning",
    ))
    await state.log_anomaly(AnomalyEvent(
        timestamp=newer,
        strategy="orb",
        check_name="signal_flood",
        severity=AlertLevel.CRITICAL,
        symbol="MSFT",
        message="Newer critical",
    ))

    items = await state.get_anomalies(strategy="orb", limit=10)

    assert len(items) == 2
    assert items[0]["check_name"] == "signal_flood"
    assert items[0]["severity"] == "critical"
    assert items[1]["check_name"] == "duplicate_trade"


# ── Atomicity-Crash-Simulation ───────────────────────────────────────

@pytest.mark.asyncio
async def test_open_trade_atomic_rollback_on_error(tmp_path: Path):
    """Simulierter Crash: Bei Fehler im atomic open bleiben
    weder Trade noch Position zurück."""
    state = PersistentState(tmp_path / "crash_test.db")
    await state.ensure_schema()

    # Patch _conn um nach dem Trade-INSERT einen Fehler zu werfen
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
                # Crash nach dem Trade-INSERT (Schritt 1),
                # vor dem Position-INSERT (Schritt 2)
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
            strategy="orb",
            symbol="AAPL",
            side="long",
            entry_ts=datetime.now(timezone.utc),
            entry_price=150.0,
            qty=10.0,
        )

    # Restore original conn for reads
    state._conn = original_conn

    # Weder Trade noch Position dürfen existieren (Rollback)
    trades = await state.get_trades(strategy="orb")
    positions = await state.get_open_positions(strategy="orb")
    assert len(trades) == 0, "Trade should be rolled back"
    assert len(positions) == 0, "Position should not exist"


# ── Legacy save_trade / close_trade still work ──────────────────────

@pytest.mark.asyncio
async def test_legacy_save_and_close_trade(state: PersistentState):
    """Die alten Einzelmethoden funktionieren weiterhin."""
    trade_id = await state.save_trade(
        strategy="orb", symbol="AAPL", side="long",
        entry_ts=datetime.now(timezone.utc), entry_price=150.0, qty=10.0,
    )
    assert trade_id > 0

    await state.close_trade(
        trade_id=trade_id,
        exit_ts=datetime.now(timezone.utc),
        exit_price=155.0,
        pnl=50.0,
    )

    trades = await state.get_trades(strategy="orb", only_closed=True)
    assert len(trades) == 1
    assert trades[0]["pnl"] == 50.0


# ── Strategy-Status from trades-based reader ──────────────────────

@pytest.mark.asyncio
async def test_get_strategy_status(state: PersistentState):
    """get_strategy_status liefert korrektes Aggregat."""
    now = datetime.now(timezone.utc)

    await state.save_equity_snapshot(
        strategy="orb", ts=now, equity=100000.0, cash=50000.0,
        drawdown_pct=-1.0, peak_equity=101000.0,
    )

    tid = await state.open_trade_atomic(
        strategy="orb", symbol="AAPL", side="long",
        entry_ts=now, entry_price=150.0, qty=10.0,
    )

    status = await state.get_strategy_status("orb")
    assert status["strategy"] == "orb"
    assert status["equity"] == 100000.0
    assert status["open_positions"] == 1

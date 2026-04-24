"""Atomic state writer tests for PersistentState."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from live.state import PersistentState

BOT = "orb_spy_paper"
STRAT = "orb"
BOT2 = "botti_nq_live"
STRAT2 = "botti"


@pytest.mark.asyncio
async def test_open_trade_atomic_rollback_on_position_failure(tmp_path):
    """If position insert fails, open_trade_atomic must leave no partial rows."""
    state = PersistentState(tmp_path / "test.db")
    await state.ensure_schema()

    async with state._conn() as conn:
        await conn.execute(
            """
            CREATE TRIGGER fail_positions_insert
            BEFORE INSERT ON positions
            WHEN NEW.strategy = 'orb' AND NEW.symbol = 'AAPL'
            BEGIN
                SELECT RAISE(ABORT, 'forced positions insert failure');
            END;
            """
        )
        await conn.commit()

    trades_before = await state.get_trades(bot_name=BOT, strategy=STRAT)
    assert len(trades_before) == 0

    with pytest.raises(Exception):
        await state.open_trade_atomic(
            bot_name=BOT, strategy=STRAT,
            symbol="AAPL", side="BUY",
            entry_ts=datetime.now(timezone.utc),
            entry_price=150.0, qty=100,
        )

    trades_after = await state.get_trades(bot_name=BOT, strategy=STRAT)
    assert len(trades_after) == 0

    positions = await state.get_open_positions(BOT, STRAT)
    assert len(positions) == 0


@pytest.mark.asyncio
async def test_close_trade_atomic_rollback(tmp_path):
    """If delete position fails after trade update, close_trade_atomic rolls back."""
    state = PersistentState(tmp_path / "test.db")
    await state.ensure_schema()

    trade_id = await state.open_trade_atomic(
        bot_name=BOT, strategy=STRAT,
        symbol="AAPL", side="BUY",
        entry_ts=datetime.now(timezone.utc),
        entry_price=150.0, qty=100,
    )

    trades = await state.get_trades(bot_name=BOT, strategy=STRAT, only_closed=False)
    assert any(t["id"] == trade_id and t["exit_ts"] is None for t in trades)

    async with state._conn() as conn:
        await conn.execute(
            """
            CREATE TRIGGER fail_positions_delete
            BEFORE DELETE ON positions
            WHEN OLD.strategy = 'orb' AND OLD.symbol = 'AAPL'
            BEGIN
                SELECT RAISE(ABORT, 'forced positions delete failure');
            END;
            """
        )
        await conn.commit()

    with pytest.raises(Exception):
        await state.close_trade_atomic(
            bot_name=BOT, strategy=STRAT,
            trade_id=trade_id,
            exit_ts=datetime.now(timezone.utc),
            exit_price=148.0, pnl=-200.0, reason="stop_hit",
        )

    trades_after = await state.get_trades(bot_name=BOT, strategy=STRAT, only_closed=False)
    open_trade = next((t for t in trades_after if t["id"] == trade_id), None)
    assert open_trade is not None
    assert open_trade["exit_ts"] is None

    positions_after = await state.get_open_positions(BOT, STRAT)
    assert any(p["symbol"] == "AAPL" for p in positions_after)


@pytest.mark.asyncio
async def test_cooldown_bot_isolation(tmp_path):
    """Cooldown für einen Bot darf keinen anderen Bot blockieren."""
    state = PersistentState(tmp_path / "test.db")
    await state.ensure_schema()

    future = datetime.now(timezone.utc) + timedelta(hours=1)
    now = datetime.now(timezone.utc)
    await state.set_cooldown(bot_name=BOT, strategy=STRAT, symbol="AAPL", until=future)

    assert await state.is_in_cooldown(bot_name=BOT, strategy=STRAT, symbol="AAPL", now=now)
    assert not await state.is_in_cooldown(bot_name=BOT2, strategy=STRAT2, symbol="AAPL", now=now)

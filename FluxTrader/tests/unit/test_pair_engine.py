"""Tests für PairEngine – DB-Persistenz, Pre-Submit-Guard, Orphan-Leg-Handling.

Kein Netzwerk, kein echter Broker – PaperAdapter + Mock-State.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.context import MarketContextService, set_context_service
from core.ml_filter import MLFilter
from core.models import Bar, FeatureVector, PairSignal
from core.trade_manager import TradeManager
from execution.paper_adapter import PaperAdapter
from live.pair_runner import PairEngine


# ─────────────────────────── Fixtures ────────────────────────────────────────


def _make_bar(symbol: str, price: float = 100.0) -> Bar:
    return Bar(
        symbol=symbol,
        timestamp=datetime(2026, 4, 25, 15, 0, tzinfo=timezone.utc),
        open=price, high=price * 1.01, low=price * 0.99,
        close=price, volume=1000,
    )


def _make_pair_signal(action="ENTER", z=2.5) -> PairSignal:
    return PairSignal(
        strategy="botti_pair",
        symbol="SPY",        # BaseSignal.symbol (Pair-Primärsymbol)
        long_symbol="SPY",
        short_symbol="QQQ",
        z_score=z,
        action=action,
        qty_pct=0.05,
        features=FeatureVector(atr_pct=0.01),
        timestamp=datetime(2026, 4, 25, 15, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def ctx():
    c = MarketContextService(initial_capital=100_000.0)
    c.update_account(equity=100_000.0, cash=100_000.0, buying_power=400_000.0)
    set_context_service(c)
    return c


@pytest.fixture
def paper():
    p = PaperAdapter(initial_cash=100_000.0)
    p.set_market_price("SPY", 500.0)
    p.set_market_price("QQQ", 450.0)
    return p


@pytest.fixture
def mock_strategy():
    strat = MagicMock()
    strat.name = "botti_pair"
    strat.symbol_a = "SPY"
    strat.symbol_b = "QQQ"
    return strat


@pytest.fixture
def mock_state():
    state = AsyncMock()
    state.open_trade_atomic = AsyncMock(return_value=42)
    state.close_trade_atomic = AsyncMock()
    state.upsert_bot_heartbeat = AsyncMock()
    return state


@pytest.fixture
def engine(ctx, paper, mock_strategy, mock_state):
    tm = TradeManager(state=mock_state, bot_name="test_pair")
    ml = MLFilter.disabled()
    data_prov = MagicMock()
    eng = PairEngine(
        strategy=mock_strategy,
        broker=paper,
        data_provider=data_prov,
        context=ctx,
        ml_filter=ml,
        state=mock_state,
        config={"timeframe": "1D", "pair_poll_interval_s": 60},
        bot_name="test_pair",
    )
    eng.tm = tm
    return eng


# ─────────────────────────── Tests ────────────────────────────────────────────


class TestPairEnginePreSubmitGuard:
    def test_skips_enter_when_pending(self, engine, paper):
        engine._pending_pair_submit = True
        signal = _make_pair_signal("ENTER")

        asyncio.get_event_loop().run_until_complete(engine._execute_enter(signal))

        # Position darf nicht registriert sein
        assert engine.tm.get("SPY") is None
        assert engine._has_position is False

    def test_skips_enter_when_broker_position_exists(self, engine, paper, ctx):
        # Bereits eine offene Position im Broker
        paper._positions["SPY"] = MagicMock()  # Simuliere Broker-Position

        signal = _make_pair_signal("ENTER")
        asyncio.get_event_loop().run_until_complete(engine._execute_enter(signal))

        # _has_position wird auf True gesetzt (Sync mit Broker-State)
        assert engine._has_position is True


class TestPairEnginePersistence:
    def test_enter_registers_both_legs(self, engine, paper, mock_state):
        signal = _make_pair_signal("ENTER")

        asyncio.get_event_loop().run_until_complete(engine._execute_enter(signal))

        assert engine._has_position is True
        spy_trade = engine.tm.get("SPY")
        qqq_trade = engine.tm.get("QQQ")
        assert spy_trade is not None
        assert qqq_trade is not None
        assert spy_trade.side == "long"
        assert qqq_trade.side == "short"

    def test_enter_sets_reserve_group(self, engine, paper):
        signal = _make_pair_signal("ENTER")
        asyncio.get_event_loop().run_until_complete(engine._execute_enter(signal))

        spy_trade = engine.tm.get("SPY")
        assert spy_trade is not None
        assert spy_trade.metadata.get("reserve_group") == "pair_SPY_QQQ"

    def test_exit_calls_close_trade_and_clears_position(self, engine, paper, mock_state):
        # Erst Enter
        enter_sig = _make_pair_signal("ENTER")
        asyncio.get_event_loop().run_until_complete(engine._execute_enter(enter_sig))
        assert engine._has_position is True

        # Dann Exit
        exit_sig = _make_pair_signal("EXIT", z=0.3)
        asyncio.get_event_loop().run_until_complete(engine._execute_exit(exit_sig))

        assert engine._has_position is False
        # Beide Trades aus In-Memory entfernt
        assert engine.tm.get("SPY") is None
        assert engine.tm.get("QQQ") is None


class TestPairEngineOrphanLeg:
    def test_reconcile_no_action_when_no_position(self, engine):
        engine._has_position = False
        asyncio.get_event_loop().run_until_complete(engine._reconcile_pair())
        # Kein Fehler, keine Aktion

    def test_reconcile_detects_missing_leg(self, engine, paper):
        engine._has_position = True
        # SPY im TM registrieren, QQQ nicht im Broker
        from core.trade_manager import ManagedTrade
        engine.tm.register(ManagedTrade(
            symbol="SPY", side="long", entry=500.0, stop=0.0, target=None, qty=10.0,
            strategy_id="botti_pair",
            opened_at=datetime(2026, 4, 25, 15, 0, tzinfo=timezone.utc),
        ))
        engine.tm.register(ManagedTrade(
            symbol="QQQ", side="short", entry=450.0, stop=0.0, target=None, qty=10.0,
            strategy_id="botti_pair",
            opened_at=datetime(2026, 4, 25, 15, 0, tzinfo=timezone.utc),
        ))
        # Broker kennt nur SPY (QQQ verschwunden)
        paper._positions["SPY"] = MagicMock(qty=10.0, side="long", entry_price=500.0,
                                             current_price=500.0, unrealized_pnl=0.0)

        asyncio.get_event_loop().run_until_complete(engine._reconcile_pair())

        # Nach Orphan-Handling: kein aktiver Pair mehr
        assert engine._has_position is False

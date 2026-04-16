"""Integration-Test: BarByBarEngine + ORBStrategy + PaperAdapter.

Deterministisch, kein Netzwerk. Prüft den gesamten Pfad:
Strategy.on_bar -> Signal -> BrokerPort.execute_signal -> PaperAdapter fill
-> TradeManager SL/TP -> Equity-Curve.
"""
from __future__ import annotations

import asyncio
from datetime import time

import pytest

from backtest.engine import BacktestConfig, BarByBarEngine
from backtest.report import build_tearsheet, format_tearsheet
from core.context import MarketContextService, reset_context_service
from execution.paper_adapter import PaperAdapter
from strategy.orb import ORBStrategy
from tests.conftest import make_ohlcv, _et_dt


@pytest.fixture()
def bt_context():
    ctx = MarketContextService(initial_capital=100_000.0)
    ctx.update_account(equity=100_000.0, cash=100_000.0, buying_power=400_000.0)
    yield ctx
    reset_context_service()


@pytest.fixture()
def bt_paper():
    return PaperAdapter(initial_cash=100_000.0, slippage_pct=0.0001,
                        commission_pct=0.00005)


class TestBarByBarEngine:
    def test_empty_data(self, bt_context, bt_paper):
        strat = ORBStrategy({
            "use_mit_probabilistic_overlay": False,
            "use_trend_filter": False,
            "use_gap_filter": False,
        }, context=bt_context)
        cfg = BacktestConfig(initial_capital=100_000.0)
        engine = BarByBarEngine(strat, bt_paper, bt_context, cfg)
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(data={})
        )
        assert result.bars_processed == 0
        assert result.final_equity == 100_000.0
        assert result.trades == []

    def test_runs_without_error(self, bt_context, bt_paper, spy_df):
        """Engine mit echten 5m-Bars läuft ohne Exception durch."""
        bt_context.set_spy_df(spy_df)
        strat = ORBStrategy({
            "min_bars": 5,
            "use_mit_probabilistic_overlay": False,
            "use_trend_filter": False,
            "use_gap_filter": False,
            "use_time_decay_filter": False,
        }, context=bt_context)
        cfg = BacktestConfig(initial_capital=100_000.0,
                             eod_close_time=time(15, 55))
        engine = BarByBarEngine(strat, bt_paper, bt_context, cfg)

        # 2 Tage à ~78 Bars
        data = {
            "AAPL": make_ohlcv(78, base=150.0, seed=10,
                               start=_et_dt(2025, 3, 12, 9, 30)),
        }
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(data=data, spy_df=spy_df)
        )
        assert result.bars_processed == 78
        assert result.final_equity > 0
        assert not result.equity_curve.empty

    def test_tearsheet_from_result(self, bt_context, bt_paper, spy_df):
        bt_context.set_spy_df(spy_df)
        strat = ORBStrategy({
            "min_bars": 5,
            "use_mit_probabilistic_overlay": False,
            "use_trend_filter": False,
            "use_gap_filter": False,
        }, context=bt_context)
        cfg = BacktestConfig(initial_capital=100_000.0)
        engine = BarByBarEngine(strat, bt_paper, bt_context, cfg)

        data = {"AAPL": make_ohlcv(50, base=150.0, seed=20,
                                    start=_et_dt(2025, 3, 12, 9, 30))}
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(data=data, spy_df=spy_df)
        )
        ts = build_tearsheet(result.equity_curve, result.trades,
                             result.initial_capital)
        assert ts.initial_capital == 100_000.0
        text = format_tearsheet(ts)
        assert "Initial" in text
        assert "Sharpe" in text


class TestPaperAdapterIsolation:
    @pytest.mark.asyncio
    async def test_paper_no_network(self):
        """PaperAdapter funktioniert komplett ohne Netzwerk."""
        paper = PaperAdapter(initial_cash=50_000.0)
        paper.set_market_price("MSFT", 420.0)

        from core.models import OrderRequest, OrderSide
        order_id = await paper.submit_order(OrderRequest(
            symbol="MSFT", side=OrderSide.BUY, qty=10,
        ))
        assert order_id.startswith("paper-")

        pos = await paper.get_position("MSFT")
        assert pos is not None
        assert pos.symbol == "MSFT"
        assert pos.qty == 10

        acct = await paper.get_account()
        assert acct["paper"] is True
        assert acct["equity"] > 0

    @pytest.mark.asyncio
    async def test_recent_closes_contains_exact_fill(self):
        paper = PaperAdapter(initial_cash=50_000.0, slippage_pct=0.0,
                             commission_pct=0.0)
        paper.set_market_price("MSFT", 420.0)

        from core.models import OrderRequest, OrderSide
        await paper.submit_order(OrderRequest(
            symbol="MSFT", side=OrderSide.BUY, qty=10,
        ))

        paper.set_market_price("MSFT", 430.0)
        close_id = await paper.submit_order(OrderRequest(
            symbol="MSFT", side=OrderSide.SELL, qty=10,
        ))

        closes = await paper.get_recent_closes(["MSFT"])
        close = closes.get("MSFT")

        assert close is not None
        assert close.order_id == close_id
        assert close.fill_price == 430.0
        assert close.qty == 10.0
        assert close.realized_pnl == pytest.approx(100.0)

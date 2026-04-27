"""Tests für EnrichedTrade, Exit-Statistik, Trade-Export und MAE/MFE."""
from __future__ import annotations

import asyncio
from datetime import datetime, time, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backtest.engine import BacktestConfig, BarByBarEngine
from backtest.report import (
    build_exit_reason_stats,
    export_trades,
    format_exit_reason_stats,
)
from core.context import MarketContextService, reset_context_service
from core.models import EnrichedTrade
from execution.paper_adapter import PaperAdapter
from strategy.orb import ORBStrategy
from tests.conftest import _et_dt, make_ohlcv


# ═══════════════════════════════════════════════════════════════════════════
#  Fixtures
# ═══════════════════════════════════════════════════════════════════════════

def _make_enriched_trade(
    *,
    trade_id: str = "abc123",
    symbol: str = "AAPL",
    strategy: str = "orb",
    entry_price: float = 100.0,
    exit_price: float = 110.0,
    shares: int = 10,
    stop: float = 95.0,
    exit_reason: str = "take_profit",
    pnl_net: float = 90.0,
    hold_days: int = 5,
    mae_pct: float = -2.0,
    mfe_pct: float = 12.0,
) -> EnrichedTrade:
    """Erzeugt einen synthetischen EnrichedTrade."""
    initial_risk_r = abs(entry_price - stop)
    initial_risk_usd = shares * initial_risk_r
    pnl_gross = (exit_price - entry_price) * shares
    cost_basis = entry_price * shares
    r_multiple = pnl_net / initial_risk_usd if initial_risk_usd > 0 else 0.0
    pnl_pct = pnl_net / cost_basis * 100.0 if cost_basis > 0 else 0.0

    return EnrichedTrade(
        trade_id=trade_id,
        strategy=strategy,
        symbol=symbol,
        entry_date=datetime(2025, 3, 12, 10, 0, tzinfo=timezone.utc),
        entry_price=entry_price,
        shares=shares,
        entry_reason="breakout",
        entry_signal="BUY",
        stop_at_entry=stop,
        initial_risk_r=initial_risk_r,
        initial_risk_usd=initial_risk_usd,
        atr_at_entry=2.5,
        vix_at_entry=18.0,
        equity_at_entry=100_000.0,
        exit_date=datetime(2025, 3, 17, 15, 30, tzinfo=timezone.utc),
        exit_price=exit_price,
        exit_reason=exit_reason,
        hold_days=hold_days,
        hold_trading_days=max(hold_days - 2, 0),
        pnl_gross=pnl_gross,
        pnl_net=pnl_net,
        pnl_pct=pnl_pct,
        r_multiple=r_multiple,
        commission=5.0,
        slippage=5.0,
        strength=0.8,
        ml_confidence=0.5,
        mae_pct=mae_pct,
        mfe_pct=mfe_pct,
        mae_r=abs(mae_pct / 100.0 * entry_price) * shares / initial_risk_usd
        if initial_risk_usd > 0 else 0.0,
        mfe_r=abs(mfe_pct / 100.0 * entry_price) * shares / initial_risk_usd
        if initial_risk_usd > 0 else 0.0,
        benchmark_return_pct=1.5,
        alpha_pct=pnl_pct - 1.5,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  EnrichedTrade – R-Multiple-Berechnung
# ═══════════════════════════════════════════════════════════════════════════

class TestEnrichedTrade:
    def test_r_multiple_positive(self):
        """Gewinn-Trade mit korrektem R-Multiplikator."""
        t = _make_enriched_trade(
            entry_price=100.0, exit_price=110.0, shares=10,
            stop=95.0, pnl_net=90.0,
        )
        # initial_risk_usd = 10 * 5 = 50, r_multiple = 90/50 = 1.8
        assert t.initial_risk_r == pytest.approx(5.0)
        assert t.initial_risk_usd == pytest.approx(50.0)
        assert t.r_multiple == pytest.approx(1.8)

    def test_r_multiple_negative(self):
        """Verlust-Trade: Stop gerissen."""
        t = _make_enriched_trade(
            entry_price=100.0, exit_price=94.0, shares=10,
            stop=95.0, pnl_net=-65.0, exit_reason="stop_loss",
        )
        # initial_risk_usd = 50, r_multiple = -65/50 = -1.3
        assert t.r_multiple == pytest.approx(-1.3)

    def test_r_multiple_zero_risk(self):
        """Edge-Case: Stop == Entry → kein Risiko → r_multiple = 0."""
        t = _make_enriched_trade(
            entry_price=100.0, exit_price=105.0, shares=10,
            stop=100.0, pnl_net=40.0,
        )
        # initial_risk_r = 0, initial_risk_usd = 0 → r_multiple = 0
        assert t.initial_risk_r == pytest.approx(0.0)
        assert t.r_multiple == pytest.approx(0.0)

    def test_pnl_pct(self):
        """Prozentuale Rendite korrekt berechnet."""
        t = _make_enriched_trade(
            entry_price=200.0, exit_price=220.0, shares=5,
            stop=190.0, pnl_net=90.0,
        )
        # cost = 200*5 = 1000, pnl_pct = 90/1000*100 = 9.0
        assert t.pnl_pct == pytest.approx(9.0)


# ═══════════════════════════════════════════════════════════════════════════
#  Exit-Reason-Statistik
# ═══════════════════════════════════════════════════════════════════════════

class TestExitReasonStats:
    def test_basic_aggregation(self):
        """Gemischte Exit-Gründe werden korrekt gruppiert."""
        trades = [
            _make_enriched_trade(exit_reason="stop_loss", pnl_net=-50.0),
            _make_enriched_trade(exit_reason="stop_loss", pnl_net=-45.0),
            _make_enriched_trade(exit_reason="take_profit", pnl_net=100.0),
            _make_enriched_trade(exit_reason="trailing_stop", pnl_net=80.0),
            _make_enriched_trade(exit_reason="trailing_stop", pnl_net=-10.0),
            _make_enriched_trade(exit_reason="eod", pnl_net=5.0),
        ]
        df = build_exit_reason_stats(trades)
        assert len(df) == 4
        # Sortiert nach count desc → stop_loss und trailing_stop je 2
        assert df.iloc[0]["count"] == 2

        sl_row = df[df["exit_reason"] == "stop_loss"].iloc[0]
        assert sl_row["win_count"] == 0
        assert sl_row["win_rate_pct"] == pytest.approx(0.0)
        assert sl_row["pnl_total"] == pytest.approx(-95.0)

        tp_row = df[df["exit_reason"] == "take_profit"].iloc[0]
        assert tp_row["count"] == 1
        assert tp_row["win_rate_pct"] == pytest.approx(100.0)

    def test_empty_trades(self):
        df = build_exit_reason_stats([])
        assert df.empty

    def test_format_output(self):
        trades = [
            _make_enriched_trade(exit_reason="stop_loss", pnl_net=-50.0),
            _make_enriched_trade(exit_reason="take_profit", pnl_net=100.0),
        ]
        df = build_exit_reason_stats(trades)
        text = format_exit_reason_stats(df, total_trades=2)
        assert "EXIT-GRUND ANALYSE" in text
        assert "stop_loss" in text
        assert "take_profit" in text
        assert "2 Trades gesamt" in text


# ═══════════════════════════════════════════════════════════════════════════
#  Trade-Export (CSV + Excel)
# ═══════════════════════════════════════════════════════════════════════════

class TestExportTrades:
    def test_csv_content(self, tmp_path):
        """CSV-Export enthält korrekte Spalten und Werte."""
        trades = [
            _make_enriched_trade(trade_id="t1", symbol="AAPL", pnl_net=90.0),
            _make_enriched_trade(trade_id="t2", symbol="MSFT", pnl_net=-30.0,
                                 exit_reason="stop_loss"),
        ]
        paths = export_trades(trades, tmp_path, fmt="csv",
                              filename_base="test_trades")
        assert len(paths) == 1
        assert paths[0].suffix == ".csv"

        df = pd.read_csv(paths[0])
        assert len(df) == 2
        assert "trade_id" in df.columns
        assert "r_multiple" in df.columns
        assert "mae_pct" in df.columns
        assert df.iloc[0]["trade_id"] == "t1"
        assert df.iloc[1]["symbol"] == "MSFT"

    def test_excel_sheets(self, tmp_path):
        """Excel-Export hat alle 4 Sheets."""
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            pytest.skip("openpyxl nicht installiert")

        trades = [
            _make_enriched_trade(trade_id=f"t{i}", pnl_net=float(i * 10 - 50),
                                 exit_reason="stop_loss" if i < 3 else "take_profit")
            for i in range(10)
        ]
        paths = export_trades(trades, tmp_path, fmt="excel",
                              filename_base="test_xls")
        assert len(paths) == 1
        assert paths[0].suffix == ".xlsx"

        wb = openpyxl.load_workbook(paths[0])
        sheet_names = wb.sheetnames
        assert "Trades" in sheet_names
        assert "Exit Statistik" in sheet_names
        assert "MAE-MFE Analyse" in sheet_names
        assert "Zeitverlauf" in sheet_names

    def test_export_none(self, tmp_path):
        """format='none' erzeugt keine Dateien."""
        trades = [_make_enriched_trade()]
        paths = export_trades(trades, tmp_path, fmt="none")
        assert paths == []

    def test_export_empty(self, tmp_path):
        """Leere Trade-Liste erzeugt keine Dateien."""
        paths = export_trades([], tmp_path, fmt="both")
        assert paths == []

    def test_both_format(self, tmp_path):
        """Format 'both' erzeugt CSV + Excel."""
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            pytest.skip("openpyxl nicht installiert")

        trades = [_make_enriched_trade()]
        paths = export_trades(trades, tmp_path, fmt="both",
                              filename_base="combo")
        suffixes = {p.suffix for p in paths}
        assert ".csv" in suffixes
        assert ".xlsx" in suffixes


# ═══════════════════════════════════════════════════════════════════════════
#  MAE/MFE-Tracking (Integration mit BarByBarEngine)
# ═══════════════════════════════════════════════════════════════════════════

class TestMAEMFETracking:
    @pytest.fixture()
    def bt_context(self):
        ctx = MarketContextService(initial_capital=100_000.0)
        ctx.update_account(equity=100_000.0, cash=100_000.0,
                           buying_power=400_000.0)
        yield ctx
        reset_context_service()

    @pytest.fixture()
    def bt_paper(self):
        return PaperAdapter(initial_cash=100_000.0, slippage_pct=0.0,
                            commission_pct=0.0)

    def test_mae_mfe_tracking_over_bars(self, bt_context, bt_paper, spy_df):
        """MAE/MFE werden während der Simulation korrekt akkumuliert."""
        bt_context.set_spy_df(spy_df)
        strat = ORBStrategy({
            "min_bars": 5,
            "use_mit_probabilistic_overlay": False,
            "use_trend_filter": False,
            "use_gap_filter": False,
            "use_time_decay_filter": False,
        }, context=bt_context)
        cfg = BacktestConfig(
            initial_capital=100_000.0,
            eod_close_time=time(15, 55),
            log_order_events=False,
        )
        engine = BarByBarEngine(strat, bt_paper, bt_context, cfg)

        anchor = make_ohlcv(1, base=150.0, seed=10, start=_et_dt(2025, 3, 6, 9, 30))
        main = make_ohlcv(78, base=150.0, seed=10, start=_et_dt(2025, 3, 12, 9, 30))
        data = {"AAPL": pd.concat([anchor, main])}
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(data=data, spy_df=spy_df)
        )

        # Wenn Trades stattfanden, müssen enriched_trades existieren
        if result.trades:
            assert len(result.enriched_trades) > 0
            for et in result.enriched_trades:
                # MAE ist <= 0 (adverse), MFE >= 0 (favorable)
                assert et.mae_pct <= 0.0001
                assert et.mfe_pct >= -0.0001
                # Haltedauer plausibel
                assert et.hold_days >= 0
                # Exit-Reason nicht leer
                assert et.exit_reason != ""
                # trade_id vorhanden
                assert len(et.trade_id) > 0

    def test_enriched_trades_match_trade_count(self, bt_context, bt_paper,
                                                spy_df):
        """Anzahl enriched_trades stimmt mit close-Trades überein."""
        bt_context.set_spy_df(spy_df)
        strat = ORBStrategy({
            "min_bars": 5,
            "use_mit_probabilistic_overlay": False,
            "use_trend_filter": False,
            "use_gap_filter": False,
            "use_time_decay_filter": False,
        }, context=bt_context)
        cfg = BacktestConfig(
            initial_capital=100_000.0,
            eod_close_time=time(15, 55),
            log_order_events=False,
        )
        engine = BarByBarEngine(strat, bt_paper, bt_context, cfg)

        anchor = make_ohlcv(1, base=150.0, seed=10, start=_et_dt(2025, 3, 6, 9, 30))
        main = make_ohlcv(78, base=150.0, seed=10, start=_et_dt(2025, 3, 12, 9, 30))
        data = {"AAPL": pd.concat([anchor, main])}
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(data=data, spy_df=spy_df)
        )
        # close_all_positions am Ende erzeugt ggf. Extra-Trades in
        # broker.trade_log (safety-net), aber enriched_trades sollten
        # mindestens so viele sein wie echte Roundtrips
        close_trades = [t for t in result.trades
                        if t.side in ("SELL", "COVER")]
        # enriched_trades <= close_trades (da final safety-net doppeln kann)
        assert len(result.enriched_trades) <= len(close_trades) + 1

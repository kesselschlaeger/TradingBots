"""Tests für den framework-weiten required_warmup_days()-Vertrag (CLAUDE.md Regel 7).

Spec:
  1. Jede Einzelsymbol-Strategie liefert einen sinnvollen int-Wert (kein None).
  2. Kritische Config-Parameter (use_multi_timeframe, asset_class, lookback_bars)
     ändern den Rückgabewert entsprechend.
  3. test_all_registered_strategies_declare_warmup – keine Strategie gibt None zurück.
  4. test_pair_strategies_declare_warmup – PairStrategy-ABC-Vertrag erfüllt.
  5. LiveRunner bricht bei zu kurzem Warmup für Botti und OBB ab (pre-flight).
"""
from __future__ import annotations

import asyncio
import math
from datetime import datetime, time, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from core.context import MarketContextService, reset_context_service, set_context_service


def _arun(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def context():
    svc = MarketContextService(initial_capital=100_000.0, bar_buffer=200)
    svc.update_account(equity=100_000.0, cash=100_000.0, buying_power=400_000.0)
    set_context_service(svc)
    yield svc
    reset_context_service()


def _make_runner(strategy, context, warmup_days=None, data_lookback=None):
    """LiveRunner mit gemockten Abhängigkeiten für Pre-flight-Tests."""
    from live.runner import LiveRunner

    cfg: dict = {"obb_exit_open_time": time(0, 0)}
    if warmup_days is not None:
        cfg["warmup_days"] = warmup_days

    data_cfg = None
    if data_lookback is not None:
        data_cfg = MagicMock()
        data_cfg.lookback_days = data_lookback

    broker = AsyncMock()
    broker.get_account = AsyncMock(
        return_value={"equity": 100_000.0, "cash": 100_000.0, "buying_power": 200_000.0}
    )
    broker.get_positions = AsyncMock(return_value={})
    broker.get_position = AsyncMock(return_value=None)

    data_provider = AsyncMock()
    data_provider.get_bars_bulk = AsyncMock(return_value={})
    data_provider.get_bars = AsyncMock(return_value=pd.DataFrame())

    state = AsyncMock()
    state.ensure_schema = AsyncMock()
    state.update_peak_equity = AsyncMock(return_value=100_000.0)
    state.save_equity_snapshot = AsyncMock()
    state.reset_day = AsyncMock()
    state.reserved_groups = AsyncMock(return_value=[])

    return LiveRunner(
        strategy=strategy,
        broker=broker,
        data_provider=data_provider,
        context=context,
        state=state,
        notifier=AsyncMock(),
        symbols=["AAPL"],
        config=cfg,
        data_cfg=data_cfg,
    )


# ── Test 1: ORBStrategy ───────────────────────────────────────────────────────

class TestRequiredWarmupDaysORB:
    def test_returns_constant(self, context):
        from strategy.orb import ORB_REQUIRED_WARMUP_DAYS, ORBStrategy

        strat = ORBStrategy({}, context=context)
        result = strat.required_warmup_days()

        assert isinstance(result, int)
        assert result == ORB_REQUIRED_WARMUP_DAYS
        assert result == 5

    def test_value_is_stable_regardless_of_trend_filter_flag(self, context):
        """ORB-Warmup ist unabhängig von use_trend_filter (SPY-Trend bleibt Hard Prereq)."""
        from strategy.orb import ORBStrategy

        strat_on = ORBStrategy({"use_trend_filter": True}, context=context)
        strat_off = ORBStrategy({"use_trend_filter": False}, context=context)

        assert strat_on.required_warmup_days() == strat_off.required_warmup_days()


# ── Test 2: OBBStrategy ───────────────────────────────────────────────────────

class TestRequiredWarmupDaysOBB:
    def test_default_returns_at_least_constant(self, context):
        from strategy.obb import OBB_REQUIRED_WARMUP_DAYS, OBBStrategy

        strat = OBBStrategy({}, context=context)
        result = strat.required_warmup_days()

        assert isinstance(result, int)
        assert result >= OBB_REQUIRED_WARMUP_DAYS

    def test_default_lookback_50(self, context):
        from strategy.obb import OBBStrategy

        strat = OBBStrategy({"lookback_bars": 50}, context=context)
        # ceil(50 × 7/5) + 3 = 73; max(75, 73) = 75
        assert strat.required_warmup_days() == 75

    def test_larger_lookback_grows_requirement(self, context):
        from strategy.obb import OBBStrategy

        strat_50 = OBBStrategy({"lookback_bars": 50}, context=context)
        strat_100 = OBBStrategy({"lookback_bars": 100}, context=context)

        assert strat_100.required_warmup_days() > strat_50.required_warmup_days()

    def test_lookback_100_formula(self, context):
        from strategy.obb import OBBStrategy

        strat = OBBStrategy({"lookback_bars": 100}, context=context)
        expected = max(75, math.ceil(100 * 7 / 5) + 3)  # max(75, 143) = 143
        assert strat.required_warmup_days() == expected


# ── Test 3: BottiStrategy ─────────────────────────────────────────────────────

class TestRequiredWarmupDaysBotti:
    def test_default_returns_int_above_50(self, context):
        from strategy.botti import BottiStrategy

        strat = BottiStrategy({}, context=context)
        result = strat.required_warmup_days()

        assert isinstance(result, int)
        assert result >= 50

    def test_mtf_off_baseline(self, context):
        from strategy.botti import BottiStrategy

        strat = BottiStrategy({"use_multi_timeframe": False}, context=context)
        # max(30, 35, 28, 20) = 35 Handelstage → ceil(35×7/5)+3 = 52; max(50,52)=52
        assert strat.required_warmup_days() == 52

    def test_mtf_on_increases_requirement(self, context):
        from strategy.botti import BottiStrategy

        strat_off = BottiStrategy({"use_multi_timeframe": False}, context=context)
        strat_on = BottiStrategy(
            {"use_multi_timeframe": True, "mtf_breakout_lookback": 5},
            context=context,
        )

        assert strat_on.required_warmup_days() > strat_off.required_warmup_days()

    def test_mtf_on_formula(self, context):
        from strategy.botti import BottiStrategy

        strat = BottiStrategy(
            {"use_multi_timeframe": True, "mtf_breakout_lookback": 5},
            context=context,
        )
        # max(30,35,28,20)=35 + 5 MTF = 40 Handelstage → ceil(40×7/5)+3=59; max(50,59)=59
        assert strat.required_warmup_days() == 59


# ── Test 4: IctOrderBlockStrategy ────────────────────────────────────────────

class TestRequiredWarmupDaysICT:
    def test_equity_returns_10(self, context):
        from strategy.ict_ob import ICT_OB_REQUIRED_WARMUP_DAYS_EQUITY, IctOrderBlockStrategy

        strat = IctOrderBlockStrategy({"asset_class": "equity"}, context=context)
        result = strat.required_warmup_days()

        assert isinstance(result, int)
        assert result == ICT_OB_REQUIRED_WARMUP_DAYS_EQUITY
        assert result == 10

    def test_futures_returns_equity_value(self, context):
        from strategy.ict_ob import ICT_OB_REQUIRED_WARMUP_DAYS_EQUITY, IctOrderBlockStrategy

        strat = IctOrderBlockStrategy({"asset_class": "futures"}, context=context)
        assert strat.required_warmup_days() == ICT_OB_REQUIRED_WARMUP_DAYS_EQUITY

    def test_crypto_returns_smaller_value(self, context):
        from strategy.ict_ob import (
            ICT_OB_REQUIRED_WARMUP_DAYS_CRYPTO,
            ICT_OB_REQUIRED_WARMUP_DAYS_EQUITY,
            IctOrderBlockStrategy,
        )

        strat = IctOrderBlockStrategy({"asset_class": "crypto"}, context=context)
        result = strat.required_warmup_days()

        assert result == ICT_OB_REQUIRED_WARMUP_DAYS_CRYPTO
        assert result == 7
        assert result < ICT_OB_REQUIRED_WARMUP_DAYS_EQUITY

    def test_default_asset_class_is_equity(self, context):
        from strategy.ict_ob import ICT_OB_REQUIRED_WARMUP_DAYS_EQUITY, IctOrderBlockStrategy

        strat = IctOrderBlockStrategy({}, context=context)
        assert strat.required_warmup_days() == ICT_OB_REQUIRED_WARMUP_DAYS_EQUITY


# ── Test 5: BottiPairStrategy ─────────────────────────────────────────────────

class TestRequiredWarmupDaysBottiPair:
    def test_default_returns_int(self, context):
        from strategy.botti_pair import BottiPairStrategy

        strat = BottiPairStrategy({}, context=context)
        result = strat.required_warmup_days()

        assert isinstance(result, int)
        assert result >= 5

    def test_pair_lookback_20_formula(self, context):
        from strategy.botti_pair import BottiPairStrategy

        strat = BottiPairStrategy({"pair_lookback": 20}, context=context)
        # max(5, ceil(20×7/5)+3) = max(5, 31) = 31
        assert strat.required_warmup_days() == 31

    def test_larger_pair_lookback_grows_requirement(self, context):
        from strategy.botti_pair import BottiPairStrategy

        strat_20 = BottiPairStrategy({"pair_lookback": 20}, context=context)
        strat_50 = BottiPairStrategy({"pair_lookback": 50}, context=context)

        assert strat_50.required_warmup_days() > strat_20.required_warmup_days()


# ── Test 6: framework-weite Registry-Prüfung ─────────────────────────────────

class TestAllRegisteredStrategiesDeclareWarmup:
    def test_all_registered_strategies_declare_warmup(self, context):
        """Kein None-Rückgabewert nach diesem Patch – alle Strategien haben Vertrag."""
        # Alle Module importieren, damit @register greift
        import strategy.botti       # noqa: F401
        import strategy.botti_pair  # noqa: F401
        import strategy.ict_ob      # noqa: F401
        import strategy.obb         # noqa: F401
        import strategy.orb         # noqa: F401
        import strategy.quick_flip  # noqa: F401

        from strategy.base import BaseStrategy
        from strategy.registry import StrategyRegistry

        failures: list[str] = []
        for name, cls in StrategyRegistry.classes().items():
            if not issubclass(cls, BaseStrategy):
                continue  # PairStrategy separat geprüft
            strat = cls({}, context=context)
            result = strat.required_warmup_days()
            if result is None:
                failures.append(f"{name}: required_warmup_days() returned None")
            elif not isinstance(result, int):
                failures.append(f"{name}: required_warmup_days() returned {type(result).__name__}, expected int")

        assert not failures, "Strategien ohne Warmup-Vertrag:\n" + "\n".join(failures)


class TestPairStrategiesDeclareWarmup:
    def test_pair_strategies_declare_warmup(self, context):
        """PairStrategy-Subklassen erfüllen den required_warmup_days()-Vertrag."""
        import strategy.botti_pair  # noqa: F401

        from strategy.base import PairStrategy
        from strategy.registry import StrategyRegistry

        failures: list[str] = []
        for name, cls in StrategyRegistry.classes().items():
            if not issubclass(cls, PairStrategy):
                continue
            strat = cls({}, context=context)
            if not hasattr(strat, "required_warmup_days"):
                failures.append(f"{name}: required_warmup_days() fehlt")
                continue
            result = strat.required_warmup_days()
            if result is None:
                failures.append(f"{name}: required_warmup_days() returned None")
            elif not isinstance(result, int):
                failures.append(f"{name}: returned {type(result).__name__}, expected int")

        assert not failures, "Pair-Strategien ohne Warmup-Vertrag:\n" + "\n".join(failures)


# ── Test 7: LiveRunner Pre-flight für Botti und OBB ──────────────────────────

class TestRunnerAbortsOnShortWarmupBotti:
    def test_runner_aborts_on_short_warmup(self, context):
        """Botti mit warmup_days < required → RuntimeError beim Warmup."""
        from strategy.botti import BottiStrategy

        strat = BottiStrategy({"use_multi_timeframe": False}, context=context)
        required = strat.required_warmup_days()  # 52

        runner = _make_runner(strat, context, warmup_days=required - 10)
        with pytest.raises(RuntimeError, match="warmup_days"):
            _arun(runner._warmup())

    def test_runner_ok_with_sufficient_warmup(self, context):
        """Botti mit warmup_days >= required → kein Fehler."""
        from strategy.botti import BottiStrategy

        strat = BottiStrategy({"use_multi_timeframe": False}, context=context)
        required = strat.required_warmup_days()

        runner = _make_runner(strat, context, warmup_days=required)
        _arun(runner._warmup())  # darf nicht werfen


class TestRunnerAbortsOnShortWarmupOBB:
    def test_runner_aborts_on_short_warmup(self, context):
        """OBB mit warmup_days < 75 → RuntimeError beim Warmup."""
        from strategy.obb import OBBStrategy

        strat = OBBStrategy({"lookback_bars": 50}, context=context)
        required = strat.required_warmup_days()  # 75

        runner = _make_runner(strat, context, warmup_days=required - 20)
        with pytest.raises(RuntimeError, match="warmup_days"):
            _arun(runner._warmup())

    def test_runner_ok_with_sufficient_warmup(self, context):
        """OBB mit warmup_days >= 75 → kein Fehler."""
        from strategy.obb import OBBStrategy

        strat = OBBStrategy({"lookback_bars": 50}, context=context)
        required = strat.required_warmup_days()  # 75

        runner = _make_runner(strat, context, warmup_days=required)
        _arun(runner._warmup())  # darf nicht werfen

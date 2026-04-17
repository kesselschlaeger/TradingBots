"""Tests für Pair-Trading: BottiPairStrategy, KalmanSpreadEstimator, PairEngine.

Kein Netzwerk, kein Broker – deterministische Bars + injizierter Context.
"""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pytest

from core.context import MarketContextService
from core.indicators import KalmanSpreadEstimator
from core.models import Bar, BaseSignal, FeatureVector, PairSignal
from strategy.botti_pair import BottiPairStrategy
from tests.conftest import _et_dt


# ─────────────────────────── KalmanSpreadEstimator ────────────────────────────


class TestKalmanSpreadEstimator:
    def test_initial_update_returns_value(self):
        k = KalmanSpreadEstimator(q=1e-5, r=0.01)
        mean, var = k.update(100.0)
        assert mean == 100.0
        assert var < 1.0

    def test_converges_to_series_mean(self):
        k = KalmanSpreadEstimator(q=1e-5, r=0.01)
        for _ in range(100):
            k.update(50.0)
        mean, _ = k.update(50.0)
        assert abs(mean - 50.0) < 0.01

    def test_z_score_positive_for_above_mean(self):
        k = KalmanSpreadEstimator(q=1e-5, r=0.01)
        for _ in range(20):
            k.update(0.0)
        z = k.z_score(5.0, 2.0)
        assert z > 0

    def test_z_score_negative_for_below_mean(self):
        k = KalmanSpreadEstimator(q=1e-5, r=0.01)
        for _ in range(20):
            k.update(0.0)
        z = k.z_score(-5.0, 2.0)
        assert z < 0

    def test_z_score_safe_with_zero_std(self):
        k = KalmanSpreadEstimator()
        z = k.z_score(10.0, 0.0)
        assert np.isfinite(z)

    def test_reset(self):
        k = KalmanSpreadEstimator()
        k.update(100.0)
        k.update(200.0)
        k.reset()
        mean, _ = k.update(50.0)
        assert mean == 50.0


# ─────────────────────────── PairSignal Model ─────────────────────────────────


class TestPairSignalModel:
    def test_is_base_signal(self):
        ps = PairSignal(
            strategy="test", symbol="SPY",
            long_symbol="SPY", short_symbol="QQQ",
            z_score=2.0, action="ENTER",
        )
        assert isinstance(ps, BaseSignal)

    def test_default_action_is_hold(self):
        ps = PairSignal(strategy="test", symbol="SPY")
        assert ps.action == "HOLD"

    def test_features_accessible(self):
        fv = FeatureVector(z_score=1.5, adx=25.0)
        ps = PairSignal(
            strategy="test", symbol="SPY", features=fv,
            long_symbol="SPY", short_symbol="QQQ",
        )
        assert ps.features.z_score == 1.5
        assert ps.features.adx == 25.0


# ─────────────────────────── BottiPairStrategy ────────────────────────────────


def _make_pair_bars(
    n: int = 30,
    spread_mean: float = 50.0,
    spread_std: float = 5.0,
    seed: int = 42,
) -> list[tuple[Bar, Bar]]:
    """Erzeuge n synchrone SPY/QQQ Bar-Paare."""
    rng = np.random.default_rng(seed)
    pairs: list[tuple[Bar, Bar]] = []
    for i in range(n):
        ts = _et_dt(2025, 3, 12 + (i % 15), 16, 0)
        spy_close = 500.0 + rng.normal(0, 2)
        qqq_close = spy_close + spread_mean + rng.normal(0, spread_std)
        bar_spy = Bar("SPY", ts, spy_close - 1, spy_close + 1,
                      spy_close - 1, spy_close, 1_000_000)
        bar_qqq = Bar("QQQ", ts, qqq_close - 1, qqq_close + 1,
                      qqq_close - 1, qqq_close, 800_000)
        pairs.append((bar_spy, bar_qqq))
    return pairs


class TestBottiPairStrategy:
    def test_name(self, context):
        strat = BottiPairStrategy(
            {"symbol_a": "SPY", "symbol_b": "QQQ"}, context=context,
        )
        assert strat.name == "botti_pair"

    def test_symbols(self, context):
        strat = BottiPairStrategy(
            {"symbol_a": "SPY", "symbol_b": "QQQ"}, context=context,
        )
        assert strat.symbol_a == "SPY"
        assert strat.symbol_b == "QQQ"

    def test_hold_with_few_bars(self, context):
        strat = BottiPairStrategy(
            {"symbol_a": "SPY", "symbol_b": "QQQ", "pair_lookback": 20},
            context=context,
        )
        pairs = _make_pair_bars(n=2)
        snapshot = context.snapshot()
        for bar_a, bar_b in pairs:
            sig = strat._generate_pair_signal(bar_a, bar_b, snapshot)
            assert sig.action == "HOLD"

    def test_returns_pair_signal(self, context):
        strat = BottiPairStrategy(
            {"symbol_a": "SPY", "symbol_b": "QQQ", "pair_lookback": 5},
            context=context,
        )
        pairs = _make_pair_bars(n=10)
        snapshot = context.snapshot()
        for bar_a, bar_b in pairs:
            sig = strat._generate_pair_signal(bar_a, bar_b, snapshot)
            assert isinstance(sig, PairSignal)
            assert sig.action in ("ENTER", "EXIT", "HOLD")
            assert sig.strategy == "botti_pair"

    def test_enter_on_extreme_z(self, context):
        """Wenn Spread weit vom Mean → ENTER Signal."""
        strat = BottiPairStrategy(
            {
                "symbol_a": "SPY", "symbol_b": "QQQ",
                "pair_lookback": 5, "z_entry": 1.0, "z_exit": 0.3,
                "kalman_q": 1e-5, "kalman_r": 0.01,
            },
            context=context,
        )
        snapshot = context.snapshot()

        # Erst paar normale Bars für Kalman-Konvergenz
        for i in range(10):
            ts = _et_dt(2025, 3, 12, 10, i)
            bar_a = Bar("SPY", ts, 500, 501, 499, 500, 1_000_000)
            bar_b = Bar("QQQ", ts, 550, 551, 549, 550, 800_000)
            strat._generate_pair_signal(bar_a, bar_b, snapshot)

        # Extremer Spread: QQQ springt hoch → positive Z → ENTER
        ts = _et_dt(2025, 3, 12, 10, 15)
        bar_a = Bar("SPY", ts, 500, 501, 499, 500, 1_000_000)
        bar_b = Bar("QQQ", ts, 600, 601, 599, 600, 800_000)
        sig = strat._generate_pair_signal(bar_a, bar_b, snapshot)

        assert sig.action == "ENTER"
        assert sig.z_score > 0

    def test_exit_when_z_returns_to_mean(self, context):
        """Nach Konvergenz der Bars → EXIT bei kleinem z."""
        strat = BottiPairStrategy(
            {
                "symbol_a": "SPY", "symbol_b": "QQQ",
                "pair_lookback": 5, "z_entry": 2.0, "z_exit": 0.3,
            },
            context=context,
        )
        snapshot = context.snapshot()

        # Alle Bars mit konstantem Spread → z nahe 0 → EXIT
        for i in range(10):
            ts = _et_dt(2025, 3, 12, 10, i)
            bar_a = Bar("SPY", ts, 500, 501, 499, 500, 1_000_000)
            bar_b = Bar("QQQ", ts, 550, 551, 549, 550, 800_000)
            sig = strat._generate_pair_signal(bar_a, bar_b, snapshot)

        # Nach genug Bars bei konstantem Spread: EXIT
        assert sig.action == "EXIT" or sig.action == "HOLD"
        assert abs(sig.z_score) < 1.0

    def test_reset_clears_state(self, context):
        strat = BottiPairStrategy(
            {"symbol_a": "SPY", "symbol_b": "QQQ"}, context=context,
        )
        snapshot = context.snapshot()
        ts = _et_dt(2025, 3, 12, 10, 0)
        for i in range(5):
            bar_a = Bar("SPY", ts, 500, 501, 499, 500, 1_000_000)
            bar_b = Bar("QQQ", ts, 550, 551, 549, 550, 800_000)
            strat._generate_pair_signal(bar_a, bar_b, snapshot)
        strat.reset()
        assert len(strat._spread_window) == 0

    def test_registered(self):
        from strategy.registry import StrategyRegistry
        assert "botti_pair" in StrategyRegistry.available()

    def test_no_broker_imports(self):
        import inspect
        import strategy.botti_pair as mod
        source = inspect.getsource(mod)
        forbidden = ["alpaca", "ib_insync", "requests", "yfinance", "httpx"]
        for word in forbidden:
            assert f"import {word}" not in source, \
                f"Verbotener Import '{word}' in strategy/botti_pair.py"

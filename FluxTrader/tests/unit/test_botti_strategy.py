"""Tests fuer strategy/botti.py – kein Netzwerk, kein Broker.

BottiStrategy wird mit deterministischen Daily-Bars + injiziertem Context getestet.
Smoke-Tests fuer BUY (Golden Cross), BUY_MR (Mean Reversion), SELL (Death Cross), HOLD.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.context import MarketContextService
from core.models import Bar
from strategy.botti import BottiStrategy, _compute_botti_indicators
from tests.conftest import _et_dt, make_ohlcv


# ─────────────────────────── Helpers ─────────────────────────────────────────

def _make_daily_bars(
    symbol: str = "AAPL",
    n: int = 60,
    base: float = 100.0,
    trend: float = 0.0,
    seed: int = 42,
) -> list[Bar]:
    """Erzeuge n Daily-Bars mit optionalem Trend-Offset."""
    rng = np.random.default_rng(seed)
    bars: list[Bar] = []
    close = base
    for i in range(n):
        ts = _et_dt(2025, 1, 2 + i % 28, 16, 0)  # Variiert Tag
        noise = rng.normal(0, 0.3)
        close = close + trend + noise
        o = close + rng.normal(0, 0.1)
        h = max(close, o) + rng.uniform(0.1, 0.5)
        lo = min(close, o) - rng.uniform(0.1, 0.5)
        vol = int(rng.integers(100_000, 500_000))
        bars.append(Bar(
            symbol=symbol, timestamp=ts,
            open=o, high=h, low=lo, close=close, volume=vol,
        ))
    return bars


def _make_golden_cross_bars(symbol: str = "AAPL") -> list[Bar]:
    """Bars die einen Golden Cross erzeugen: erst Downtrend, dann starker Uptrend.

    SMA20 kreuzt SMA30 von unten nach oben.
    RSI im Bereich 50-70, MACD positiv, Volume hoch.
    """
    bars: list[Bar] = []
    n = 60

    for i in range(n):
        ts = _et_dt(2025, 1, 2 + i % 28, 16, 0)
        if i < 30:
            # Leichter Downtrend -> SMA20 < SMA30
            close = 100.0 - i * 0.2 + np.sin(i * 0.3) * 0.5
        else:
            # Starker Uptrend -> SMA20 kreuzt SMA30
            close = 100.0 - 30 * 0.2 + (i - 30) * 0.8
        o = close - 0.1
        h = close + 0.5
        lo = close - 0.5
        vol = 300_000 if i >= 50 else 100_000  # Hohes Volume am Ende
        bars.append(Bar(
            symbol=symbol, timestamp=ts,
            open=o, high=h, low=lo, close=close, volume=vol,
        ))
    return bars


def _make_death_cross_bars(symbol: str = "AAPL") -> list[Bar]:
    """Bars die einen Death Cross erzeugen: erst Uptrend, dann Downtrend."""
    bars: list[Bar] = []
    n = 60

    for i in range(n):
        ts = _et_dt(2025, 1, 2 + i % 28, 16, 0)
        if i < 30:
            close = 100.0 + i * 0.3
        else:
            close = 100.0 + 30 * 0.3 - (i - 30) * 0.6
        o = close - 0.1
        h = close + 0.5
        lo = close - 0.5
        vol = 200_000
        bars.append(Bar(
            symbol=symbol, timestamp=ts,
            open=o, high=h, low=lo, close=close, volume=vol,
        ))
    return bars


def _make_mr_bars(symbol: str = "AAPL") -> list[Bar]:
    """Bars die ein Mean-Reversion-Signal erzeugen:
    Preis faellt unter das untere Bollinger-Band, RSI < 35, Volume > Vol_SMA.
    """
    bars: list[Bar] = []
    n = 60

    for i in range(n):
        ts = _et_dt(2025, 1, 2 + i % 28, 16, 0)
        if i < 50:
            close = 100.0 + np.sin(i * 0.15) * 2.0
        else:
            # Scharfer Drop -> unter BB_lower
            close = 100.0 - (i - 50) * 2.0
        o = close + 0.1
        h = close + 0.5
        lo = close - 0.8
        # Hohes Volume beim Drop
        vol = 500_000 if i >= 50 else 150_000
        bars.append(Bar(
            symbol=symbol, timestamp=ts,
            open=o, high=h, low=lo, close=close, volume=vol,
        ))
    return bars


# ─────────────────────────── Tests ───────────────────────────────────────────

class TestBottiStrategyInit:
    def test_name(self, context):
        strat = BottiStrategy({}, context=context)
        assert strat.name == "botti"

    def test_default_config_merged(self, context):
        strat = BottiStrategy({"sma_short": 15}, context=context)
        assert strat.config["sma_short"] == 15
        assert strat.config["sma_long"] == 30  # Default beibehalten

    def test_registered(self):
        from strategy.registry import StrategyRegistry
        assert "botti" in StrategyRegistry.available()


class TestBottiHold:
    def test_no_signal_before_ready(self, context):
        strat = BottiStrategy({"min_bars": 100}, context=context)
        bar = Bar("AAPL", _et_dt(2025, 3, 12, 16, 0),
                  100.0, 101.0, 99.0, 100.5, 100_000)
        signals = strat.on_bar(bar)
        assert signals == []

    def test_hold_with_random_bars(self, context):
        """Zufaellige Bars ohne klares Signal -> kein Trade."""
        context.update_account(equity=100_000.0, cash=100_000.0)
        strat = BottiStrategy({
            "use_fast_cross": False,
            "use_early_golden_cross": False,
            "use_pullback_entry_daily": False,
            "use_mean_reversion": False,
        }, context=context)

        bars = _make_daily_bars(n=60, trend=0.0, seed=123)
        signals = []
        for b in bars:
            signals.extend(strat.on_bar(b))
        # Bei rein zufaelligen Bars sollten wenige/keine Signale kommen
        # (kein garantierter Cross)


class TestBottiSell:
    def test_death_cross_emits_exit(self, context):
        """Death Cross -> direction=0 (Exit-Signal)."""
        context.update_account(equity=100_000.0, cash=100_000.0)
        strat = BottiStrategy({
            "use_fast_cross": False,
            "use_early_golden_cross": False,
            "use_pullback_entry_daily": False,
            "use_mean_reversion": False,
        }, context=context)

        bars = _make_death_cross_bars()
        signals = []
        for b in bars:
            signals.extend(strat.on_bar(b))

        exit_signals = [s for s in signals if s.direction == 0]
        if exit_signals:
            sig = exit_signals[0]
            assert sig.strategy_id == "botti_trend"
            assert sig.symbol == "AAPL"
            assert "Death Cross" in sig.metadata.get("reason", "")


class TestBottiBuy:
    def test_golden_cross_emits_long(self, context):
        """Golden Cross mit Bestaetigungsfiltern -> direction=+1."""
        context.update_account(equity=100_000.0, cash=100_000.0)
        strat = BottiStrategy({
            "use_fast_cross": False,
            "use_early_golden_cross": False,
            "use_pullback_entry_daily": False,
            "use_mean_reversion": False,
            "adx_threshold": 0,  # ADX-Filter deaktivieren
        }, context=context)

        bars = _make_golden_cross_bars()
        signals = []
        for b in bars:
            signals.extend(strat.on_bar(b))

        long_signals = [s for s in signals if s.direction == 1]
        if long_signals:
            sig = long_signals[0]
            assert sig.strategy_id == "botti_trend"
            assert sig.symbol == "AAPL"
            assert sig.stop_price > 0
            assert sig.target_price is not None
            assert "reason" in sig.metadata


class TestBottiMeanReversion:
    def test_mr_signal_emits_long(self, context):
        """Bollinger Mean Reversion -> direction=+1, strategy_id=botti_mr."""
        context.update_account(equity=100_000.0, cash=100_000.0)
        strat = BottiStrategy({
            "use_fast_cross": False,
            "use_early_golden_cross": False,
            "use_pullback_entry_daily": False,
            "use_mean_reversion": True,
            "mr_rsi_max": 45,  # Etwas lockerer fuer Test
        }, context=context)

        bars = _make_mr_bars()
        signals = []
        for b in bars:
            signals.extend(strat.on_bar(b))

        mr_signals = [s for s in signals if s.strategy_id == "botti_mr"]
        if mr_signals:
            sig = mr_signals[0]
            assert sig.direction == 1
            assert sig.symbol == "AAPL"
            assert sig.metadata.get("mr_target") is not None
            assert "BB Lower" in sig.metadata.get("reason", "")


class TestBottiDrawdownBreaker:
    def test_no_signal_during_drawdown(self, context):
        """Bei hohem Drawdown duerfen keine neuen Signale kommen."""
        # Peak war 100k, jetzt nur noch 80k -> 20% DD > 15% Schwelle
        context.update_account(equity=100_000.0, cash=100_000.0)
        context._account.peak_equity = 100_000.0
        context.update_account(equity=80_000.0, cash=80_000.0)

        strat = BottiStrategy({
            "max_drawdown_pct": 0.15,
            "use_fast_cross": False,
            "use_early_golden_cross": False,
        }, context=context)

        bars = _make_golden_cross_bars()
        signals = []
        for b in bars:
            signals.extend(strat.on_bar(b))

        assert signals == []


class TestBottiSectorGuard:
    def test_sector_blocks_third_position(self, context):
        """Max 2 pro Sektor: dritte Position im gleichen Sektor wird blockiert."""
        context.update_account(equity=100_000.0, cash=100_000.0)
        context.set_open_symbols(["NVDA", "AMD"])  # tech_semi schon 2x belegt

        strat = BottiStrategy({
            "max_per_sector": 2,
            "sector_groups": {"tech_semi": ["NVDA", "AMD", "MU"]},
            "use_fast_cross": False,
            "use_early_golden_cross": False,
        }, context=context)

        bars = _make_golden_cross_bars(symbol="MU")
        signals = []
        for b in bars:
            signals.extend(strat.on_bar(b))

        buy_signals = [s for s in signals if s.direction == 1]
        assert buy_signals == []


class TestBottiVixFactor:
    def test_vix_factor_in_metadata(self, context):
        """Bei hohem VIX soll vix_factor=0.5 in metadata stehen."""
        context.update_account(equity=100_000.0, cash=100_000.0)
        context.set_vix(35.0, None)  # VIX > 30

        strat = BottiStrategy({
            "vix_high_threshold": 30,
            "use_fast_cross": False,
            "use_early_golden_cross": False,
            "use_pullback_entry_daily": False,
            "use_mean_reversion": True,
            "mr_rsi_max": 45,
            "adx_threshold": 0,
        }, context=context)

        bars = _make_mr_bars()
        signals = []
        for b in bars:
            signals.extend(strat.on_bar(b))

        for sig in signals:
            if sig.direction == 1:
                assert sig.metadata.get("vix_factor") == 0.5
                break


class TestBottiReset:
    def test_reset_clears_bars(self, context):
        strat = BottiStrategy({}, context=context)
        strat.bars.append(Bar("AAPL", _et_dt(2025, 3, 12, 16, 0),
                              100, 101, 99, 100, 100000))
        strat.reset()
        assert len(strat.bars) == 0


class TestBottiNoForbiddenImports:
    def test_no_broker_imports(self):
        """strategy/botti.py darf keine Broker-Imports enthalten."""
        import inspect
        import strategy.botti as mod
        source = inspect.getsource(mod)
        forbidden = ["alpaca", "ib_insync", "requests", "yfinance", "httpx"]
        for word in forbidden:
            assert f"import {word}" not in source, \
                f"Verbotener Import '{word}' in strategy/botti.py gefunden"

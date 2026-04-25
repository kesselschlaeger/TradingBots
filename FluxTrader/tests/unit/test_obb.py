"""Tests für strategy/obb.py – reserve_group und MIT-Correlation-Groups.

Kein Netzwerk, kein Broker.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core.context import MarketContextService, set_context_service
from core.models import Bar
from strategy.obb import OBBStrategy


def _daily_bar(symbol: str, close: float, high: float = None,
               low: float = None, day: int = 1) -> Bar:
    h = high if high is not None else close * 1.01
    l = low if low is not None else close * 0.99
    return Bar(
        symbol=symbol,
        timestamp=datetime(2026, 4, day, 16, 0, tzinfo=timezone.utc),
        open=close * 0.998, high=h, low=l, close=close, volume=500_000,
    )


@pytest.fixture
def ctx():
    c = MarketContextService(initial_capital=100_000.0)
    c.update_account(equity=100_000.0, cash=100_000.0, buying_power=400_000.0)
    set_context_service(c)
    return c


class TestOBBReserveGroup:
    def test_signal_has_reserve_group_key(self, ctx):
        # lookback_bars=3 → braucht lookback+2=5 Bars im Buffer (Fallback-Pfad)
        strat = OBBStrategy(
            config={
                "lookback_bars": 3,
                "allow_shorts": True,
                "position_size_pct": 0.10,
            },
            context=ctx,
        )
        # Baue 4 Warmup-Bars mit mäßigem High (max High ca. 100.5–103.5)
        for i in range(4):
            bar = _daily_bar("SPY", close=100.0 + i, high=100.0 + i + 0.5, day=i + 1)
            strat.on_bar(bar)

        # Breakout: Close > max(High der letzten 3 Bars) = max(101.5, 102.5, 103.5) = 103.5
        breakout_bar = _daily_bar("SPY", close=105.0, high=105.5, low=104.0, day=5)
        signals = strat.on_bar(breakout_bar)

        assert len(signals) == 1, f"Erwartete Signal, bekam: {signals}"
        sig = signals[0]
        assert "reserve_group" in sig.metadata

    def test_spy_qqq_same_correlation_group(self, ctx):
        """SPY und QQQ sind in 'equity_us_large' – Reserve-Group muss identisch sein."""
        from core.filters import correlation_group
        from strategy.obb import OBB_DEFAULT_PARAMS
        groups = OBB_DEFAULT_PARAMS["mit_correlation_groups"]
        assert correlation_group("SPY", groups) == correlation_group("QQQ", groups)

    def test_unrelated_symbol_has_empty_group(self, ctx):
        """Ein Symbol ohne Korrelationsgruppe bekommt leeren String zurück."""
        from core.filters import correlation_group
        from strategy.obb import OBB_DEFAULT_PARAMS
        groups = OBB_DEFAULT_PARAMS["mit_correlation_groups"]
        result = correlation_group("SOMETHINGWEIRD", groups)
        assert result == ""

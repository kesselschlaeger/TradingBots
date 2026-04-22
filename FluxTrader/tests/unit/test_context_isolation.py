"""Context isolation tests for runner/strategy usage."""
from __future__ import annotations

from datetime import datetime, timezone

from core.context import MarketContextService
from strategy.orb import ORBStrategy


def _minimal_orb_config() -> dict:
    return {
        "min_bars": 5,
        "use_gap_filter": False,
        "use_trend_filter": False,
        "use_mit_probabilistic_overlay": False,
        "use_time_decay_filter": False,
    }


def test_two_runners_context_isolation():
    """Two context instances must not overwrite each other."""
    ctx_a = MarketContextService(initial_capital=100_000)
    ctx_b = MarketContextService(initial_capital=50_000)

    ts_a = datetime(2026, 4, 22, 9, 31, 0, tzinfo=timezone.utc)
    ts_b = datetime(2026, 4, 22, 9, 31, 5, tzinfo=timezone.utc)

    ctx_a.set_now(ts_a)
    ctx_b.set_now(ts_b)

    assert ctx_a.snapshot().now == ts_a
    assert ctx_b.snapshot().now == ts_b
    assert ctx_a.snapshot().now != ts_b


def test_strategy_uses_injected_context():
    """Strategy instantiated with ctx_a reads ctx_a even if ctx_b changes."""
    ctx_a = MarketContextService(initial_capital=100_000)
    ctx_b = MarketContextService(initial_capital=100_000)

    ts_a = datetime(2026, 4, 22, 9, 31, 0, tzinfo=timezone.utc)
    ts_b = datetime(2026, 4, 22, 9, 31, 5, tzinfo=timezone.utc)

    ctx_a.set_now(ts_a)
    strategy = ORBStrategy(config=_minimal_orb_config(), context=ctx_a)

    ctx_b.set_now(ts_b)

    snap = strategy.context.snapshot()
    assert snap.now == ts_a


def test_context_fixture_still_works(context):
    """The existing context fixture path remains functional after refactor."""
    assert context is not None
    context.set_now(datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert context.snapshot().now is not None
    strategy = ORBStrategy(config=_minimal_orb_config())
    assert strategy.context is context

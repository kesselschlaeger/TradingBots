"""Cross-Strategy Status-Reporting: _record_status / set_status_sink API.

Schlanke Smoke-Tests für alle Strategien, die sicherstellen, dass der Sink
aus live/runner.py bzw. live/pair_runner.py ohne Fehler gebunden werden kann
und Aufrufe im Null-Object-Fall (ohne Sink) folgenlos bleiben.
"""
from __future__ import annotations

import pytest

from strategy.base import BaseStrategy, PairStrategy
from strategy.botti import BottiStrategy
from strategy.botti_pair import BottiPairStrategy
from strategy.ict_ob import IctOrderBlockStrategy
from strategy.obb import OBBStrategy
from strategy.orb import ORBStrategy


@pytest.mark.parametrize("cls", [
    ORBStrategy, OBBStrategy, BottiStrategy, IctOrderBlockStrategy,
])
def test_single_symbol_strategies_have_status_api(cls, context):
    strat = cls({}, context=context)
    assert isinstance(strat, BaseStrategy)
    # Null-Object-Pattern: Aufruf ohne Sink darf nicht werfen
    strat._record_status("AAPL", "WAIT_SETUP", "ohne Setup")
    # Sink setzen und prüfen, dass er gerufen wird
    captured: list[tuple[str, str, str]] = []
    strat.set_status_sink(
        lambda sym, code, reason: captured.append((sym, code, reason))
    )
    strat._record_status("AAPL", "TEST", "detail")
    assert captured == [("AAPL", "TEST", "detail")]


def test_pair_strategy_has_status_api(context):
    strat = BottiPairStrategy(
        {"symbol_a": "SPY", "symbol_b": "QQQ"}, context=context,
    )
    assert isinstance(strat, PairStrategy)
    assert strat.pair_key == "SPY/QQQ"
    strat._record_status("SPY/QQQ", "WAIT_WARMUP")  # darf nicht werfen
    captured: list[tuple[str, str, str]] = []
    strat.set_status_sink(
        lambda k, code, reason: captured.append((k, code, reason))
    )
    strat._record_status("SPY/QQQ", "SIGNAL", "ENTER")
    assert captured == [("SPY/QQQ", "SIGNAL", "ENTER")]


def test_sink_exception_is_swallowed(context):
    """Ein defekter Sink darf die Strategie nicht crashen."""
    strat = OBBStrategy({}, context=context)

    def boom(_sym, _code, _reason):
        raise RuntimeError("sink broken")

    strat.set_status_sink(boom)
    # Kein Fehler nach außen:
    strat._record_status("AAPL", "WAIT_BREAKOUT", "")


def test_pair_sink_exception_is_swallowed(context):
    strat = BottiPairStrategy(
        {"symbol_a": "SPY", "symbol_b": "QQQ"}, context=context,
    )

    def boom(_k, _code, _reason):
        raise RuntimeError("sink broken")

    strat.set_status_sink(boom)
    strat._record_status("SPY/QQQ", "WAIT_WARMUP", "")

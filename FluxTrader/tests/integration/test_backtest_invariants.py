"""Backtest-Invariant-Tests: Parameter-Sensitivit\u00e4t und Look-Ahead-Freiheit.

Diese Tests schlie\u00dfen die L\u00fccken, durch die der ORB-Trend-Filter-Bug
(April 2026) unentdeckt durchrutschen konnte:

1. **Parameter-Matrix**: `allow_shorts` und `use_trend_filter` m\u00fcssen unter
   geeigneten Daten nachweisbare Wirkung zeigen. Ein Default-Test, der beide
   Flags isoliert voneinander pr\u00fcft, verpasst Interaktionen (z.B. ein
   gebrochener Trend-Filter, der `allow_shorts` \u00fcberschattet).

2. **Look-Ahead-Property**: Zuk\u00fcnftige SPY-Bars d\u00fcrfen das Backtest-
   Ergebnis niemals beeinflussen. Wir injizieren vergiftete Future-Bars
   (extreme Werte nach Backtest-Ende) und verifizieren, dass Trades und
   Equity bit-identisch bleiben.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta, timezone
from typing import Tuple

import numpy as np
import pandas as pd
import pytest
import pytz

from backtest.engine import BacktestConfig, BarByBarEngine
from core.context import MarketContextService, reset_context_service
from core.models import Trade
from execution.paper_adapter import PaperAdapter
from strategy.orb import ORBStrategy

ET = pytz.timezone("America/New_York")


# ---------------------------------------------------------------------------
# Test-Daten-Helfer
# ---------------------------------------------------------------------------

def _trading_day_intraday(day: datetime, direction: str,
                          base: float = 100.0,
                          n_bars: int = 78) -> pd.DataFrame:
    """Ein Handelstag mit 5-Min-Bars 9:30-16:00 ET und klarer Richtung.

    - ``direction == "up"``:   Enges ORB (99.5-101), danach starker Anstieg auf ~108.
    - ``direction == "down"``: Enges ORB (99.5-101), danach starker Abverkauf auf ~92.
    - ``direction == "flat"``: Reines Rauschen um ``base`` (Warmup).
    """
    start = ET.localize(day.replace(hour=9, minute=30)).astimezone(timezone.utc)
    idx = pd.date_range(start, periods=n_bars, freq="5min", tz="UTC")

    # 6 ORB-Bars (9:30-9:55) mit enger Range
    orb_close = np.linspace(base - 0.5, base + 0.5, 6)

    rng = np.random.default_rng(7)
    if direction == "up":
        post = np.linspace(base + 2.0, base + 8.0, n_bars - 6)
        post = post + rng.normal(0.0, 0.1, n_bars - 6)
    elif direction == "down":
        post = np.linspace(base - 2.0, base - 8.0, n_bars - 6)
        post = post + rng.normal(0.0, 0.1, n_bars - 6)
    else:
        post = base + rng.normal(0.0, 0.15, n_bars - 6)

    close = np.concatenate([orb_close, post])
    return pd.DataFrame({
        "Open": close,
        "High": close + 0.3,
        "Low": close - 0.3,
        "Close": close,
        "Volume": np.full(n_bars, 500_000, dtype=np.int64),
    }, index=idx)


def _make_multi_day_data() -> pd.DataFrame:
    """Warmup-Tag (flat) + Up-Tag + Down-Tag = 3 x 78 5-Min-Bars."""
    frames = [
        _trading_day_intraday(datetime(2025, 3, 11), "flat"),
        _trading_day_intraday(datetime(2025, 3, 12), "up"),
        _trading_day_intraday(datetime(2025, 3, 13), "down"),
    ]
    return pd.concat(frames).sort_index()


def _bullish_spy_daily(days_before: int = 40,
                       last_day: datetime | None = None) -> pd.DataFrame:
    """Bullishe SPY-Daily-Historie, endet am Vortag des Backtest-Starts."""
    if last_day is None:
        last_day = datetime(2025, 3, 10)  # Tag vor Warmup
    start = pd.Timestamp(last_day, tz="UTC") - pd.Timedelta(days=days_before - 1)
    idx = pd.date_range(start, periods=days_before, freq="1D", tz="UTC")
    close = np.linspace(480.0, 520.0, days_before)
    return pd.DataFrame({
        "Open": close - 0.5,
        "High": close + 1.0,
        "Low": close - 1.0,
        "Close": close,
        "Volume": np.full(days_before, 10_000_000, dtype=np.int64),
    }, index=idx)


def _trade_signature(t: Trade) -> tuple:
    """Vergleichbare Trade-Signatur ohne UUID-order_id.

    ``timestamp`` wird im Backtest aus der Bar-Zeit gef\u00fcllt (siehe
    ``PaperAdapter.set_sim_clock``) und ist daher deterministisch.
    """
    return (t.symbol, t.side, float(t.qty), round(float(t.price), 6),
            round(float(t.pnl), 6), t.timestamp, t.reason, t.strategy_id)


def _count_short_closes(trades: list[Trade]) -> int:
    """Zahl der geschlossenen Shorts (COVER-Trades).

    Der ``PaperAdapter`` loggt nur Close-Trades:
    - LONG geschlossen -> side='SELL'
    - SHORT geschlossen -> side='COVER'
    Damit ist COVER der einzig verl\u00e4ssliche Nachweis f\u00fcr er\u00f6ffnete Shorts.
    """
    return sum(1 for t in trades if t.side == "COVER")


# ---------------------------------------------------------------------------
# Run-Harness
# ---------------------------------------------------------------------------

def _run_once(data: pd.DataFrame, spy_df: pd.DataFrame, *,
              allow_shorts: bool, use_trend_filter: bool):
    ctx = MarketContextService(initial_capital=100_000.0)
    ctx.update_account(equity=100_000.0, cash=100_000.0,
                       buying_power=400_000.0)
    broker = PaperAdapter(initial_cash=100_000.0, slippage_pct=0.0,
                          commission_pct=0.0)

    strat = ORBStrategy({
        "min_bars": 20,
        "allow_shorts": allow_shorts,
        "use_trend_filter": use_trend_filter,
        "use_gap_filter": False,
        "use_mit_probabilistic_overlay": False,
        "use_time_decay_filter": False,
        "volume_multiplier": 0.0,      # Volumen kein Filter
        "min_signal_strength": 0.0,    # kein Schwellwert
        "trend_ema_period": 20,
    }, context=ctx)

    cfg = BacktestConfig(initial_capital=100_000.0,
                         eod_close_time=time(15, 55))
    engine = BarByBarEngine(strat, broker, ctx, cfg)

    result = asyncio.get_event_loop().run_until_complete(
        engine.run(data={"AAPL": data}, spy_df=spy_df)
    )
    reset_context_service()
    return result


# ---------------------------------------------------------------------------
# Test 1: Parameter-Matrix
# ---------------------------------------------------------------------------

class TestParameterMatrix:
    """Jeder Config-Flag muss unter geeigneten Daten Wirkung zeigen.

    Mit bullisher SPY-Historie und einem bearishen Tag muss die Matrix
    ``(use_trend_filter, allow_shorts)`` vier verschiedene Verhaltensweisen
    zeigen:

    | trend | shorts | erwartet                                         |
    |-------|--------|--------------------------------------------------|
    | False | True   | Longs + Shorts fallen durch                      |
    | False | False  | nur Longs (Shorts weggefiltert durch allow_shorts)|
    | True  | True   | nur Longs (Shorts weggefiltert durch Trend=bullish)|
    | True  | False  | nur Longs                                        |
    """

    @pytest.fixture(scope="class")
    def data(self) -> pd.DataFrame:
        return _make_multi_day_data()

    @pytest.fixture(scope="class")
    def spy(self) -> pd.DataFrame:
        return _bullish_spy_daily()

    @pytest.fixture(scope="class")
    def results(self, data, spy) -> dict:
        grid = {}
        for uf in (False, True):
            for sh in (False, True):
                grid[(uf, sh)] = _run_once(
                    data, spy, allow_shorts=sh, use_trend_filter=uf,
                )
        return grid

    def test_baseline_produces_both_sides(self, results):
        """Ohne Filter m\u00fcssen Longs UND Shorts entstehen -- sonst ist der
        Test vakuum und k\u00f6nnte den eigentlichen Bug nicht auffangen."""
        r = results[(False, True)]
        sides = {t.side for t in r.trades}
        assert "SELL" in sides, (
            f"Erwartete Long-Close (SELL) in Baseline, sah: {sides}"
        )
        assert "COVER" in sides, (
            f"Baseline-Daten produzieren keine Short-Seite (COVER) -- "
            f"Test vakuum. Sides: {sides}"
        )

    def test_allow_shorts_false_removes_shorts(self, results):
        """Mit ``allow_shorts=False`` darf keine Short-Er\u00f6ffnung stattfinden."""
        r = results[(False, False)]
        assert _count_short_closes(r.trades) == 0, (
            f"allow_shorts=False, aber Shorts ge\u00f6ffnet (COVER): "
            f"{[t for t in r.trades if t.side == 'COVER']}"
        )

    def test_allow_shorts_changes_result(self, results):
        """Toggeln von ``allow_shorts`` muss den Ergebnis-Set ver\u00e4ndern."""
        sigs_true = [_trade_signature(t) for t in results[(False, True)].trades]
        sigs_false = [_trade_signature(t) for t in results[(False, False)].trades]
        assert sigs_true != sigs_false, (
            "allow_shorts True/False liefern identische Trades -- Flag wirkungslos."
        )

    def test_use_trend_filter_changes_result(self, results):
        """Bei bullisher SPY muss ``use_trend_filter=True`` Shorts blockieren
        und damit ein anderes Trade-Set erzeugen als ohne Filter."""
        sigs_off = [_trade_signature(t) for t in results[(False, True)].trades]
        sigs_on = [_trade_signature(t) for t in results[(True, True)].trades]
        assert sigs_off != sigs_on, (
            "use_trend_filter True/False liefern identische Trades -- "
            "Filter wirkungslos."
        )

    def test_trend_filter_blocks_shorts_on_bullish_spy(self, results):
        """Bullishe SPY + ``use_trend_filter=True`` -> keine Short-Er\u00f6ffnungen."""
        r = results[(True, True)]
        assert _count_short_closes(r.trades) == 0, (
            f"Bullishe SPY sollte Shorts blockieren, aber: "
            f"{[t for t in r.trades if t.side == 'COVER']}"
        )


# ---------------------------------------------------------------------------
# Test 2: Look-Ahead-Property
# ---------------------------------------------------------------------------

class TestLookAheadInvariance:
    """Zuk\u00fcnftige SPY-Bars d\u00fcrfen das Backtest-Ergebnis nie beeinflussen.

    Wir laufen denselben Backtest zweimal:

    - *clean*: SPY-Historie endet vor dem Backtest.
    - *poisoned*: zus\u00e4tzlich werden extrem bearishe SPY-Bars NACH dem letzten
      Backtest-Timestamp angeh\u00e4ngt. Ein fehlerhafter Filter, der in die
      Zukunft schaut, w\u00fcrde Long-Signale blockieren und das Ergebnis ver\u00e4ndern.

    Die Invariante ``clean == poisoned`` schl\u00e4gt jede Form von Look-Ahead
    im SPY-Pfad.
    """

    @pytest.fixture(scope="class")
    def data(self) -> pd.DataFrame:
        return _make_multi_day_data()

    @pytest.fixture(scope="class")
    def spy_clean(self) -> pd.DataFrame:
        return _bullish_spy_daily()

    @pytest.fixture(scope="class")
    def spy_poisoned(self, spy_clean) -> pd.DataFrame:
        """Bullische SPY + 20 extrem bearishe Future-Bars nach Backtest-Ende."""
        last = spy_clean.index[-1]
        future_start = last + pd.Timedelta(days=30)
        future_idx = pd.date_range(future_start, periods=20, freq="1D",
                                   tz="UTC")
        # Extrem niedrige Closes -> EMA-Cross nach unten -> trend["bearish"]
        poison_close = np.linspace(10.0, 1.0, 20)
        poison = pd.DataFrame({
            "Open": poison_close,
            "High": poison_close + 0.1,
            "Low": poison_close - 0.1,
            "Close": poison_close,
            "Volume": np.full(20, 1_000, dtype=np.int64),
        }, index=future_idx)
        return pd.concat([spy_clean, poison])

    def test_future_spy_bars_are_invisible(self, data, spy_clean,
                                           spy_poisoned):
        """Trades und Equity m\u00fcssen identisch sein, egal ob SPY Future-Bars
        enth\u00e4lt oder nicht."""
        r_clean = _run_once(
            data, spy_clean, allow_shorts=True, use_trend_filter=True,
        )
        r_poisoned = _run_once(
            data, spy_poisoned, allow_shorts=True, use_trend_filter=True,
        )

        sigs_clean = [_trade_signature(t) for t in r_clean.trades]
        sigs_poisoned = [_trade_signature(t) for t in r_poisoned.trades]

        assert sigs_clean == sigs_poisoned, (
            "Future-SPY-Bars haben Trade-Sequenz ver\u00e4ndert -- Look-Ahead-Leak. "
            f"clean={len(sigs_clean)}, poisoned={len(sigs_poisoned)}"
        )
        assert r_clean.final_equity == pytest.approx(
            r_poisoned.final_equity, rel=1e-9,
        ), "Future-SPY-Bars haben Equity ver\u00e4ndert -- Look-Ahead-Leak."

    def test_future_spy_bars_do_not_shift_trend_cache(self, data, spy_clean):
        """Doppellauf mit identischer SPY-Historie muss auch identisch sein.

        Absicherung: Zeigt, dass der Test stabil deterministisch ist (kein
        False Positive durch Zufall in _run_once). L\u00e4uft der vorherige Test
        fehl, zeigt dieser hier, ob das Setup selbst unzuverl\u00e4ssig ist."""
        r_a = _run_once(
            data, spy_clean, allow_shorts=True, use_trend_filter=True,
        )
        r_b = _run_once(
            data, spy_clean, allow_shorts=True, use_trend_filter=True,
        )

        sigs_a = [_trade_signature(t) for t in r_a.trades]
        sigs_b = [_trade_signature(t) for t in r_b.trades]
        assert sigs_a == sigs_b
        assert r_a.final_equity == pytest.approx(r_b.final_equity, rel=1e-9)

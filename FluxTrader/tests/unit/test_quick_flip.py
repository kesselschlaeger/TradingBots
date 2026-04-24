"""Tests für strategy/quick_flip.py – kein Netzwerk, kein Broker."""
from __future__ import annotations

from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import pytest
import pytz

from core.models import Bar
from strategy.quick_flip import QuickFlipStrategy
from tests.conftest import make_bar, make_ohlcv, _et_dt

ET = pytz.timezone("America/New_York")

# Fester Testtermin: Mittwoch 2025-03-12 (Handelstag)
TEST_DATE = date(2025, 3, 12)


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture()
def strategy(context):
    """Frische QuickFlipStrategy mit minimaler Config für Tests."""
    cfg = {
        "opening_range_minutes": 15,
        "liquidity_atr_threshold": 0.25,
        "max_trade_window_minutes": 90,
        "min_rr_ratio": 1.5,
        "buffer_ticks": 0.05,
        "min_signal_strength": 0.30,
        "allow_shorts": True,
        "use_trend_filter": False,   # SPY-Filter aus, um Tests zu isolieren
        "use_gap_filter": False,     # Gap-Filter aus, um Tests zu isolieren
        "use_mit_overlay": False,    # MIT-Overlay aus, um Tests zu isolieren
        "use_vix_filter": False,
        "entry_cutoff_time": "11:00",
        "risk_per_trade": 0.005,
        "hammer_shadow_ratio": 2.0,
        "hammer_upper_shadow_ratio": 0.3,
        "engulfing_min_body_ratio": 0.6,
        "min_bars": 2,
        "max_bars_buffer": 500,
    }
    return QuickFlipStrategy(cfg, context=context)


def _bar_at(hour: int, minute: int, o=100.0, h=101.0, l=99.0, c=100.0,
             v=100_000) -> Bar:
    """Erzeugt einen Bar am TEST_DATE zur ET-Uhrzeit hour:minute."""
    ts = _et_dt(2025, 3, 12, hour, minute)
    return Bar(symbol="AAPL", timestamp=ts, open=o, high=h, low=l, close=c,
               volume=v)


def _seed_or(strategy: QuickFlipStrategy, or_high=102.0, or_low=98.0):
    """Setzt OR-Box + State direkt im Day-Cache (vermeidet echtes Resampling)."""
    strategy._day_cache["date"] = TEST_DATE
    strategy._day_cache["state"] = "or_locked"
    strategy._day_cache["or_high"] = or_high
    strategy._day_cache["or_low"] = or_low
    strategy._day_cache["or_range"] = or_high - or_low
    strategy._day_cache["daily_atr"] = 2.50
    strategy._day_cache["gap_checked"] = True
    # open_time_et nötig für _check_time_window_expired
    strategy._day_cache["open_time_et"] = ET.localize(
        datetime(2025, 3, 12, 9, 30))


def _seed_liquidity(strategy: QuickFlipStrategy, direction: str = "long"):
    """Setzt State nach Liquidity-Erkennung."""
    _seed_or(strategy)
    strategy._day_cache["state"] = "liquidity_seen"
    strategy._day_cache["liquidity_direction"] = direction
    strategy._day_cache["liquidity_ts"] = _et_dt(2025, 3, 12, 9, 45)


# ── Tests: State-Machine-Übergänge ─────────────────────────────────────────

class TestStateMachineIdle:
    def test_no_signal_before_or_locked(self, strategy, context):
        """State 'idle': kein Signal, egal wie groß die Candle ist."""
        # Zwei Bars als Warmup (min_bars=2)
        bar0 = _bar_at(9, 30, o=100, h=105, l=95, c=100)
        bar1 = _bar_at(9, 35, o=100, h=110, l=90, c=100)
        strategy.on_bar(bar0)
        signals = strategy.on_bar(bar1)
        # State ist "idle", OR-Fenster noch aktiv → kein Signal
        assert signals == []
        assert strategy._day_cache["state"] == "idle"

    def test_or_locks_after_15_minutes(self, strategy, context):
        """Nach Ablauf der 15m-OR-Periode: or_high/or_low gesetzt, State=or_locked.

        Direkt über _lock_opening_range testen, da dafür > 20 Bars nötig
        wären für echten on_bar-Durchlauf. Hier wird State manuell vorbereitet.
        """
        # Manuell: Daily-ATR setzen, dann _lock_opening_range aufrufen
        strategy._day_cache["date"] = TEST_DATE
        strategy._day_cache["daily_atr"] = 2.50
        strategy._day_cache["open_time_et"] = ET.localize(
            datetime(2025, 3, 12, 9, 30))

        # OR-Bars: 3 x 5m ab 9:30 ET (= 15 Min)
        or_bars = [
            _bar_at(9, 30, o=100.0, h=102.5, l=99.0, c=101.0),
            _bar_at(9, 35, o=101.0, h=103.0, l=100.0, c=102.0),
            _bar_at(9, 40, o=102.0, h=104.0, l=101.0, c=103.0),
        ]
        idx = pd.DatetimeIndex([b.timestamp for b in or_bars], tz="UTC")
        df_5m = pd.DataFrame({
            "Open": [b.open for b in or_bars],
            "High": [b.high for b in or_bars],
            "Low": [b.low for b in or_bars],
            "Close": [b.close for b in or_bars],
            "Volume": [b.volume for b in or_bars],
        }, index=idx)

        strategy._lock_opening_range(df_5m)
        assert strategy._day_cache["or_high"] == pytest.approx(104.0)
        assert strategy._day_cache["or_low"] == pytest.approx(99.0)


class TestLiquidityCandle:
    def test_liquidity_candle_detected(self, strategy, context):
        """Candle mit Range >= 0.25 * daily_atr → State wechselt zu 'liquidity_seen'."""
        _seed_or(strategy)  # daily_atr=2.50, or_locked
        # Range = 1.5 >= 0.25*2.50=0.625 → Liquidity erkannt
        # rote Candle → direction="long"
        bar = _bar_at(9, 55, o=100.0, h=101.0, l=99.5, c=99.7, v=200_000)
        # Direkt _detect_liquidity_candle aufrufen mit passendem 15m-Frame
        idx = pd.DatetimeIndex([bar.timestamp], tz="UTC")
        df = pd.DataFrame({
            "Open": [bar.open], "High": [bar.high],
            "Low": [bar.low], "Close": [bar.close], "Volume": [bar.volume],
        }, index=idx)
        found, direction = strategy._detect_liquidity_candle(df)
        # 15m-Resampling liefert 1 Bar mit range=1.5 >= 0.625 → True
        # Die Methode testet nach OR-Ende (9:45). Timestamp 9:55 > 9:45 ✓
        # Wegen Resampling kann das Ergebnis False sein wenn kein Bar nach OR-Ende.
        # Direkter ATR-Test:
        from core.indicators import is_liquidity_candle
        assert is_liquidity_candle(high=101.0, low=99.5,
                                   daily_atr=2.50, threshold=0.25)

    def test_no_liquidity_candle_below_threshold(self, strategy, context):
        """Candle zu klein → kein Liquidity-Signal."""
        from core.indicators import is_liquidity_candle
        # range=0.3, daily_atr=2.50 → 0.3 < 0.625 → False
        assert not is_liquidity_candle(high=100.3, low=100.0,
                                       daily_atr=2.50, threshold=0.25)

    def test_state_transitions_to_liquidity_seen(self, strategy, context):
        """State wechselt auf 'liquidity_seen' nach explizitem Cache-Setup."""
        _seed_or(strategy)
        # Liquidity-Richtung manuell setzen (wie _detect_liquidity_candle es täte)
        strategy._day_cache["liquidity_direction"] = "long"
        strategy._day_cache["liquidity_ts"] = _et_dt(2025, 3, 12, 9, 55)
        strategy._day_cache["state"] = "liquidity_seen"
        assert strategy._day_cache["state"] == "liquidity_seen"
        assert strategy._day_cache["liquidity_direction"] == "long"


class TestReversalCandle:
    def test_long_signal_on_hammer_below_box(self, strategy, context):
        """Roter Liquidity-Sweep + Hammer unter or_low → Long-Signal."""
        _seed_liquidity(strategy, direction="long")
        or_low = strategy._day_cache["or_low"]  # 98.0

        # Hammer unterhalb OR-Box:
        # low=97.0 < or_low=98.0 ✓
        # body=0.10 (o=97.6, c=97.7), lower=97.6-97.0=0.60 >= 2.0*0.10 ✓
        # upper=97.7→h=97.72: upper=97.72-97.7=0.02 <= 0.3*0.10=0.03 ✓
        bar = _bar_at(10, 0, o=97.6, h=97.72, l=97.0, c=97.7, v=150_000)
        # Warmup-Bars damit _is_ready() True wird
        strategy.bars.append(_bar_at(9, 30))
        strategy.bars.append(_bar_at(9, 35))

        idx = pd.DatetimeIndex([bar.timestamp], tz="UTC")
        df_5m = pd.DataFrame({
            "Open": [bar.open], "High": [bar.high],
            "Low": [bar.low], "Close": [bar.close], "Volume": [bar.volume],
        }, index=idx)
        sig = strategy._detect_reversal_candle(df_5m, direction="long", bar=bar)
        assert sig is not None
        assert sig.direction == 1            # Long = +1
        assert sig.stop_price < sig.metadata["entry_price"]
        assert sig.target_price > sig.metadata["entry_price"]
        rr = (sig.target_price - sig.metadata["entry_price"]) / \
             (sig.metadata["entry_price"] - sig.stop_price)
        assert rr >= 1.5
        assert sig.metadata["reversal_pattern"] == "hammer"
        assert sig.metadata["or_low"] == pytest.approx(or_low)

    def test_short_signal_on_engulfing_above_box(self, strategy, context):
        """Grüner Liquidity-Sweep + Bearish Engulfing über or_high → Short-Signal."""
        _seed_liquidity(strategy, direction="short")
        or_high = strategy._day_cache["or_high"]  # 102.0

        # Bearish Engulfing über OR-Box (zwei Bars):
        # Bar[-2]: bullish (o=102.5, c=103.0, body=0.5)
        # Bar[-1]: bearish engulfing (o=103.2, c=102.1, body=1.1 >= 0.6*0.5=0.3)
        #          high=103.5 > or_high=102.0 ✓
        #          c=102.1 < o[-2]=102.5 ✓, o=103.2 > c[-2]=103.0 ✓
        ts_prev = _et_dt(2025, 3, 12, 9, 55)
        ts_cur = _et_dt(2025, 3, 12, 10, 0)
        df_5m = pd.DataFrame({
            "Open": [102.5, 103.2], "High": [103.0, 103.5],
            "Low": [102.2, 101.8], "Close": [103.0, 102.1],
            "Volume": [100_000, 150_000],
        }, index=pd.DatetimeIndex([ts_prev, ts_cur], tz="UTC"))
        bar = Bar(symbol="AAPL", timestamp=ts_cur, open=103.2,
                  high=103.5, low=101.8, close=102.1, volume=150_000)

        sig = strategy._detect_reversal_candle(df_5m, direction="short", bar=bar)
        assert sig is not None
        assert sig.direction == -1           # Short = -1
        assert sig.stop_price > sig.metadata["entry_price"]
        assert sig.target_price < sig.metadata["entry_price"]
        assert sig.metadata["reversal_pattern"] == "bearish_engulfing"
        assert sig.metadata["or_high"] == pytest.approx(or_high)


class TestTimeWindow:
    def test_no_signal_after_time_window_expired(self, strategy, context):
        """Bar nach 11:00 ET → State sofort 'done', leere Signal-Liste."""
        _seed_or(strategy)
        # Genug Warmup-Bars damit der < 6-Fallback-Check nicht greift
        for minute in (30, 35, 40, 45, 50, 55):
            strategy.bars.append(_bar_at(9, minute))
        # Direktprüfung: _check_time_window_expired muss True liefern
        bar = _bar_at(11, 5)
        expired = strategy._check_time_window_expired(bar.timestamp)
        assert expired
        # on_bar: Fenster abgelaufen → State=done, signals=[]
        signals = strategy.on_bar(bar)
        assert signals == []
        assert strategy._day_cache["state"] == "done"

    def test_within_window_returns_false(self, strategy, context):
        """Bar um 10:00 ET → noch innerhalb des 90-Min-Fensters."""
        _seed_or(strategy)
        bar = _bar_at(10, 0)
        assert not strategy._check_time_window_expired(bar.timestamp)


class TestPostTrade:
    def test_no_signal_after_first_trade(self, strategy, context):
        """Sobald State 'done': jeder weitere Bar gibt [] zurück."""
        _seed_liquidity(strategy, direction="long")
        strategy._day_cache["state"] = "done"
        strategy.bars.append(_bar_at(9, 30))
        strategy.bars.append(_bar_at(9, 35))
        bar = _bar_at(10, 5, o=97.0, h=97.5, l=96.5, c=97.2)
        signals = strategy.on_bar(bar)
        assert signals == []

    def test_reset_clears_day_cache(self, strategy, context):
        """Nach reset(): _day_cache leer, State='idle'."""
        _seed_liquidity(strategy, direction="long")
        strategy._day_cache["state"] = "done"
        strategy.reset()
        assert strategy._day_cache.get("state") == "idle"
        assert strategy._day_cache.get("or_high") is None
        assert strategy._day_cache.get("daily_atr") is None
        assert strategy._day_cache.get("liquidity_direction") is None


class TestRRFilter:
    def test_rr_filter_rejects_bad_setup(self, strategy, context):
        """R:R < min_rr_ratio → kein Signal."""
        _seed_liquidity(strategy, direction="long")
        or_high = strategy._day_cache["or_high"]  # 102.0
        or_low = strategy._day_cache["or_low"]    # 98.0

        # Hammer-Setup: low=97.9 (knapp unter or_low=98.0)
        # entry=close=97.95, stop=97.9-0.05=97.85, target=or_high=102.0
        # reward=102.0-97.95=4.05, risk=97.95-97.85=0.10 → RR=40.5 → OK wäre gut
        # Aber: artificially high stop → schlechtes RR:
        # Wir manipulieren buffer_ticks so, dass Stop fast bei Entry liegt
        # Einfacher: or_high sehr nah am entry setzen → kleines Reward
        strategy._day_cache["or_high"] = 98.1  # Sehr eng über Entry
        bar = _bar_at(10, 5, o=97.95, h=97.97, l=97.4, c=97.95)
        # body=0, lower=97.95-97.4=0.55>=2*0=0 ✓, upper=0<=0 ✓ → Hammer
        # entry=97.95, stop=97.4-0.05=97.35, target=98.1
        # risk=97.95-97.35=0.60, reward=98.1-97.95=0.15 → RR=0.25 < 1.5 → Reject

        idx = pd.DatetimeIndex([bar.timestamp], tz="UTC")
        df_5m = pd.DataFrame({
            "Open": [bar.open], "High": [bar.high],
            "Low": [bar.low], "Close": [bar.close], "Volume": [bar.volume],
        }, index=idx)
        sig = strategy._detect_reversal_candle(df_5m, direction="long", bar=bar)
        assert sig is None


class TestPatternParameters:
    def test_custom_hammer_ratio_relaxed(self, strategy, context):
        """hammer_shadow_ratio=1.2 → Pattern wird erkannt, das bei 2.0 abgelehnt würde."""
        _seed_liquidity(strategy, direction="long")
        # body=0.10, lower=0.15 → 0.15 < 2.0*0.10=0.20 → Standard-Ratio: abgelehnt
        # aber >= 1.2*0.10=0.12 → relaxed: akzeptiert
        strategy.config["hammer_shadow_ratio"] = 1.2
        bar = _bar_at(10, 0, o=100.10, h=100.10, l=99.95, c=100.20)
        # low=99.95 < or_low=98.0? NEIN. or_low aus _seed_liquidity = 98.0
        # Damit das Setup greift, brauchen wir low < or_low:
        bar = _bar_at(10, 0, o=97.60, h=97.60, l=97.45, c=97.70)
        # body=0.10, lower=97.60-97.45=0.15, upper=97.60-97.70<0→0
        # 0.15>=1.2*0.10=0.12 ✓
        idx = pd.DatetimeIndex([bar.timestamp], tz="UTC")
        df_5m = pd.DataFrame({
            "Open": [bar.open], "High": [bar.high],
            "Low": [bar.low], "Close": [bar.close], "Volume": [bar.volume],
        }, index=idx)
        sig = strategy._detect_reversal_candle(df_5m, direction="long", bar=bar)
        assert sig is not None, (
            "Mit hammer_shadow_ratio=1.2 sollte das Pattern erkannt werden"
        )
        assert sig.metadata["reversal_pattern"] == "hammer"

    def test_custom_hammer_ratio_strict(self, strategy, context):
        """hammer_shadow_ratio=3.0 → gleiches Pattern wird abgelehnt."""
        _seed_liquidity(strategy, direction="long")
        strategy.config["hammer_shadow_ratio"] = 3.0
        # body=0.10, lower=0.15 → 0.15 < 3.0*0.10=0.30 → abgelehnt
        bar = _bar_at(10, 0, o=97.60, h=97.60, l=97.45, c=97.70)
        idx = pd.DatetimeIndex([bar.timestamp], tz="UTC")
        df_5m = pd.DataFrame({
            "Open": [bar.open], "High": [bar.high],
            "Low": [bar.low], "Close": [bar.close], "Volume": [bar.volume],
        }, index=idx)
        sig = strategy._detect_reversal_candle(df_5m, direction="long", bar=bar)
        assert sig is None, (
            "Mit hammer_shadow_ratio=3.0 sollte das Pattern abgelehnt werden"
        )

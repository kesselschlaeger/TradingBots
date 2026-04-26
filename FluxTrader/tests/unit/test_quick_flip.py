"""Tests für strategy/quick_flip.py – kein Netzwerk, kein Broker.

Spec: One-Candle-Scalping / Quick-Flip.
  OR-Eröffnungskerze = Manipulationskerze; Direction aus or_color;
  Entry/Stop pattern-spezifisch (STOP-Order); max_trade_window_minutes allein.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest
import pytz

from core.models import Bar
from strategy.quick_flip import QuickFlipStrategy
from tests.conftest import _et_dt, make_bar

ET = pytz.timezone("America/New_York")

# Fester Testtermin: Mittwoch 2025-03-12 (Handelstag)
TEST_DATE = date(2025, 3, 12)


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture()
def strategy(context):
    """Frische QuickFlipStrategy mit isolierter Config für Tests."""
    cfg = {
        "opening_range_minutes": 15,
        "liquidity_atr_threshold": 0.25,
        "max_trade_window_minutes": 90,
        "min_rr_ratio": 1.5,
        "buffer_ticks": 0.05,
        "min_signal_strength": 0.30,
        "allow_shorts": True,
        "use_trend_filter": False,
        "use_gap_filter": False,
        "use_mit_overlay": False,
        "use_vix_filter": False,
        "use_extended_target": False,
        "risk_per_trade": 0.005,
        "hammer_shadow_ratio": 2.0,
        "hammer_upper_shadow_ratio": 0.3,
        "engulfing_min_body_ratio": 0.6,
        "min_bars": 2,
        "max_bars_buffer": 500,
    }
    return QuickFlipStrategy(cfg, context=context)


def _bar_at(hour: int, minute: int, o=100.0, h=101.0, l=99.0, c=100.0,
            v=100_000, symbol="AAPL") -> Bar:
    """Erzeugt einen Bar am TEST_DATE zur ET-Uhrzeit hour:minute."""
    ts = _et_dt(2025, 3, 12, hour, minute)
    return Bar(symbol=symbol, timestamp=ts, open=o, high=h, low=l, close=c, volume=v)


def _bar_minutes_after_open(minutes: int, **kwargs) -> Bar:
    """Bar exakt N Minuten nach 9:30 ET am TEST_DATE."""
    total = 9 * 60 + 30 + minutes
    h, m = divmod(total, 60)
    return _bar_at(h, m, **kwargs)


def _make_df(bars: list[Bar]) -> pd.DataFrame:
    idx = pd.DatetimeIndex([b.timestamp for b in bars], tz="UTC")
    return pd.DataFrame({
        "Open": [b.open for b in bars],
        "High": [b.high for b in bars],
        "Low": [b.low for b in bars],
        "Close": [b.close for b in bars],
        "Volume": [b.volume for b in bars],
    }, index=idx)


def _seed_or_complete(strategy: QuickFlipStrategy,
                      or_high=102.0, or_low=98.0,
                      or_open=100.5, or_close=99.5,
                      daily_atr=4.0) -> None:
    """Setzt Cache auf 'or_complete' für ATR-Validierungs-Tests."""
    strategy._day_cache.update({
        "date": TEST_DATE,
        "state": "or_complete",
        "or_high": or_high,
        "or_low": or_low,
        "or_range": or_high - or_low,
        "or_open": or_open,
        "or_close": or_close,
        "or_color": "green" if or_close > or_open else "red",
        "daily_atr": daily_atr,
        "gap_checked": True,
        "open_time_et": ET.localize(datetime(2025, 3, 12, 9, 30)),
    })


def _seed_armed(strategy: QuickFlipStrategy,
                direction="long",
                or_high=102.0, or_low=98.0,
                daily_atr=4.0) -> None:
    """Setzt Cache auf 'armed' für Reversal-Tests."""
    or_open = 100.5 if direction == "long" else 99.5
    or_close = 99.5 if direction == "long" else 100.5
    strategy._day_cache.update({
        "date": TEST_DATE,
        "state": "armed",
        "or_high": or_high,
        "or_low": or_low,
        "or_range": or_high - or_low,
        "or_open": or_open,
        "or_close": or_close,
        "or_color": "red" if direction == "long" else "green",
        "direction": direction,
        "daily_atr": daily_atr,
        "gap_checked": True,
        "open_time_et": ET.localize(datetime(2025, 3, 12, 9, 30)),
    })


# ── Test 1: OR_TOO_SMALL ────────────────────────────────────────────────────

class TestOrTooSmall:
    def test_or_too_small_blocks_trade(self, strategy, context):
        """OR-Range < 0.25 * daily_atr → state='done', kein Signal."""
        # daily_atr=4.0, threshold=0.25 → Mindestwert=1.0
        # or_range=0.8 < 1.0 → OR_TOO_SMALL
        _seed_or_complete(strategy,
                          or_high=100.8, or_low=100.0,
                          or_open=100.5, or_close=100.3,
                          daily_atr=4.0)

        # Mindestens 6 Bars im Buffer für den Fallback-Pfad
        for m in (30, 35, 40, 45, 50, 55):
            strategy.bars.append(_bar_at(9, m))

        bar = _bar_at(9, 45)
        signals = strategy._generate_signals(bar)
        assert signals == []
        assert strategy._day_cache["state"] == "done"

    def test_or_exactly_at_threshold_passes(self, strategy, context):
        """OR-Range == 0.25 * daily_atr → KEIN Block (>= Bedingung)."""
        # daily_atr=4.0, or_range=1.0 → genau 0.25*4.0 → OK
        _seed_or_complete(strategy,
                          or_high=101.0, or_low=100.0,
                          or_open=100.5, or_close=100.3,
                          daily_atr=4.0)
        for m in (30, 35, 40, 45, 50, 55):
            strategy.bars.append(_bar_at(9, m))

        bar = _bar_at(9, 45)
        strategy._generate_signals(bar)
        # Kein done durch OR_TOO_SMALL; state sollte nun 'armed' sein
        assert strategy._day_cache["state"] == "armed"


# ── Test 2: Rote OR + Hammer → Long ─────────────────────────────────────────

class TestRedOpenLongHammer:
    def test_signal_direction_long(self, strategy, context):
        """Rote OR + Hammer unter or_low → Signal direction=+1."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)

        # Hammer: o=97.6, h=97.62, l=97.0, c=97.7
        # body = |97.7-97.6| = 0.10
        # lower = 97.6-97.0 = 0.60 >= 2.0*0.10=0.20 ✓
        # upper = 97.62-97.7 < 0 → 0 <= 0.3*0.10=0.03 ✓
        # bar.low=97.0 < or_low=98.0 → Sweep ✓
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=97.6, h=97.62, l=97.0, c=97.7, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "long", bar)
        assert sig is not None
        assert sig.direction == 1

    def test_entry_at_hammer_high_plus_buffer(self, strategy, context):
        """Long + hammer: entry = high(reversal_bar) + buffer_ticks."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=97.6, h=97.62, l=97.0, c=97.7, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "long", bar)
        assert sig is not None
        assert sig.metadata["entry_price"] == pytest.approx(97.62 + 0.05)

    def test_stop_at_hammer_low_minus_buffer(self, strategy, context):
        """Long + hammer: stop = low(reversal_bar) - buffer_ticks."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=97.6, h=97.62, l=97.0, c=97.7, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "long", bar)
        assert sig is not None
        assert sig.stop_price == pytest.approx(97.0 - 0.05)

    def test_target_is_or_high(self, strategy, context):
        """Long-Setup: Primärziel = or_high."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=97.6, h=97.62, l=97.0, c=97.7, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "long", bar)
        assert sig is not None
        assert sig.target_price == pytest.approx(102.0)

    def test_metadata_has_or_color_and_pattern(self, strategy, context):
        """Signal-Metadaten: or_color='red', reversal_pattern='hammer'."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=97.6, h=97.62, l=97.0, c=97.7, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "long", bar)
        assert sig is not None
        assert sig.metadata["or_color"] == "red"
        assert sig.metadata["reversal_pattern"] == "hammer"
        assert sig.metadata["rr_ratio"] >= 1.5

    def test_no_signal_without_sweep(self, strategy, context):
        """Kein Sweep unter or_low → kein Signal (bar.low >= or_low)."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        # bar.low=98.5 >= or_low=98.0 → kein Sweep
        bar = Bar("AAPL", ts, o=98.6, h=98.65, l=98.5, c=98.7, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "long", bar)
        assert sig is None


# ── Test 3: Grüne OR + Inverted Hammer → Short ──────────────────────────────

class TestGreenOpenShortInvHammer:
    def test_signal_direction_short(self, strategy, context):
        """Grüne OR + Inverted Hammer über or_high → Signal direction=-1."""
        _seed_armed(strategy, direction="short", or_high=102.0, or_low=98.0)

        # inv_hammer: o=102.2, h=103.0, l=102.22, c=102.3
        # body = |102.3-102.2| = 0.10
        # upper = 103.0-102.3 = 0.70 >= 2.0*0.10=0.20 ✓
        # lower = 102.2-102.22 < 0 → 0 <= 0.3*0.10=0.03 ✓
        # bar.high=103.0 > or_high=102.0 → Sweep ✓
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=102.2, h=103.0, l=102.22, c=102.3, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "short", bar)
        assert sig is not None
        assert sig.direction == -1

    def test_entry_at_inv_hammer_low_minus_buffer(self, strategy, context):
        """Short + inverted_hammer: entry = low(reversal_bar) - buffer_ticks."""
        _seed_armed(strategy, direction="short", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=102.2, h=103.0, l=102.22, c=102.3, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "short", bar)
        assert sig is not None
        assert sig.metadata["entry_price"] == pytest.approx(102.22 - 0.05)

    def test_stop_at_inv_hammer_high_plus_buffer(self, strategy, context):
        """Short + inverted_hammer: stop = high(reversal_bar) + buffer_ticks."""
        _seed_armed(strategy, direction="short", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=102.2, h=103.0, l=102.22, c=102.3, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "short", bar)
        assert sig is not None
        assert sig.stop_price == pytest.approx(103.0 + 0.05)

    def test_target_is_or_low(self, strategy, context):
        """Short-Setup: Primärziel = or_low."""
        _seed_armed(strategy, direction="short", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=102.2, h=103.0, l=102.22, c=102.3, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "short", bar)
        assert sig is not None
        assert sig.target_price == pytest.approx(98.0)


# ── Test 4: Bullish Engulfing Entry am Hoch der Vorgängerkerze ───────────────

class TestBullishEngulfingEntryUsesPrevHigh:
    def test_entry_uses_prev_bar_high(self, strategy, context):
        """Long + bullish_engulfing: entry = high(prev_bar) + buffer, NICHT high(current)."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)

        # prev (bearish): o=98.5, h=99.0, l=97.5, c=98.0  → prev_high=99.0
        # cur  (bullish engulfing): o=97.8, h=99.5, l=97.3, c=98.8
        #   body_cur = |98.8-97.8| = 1.0
        #   body_prev = |98.0-98.5| = 0.5
        #   1.0 >= 0.6*0.5=0.3 ✓ | c=98.8 > po=98.5 ✓ | o=97.8 < pc=98.0 ✓
        # bar.low=97.3 < or_low=98.0 → Sweep ✓
        ts_prev = _et_dt(2025, 3, 12, 9, 55)
        ts_cur = _et_dt(2025, 3, 12, 10, 0)
        prev = Bar("AAPL", ts_prev, o=98.5, h=99.0, l=97.5, c=98.0, v=100_000)
        cur = Bar("AAPL", ts_cur, o=97.8, h=99.5, l=97.3, c=98.8, v=150_000)
        df = _make_df([prev, cur])

        sig = strategy._detect_reversal_and_build_signal(df, "long", cur)
        assert sig is not None
        assert sig.metadata["reversal_pattern"] == "bullish_engulfing"
        # Entry muss prev_bar.high + buffer = 99.0 + 0.05 sein
        assert sig.metadata["entry_price"] == pytest.approx(99.0 + 0.05)
        # NICHT cur_bar.high + buffer = 99.5 + 0.05 = 99.55
        assert sig.metadata["entry_price"] != pytest.approx(99.5 + 0.05)

    def test_stop_uses_engulfing_bar_low(self, strategy, context):
        """Long + bullish_engulfing: stop = low(engulfing_bar) - buffer."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        ts_prev = _et_dt(2025, 3, 12, 9, 55)
        ts_cur = _et_dt(2025, 3, 12, 10, 0)
        prev = Bar("AAPL", ts_prev, o=98.5, h=99.0, l=97.5, c=98.0, v=100_000)
        cur = Bar("AAPL", ts_cur, o=97.8, h=99.5, l=97.3, c=98.8, v=150_000)
        df = _make_df([prev, cur])

        sig = strategy._detect_reversal_and_build_signal(df, "long", cur)
        assert sig is not None
        assert sig.stop_price == pytest.approx(97.3 - 0.05)

    def test_bearish_engulfing_entry_uses_prev_low(self, strategy, context):
        """Short + bearish_engulfing: entry = low(prev_bar) - buffer."""
        _seed_armed(strategy, direction="short", or_high=102.0, or_low=98.0)

        # prev (bullish): o=102.5, h=103.0, l=102.2, c=103.0 → prev_low=102.2
        # cur  (bearish engulfing): o=103.2, h=103.5, l=101.8, c=102.1
        #   body_cur = |102.1-103.2| = 1.1
        #   body_prev = |103.0-102.5| = 0.5
        #   1.1 >= 0.6*0.5=0.3 ✓ | c=102.1 < po=102.5 ✓ | o=103.2 > pc=103.0 ✓
        # bar.high=103.5 > or_high=102.0 → Sweep ✓
        ts_prev = _et_dt(2025, 3, 12, 9, 55)
        ts_cur = _et_dt(2025, 3, 12, 10, 0)
        prev = Bar("AAPL", ts_prev, o=102.5, h=103.0, l=102.2, c=103.0, v=100_000)
        cur = Bar("AAPL", ts_cur, o=103.2, h=103.5, l=101.8, c=102.1, v=150_000)
        df = _make_df([prev, cur])

        sig = strategy._detect_reversal_and_build_signal(df, "short", cur)
        assert sig is not None
        assert sig.metadata["reversal_pattern"] == "bearish_engulfing"
        # Entry = prev_bar.low - buffer = 102.2 - 0.05 = 102.15
        assert sig.metadata["entry_price"] == pytest.approx(102.2 - 0.05)


# ── Test 5: Window Expired → kein Signal ────────────────────────────────────

class TestWindowExpiredNoSignal:
    def test_bar_at_91min_blocked(self, strategy, context):
        """Bar 91 Min nach Open = 11:01 ET → Fenster abgelaufen, kein Signal."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        for m in (30, 35, 40, 45, 50, 55):
            strategy.bars.append(_bar_at(9, m))

        bar = _bar_minutes_after_open(91, l=97.0, h=97.62, o=97.6, c=97.7)
        signals = strategy._generate_signals(bar)
        assert signals == []
        assert strategy._day_cache["state"] == "done"

    def test_reversal_at_91min_produces_no_signal(self, strategy, context):
        """Auch ein perfektes Hammer-Pattern nach 91 Min liefert [] zurück."""
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        for m in (30, 35, 40, 45, 50, 55):
            strategy.bars.append(_bar_at(9, m))

        bar = _bar_minutes_after_open(91, o=97.6, h=97.62, l=97.0, c=97.7)
        assert strategy._generate_signals(bar) == []

    def test_check_time_window_expired_at_91min(self, strategy, context):
        """_check_time_window_expired() gibt True bei 91 Min."""
        strategy._day_cache["open_time_et"] = ET.localize(
            datetime(2025, 3, 12, 9, 30))
        bar = _bar_minutes_after_open(91)
        assert strategy._check_time_window_expired(bar.timestamp) is True

    def test_check_time_window_not_expired_at_90min(self, strategy, context):
        """_check_time_window_expired() gibt False bei genau 90 Min (inklusiv)."""
        strategy._day_cache["open_time_et"] = ET.localize(
            datetime(2025, 3, 12, 9, 30))
        bar = _bar_minutes_after_open(90)
        assert strategy._check_time_window_expired(bar.timestamp) is False


# ── Test 6: allow_shorts=False mit grüner OR ─────────────────────────────────

class TestAllowShortsFalse:
    def test_green_or_no_short_state_done(self, strategy, context):
        """Grüne OR bei allow_shorts=False → state='done', kein Signal."""
        strategy.config["allow_shorts"] = False
        # Grüne OR: or_close > or_open
        _seed_or_complete(strategy,
                          or_high=102.0, or_low=98.0,
                          or_open=99.5, or_close=100.5,  # grün
                          daily_atr=4.0)
        for m in (30, 35, 40, 45, 50, 55):
            strategy.bars.append(_bar_at(9, m))

        bar = _bar_at(9, 45)
        signals = strategy._generate_signals(bar)
        assert signals == []
        assert strategy._day_cache["state"] == "done"

    def test_red_or_long_still_works_with_shorts_disabled(self, strategy, context):
        """Rote OR bei allow_shorts=False → Long-Setup bleibt aktiv."""
        strategy.config["allow_shorts"] = False
        # Rote OR: or_close < or_open
        _seed_or_complete(strategy,
                          or_high=102.0, or_low=98.0,
                          or_open=100.5, or_close=99.5,  # rot
                          daily_atr=4.0)
        for m in (30, 35, 40, 45, 50, 55):
            strategy.bars.append(_bar_at(9, m))

        bar = _bar_at(9, 45)
        strategy._generate_signals(bar)
        assert strategy._day_cache["state"] == "armed"
        assert strategy._day_cache["direction"] == "long"


# ── Test 7: Extended Target Flag ─────────────────────────────────────────────

class TestExtendedTargetFlag:
    def test_extended_target_used_when_rr_sufficient(self, strategy, context):
        """use_extended_target=True + R:R >= 2.5 → Ziel über or_high hinaus."""
        strategy.config["use_extended_target"] = True
        strategy.config["extended_target_min_rr"] = 2.5
        # or_high=102, or_low=98, or_range=4
        # Extended target (Long) = 102 + 4 = 106
        # Hammer: entry=97.62+0.05=97.67, stop=97.0-0.05=96.95
        # risk=97.67-96.95=0.72, reward_ext=106-97.67=8.33 → rr_ext=11.6 >= 2.5 ✓
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=97.6, h=97.62, l=97.0, c=97.7, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "long", bar)
        assert sig is not None
        # Extended: target = or_high + or_range = 102 + 4 = 106
        assert sig.target_price == pytest.approx(106.0)
        assert sig.target_price > 102.0  # über or_high

    def test_no_extended_target_when_flag_false(self, strategy, context):
        """use_extended_target=False → Ziel bleibt bei or_high."""
        strategy.config["use_extended_target"] = False
        _seed_armed(strategy, direction="long", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=97.6, h=97.62, l=97.0, c=97.7, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "long", bar)
        assert sig is not None
        assert sig.target_price == pytest.approx(102.0)

    def test_short_extended_target_below_or_low(self, strategy, context):
        """Short + extended: target = or_low - or_range."""
        strategy.config["use_extended_target"] = True
        strategy.config["extended_target_min_rr"] = 2.5
        # or_high=102, or_low=98, or_range=4
        # Extended Short = 98 - 4 = 94
        _seed_armed(strategy, direction="short", or_high=102.0, or_low=98.0)
        ts = _et_dt(2025, 3, 12, 10, 0)
        bar = Bar("AAPL", ts, o=102.2, h=103.0, l=102.22, c=102.3, v=150_000)
        df = _make_df([bar])

        sig = strategy._detect_reversal_and_build_signal(df, "short", bar)
        assert sig is not None
        assert sig.target_price == pytest.approx(94.0)


# ── Tests 8–10: Zeitfenster-Randwerte (entry_cutoff entfernt) ────────────────

class TestTimeWindowBoundaries:
    def test_signal_at_85min_passes(self, strategy, context):
        """Bar bei 85 Min nach Open → innerhalb Fenster, kein Block."""
        strategy._day_cache["open_time_et"] = ET.localize(
            datetime(2025, 3, 12, 9, 30))
        bar = _bar_minutes_after_open(85)
        assert strategy._check_time_window_expired(bar.timestamp) is False

    def test_signal_at_91min_blocked(self, strategy, context):
        """Bar bei 91 Min nach Open → außerhalb 90-Min-Fenster, geblockt."""
        strategy._day_cache["open_time_et"] = ET.localize(
            datetime(2025, 3, 12, 9, 30))
        bar = _bar_minutes_after_open(91)
        assert strategy._check_time_window_expired(bar.timestamp) is True

    def test_signal_at_75min_passes(self, strategy, context):
        """Bar bei 75 Min nach Open (10:45 ET) → kein Block.

        Vorher: entry_cutoff_time=10:45 hätte dies geblockt.
        Jetzt: einziger Filter ist max_trade_window_minutes=90 → passiert.
        """
        strategy._day_cache["open_time_et"] = ET.localize(
            datetime(2025, 3, 12, 9, 30))
        bar = _bar_minutes_after_open(75)
        # 9:30 + 75min = 10:45 ET – MUSS passieren (kein entry_cutoff mehr)
        assert strategy._check_time_window_expired(bar.timestamp) is False

    def test_no_entry_cutoff_param_in_config(self, strategy, context):
        """entry_cutoff_time darf nicht in QUICK_FLIP_DEFAULT_PARAMS stehen."""
        from strategy.quick_flip import QUICK_FLIP_DEFAULT_PARAMS
        assert "entry_cutoff_time" not in QUICK_FLIP_DEFAULT_PARAMS


# ── Test 11: ATR-Resampling überspringt Wochenend-Bins ──────────────────────

class TestAtrSkipsWeekendBars:
    def test_atr_with_3_weeks_returns_value(self, strategy, context):
        """3 Wochen Bars (15 Handelstage + Wochenenden) → ATR berechenbar."""
        from strategy.quick_flip import QuickFlipStrategy as QF

        # Montag 2025-02-17 als Startpunkt (3 Wochen = 15 Handelstage)
        start_et = ET.localize(datetime(2025, 2, 17, 9, 30))
        bars: list[Bar] = []
        for day_offset in range(21):  # 3 Kalenderwochen
            day = start_et + timedelta(days=day_offset)
            if day.weekday() >= 5:   # Samstag/Sonntag überspringen
                continue
            ts = day.astimezone(timezone.utc)
            bars.append(Bar("AAPL", ts,
                            o=100.0, h=102.0, l=98.0, c=100.0, v=100_000))

        assert len(bars) == 15, f"Erwartet 15 Handelstage, erhalten: {len(bars)}"

        from strategy.quick_flip import _bars_to_df
        df = _bars_to_df(bars)
        atr_val = strategy._compute_daily_atr(df)
        assert atr_val is not None, "ATR sollte mit 14+ Vortagen berechenbar sein"
        assert atr_val == pytest.approx(4.0, abs=0.5)

    def test_atr_groupby_not_phantom_weekend_bins(self, strategy, context):
        """groupby(normalize()) erzeugt KEINE Phantom-Zeilen für Wochenenden.

        Mit resample('1D') auf UTC-Index entstehen ggf. 21 Bins (inkl. leere
        Wochenend-Bins). groupby(normalize()) erzeugt exakt 15 Bins (Handelstage).
        """
        from core.filters import to_et
        from strategy.quick_flip import _bars_to_df

        start_et = ET.localize(datetime(2025, 2, 17, 9, 30))
        bars: list[Bar] = []
        for day_offset in range(21):
            day = start_et + timedelta(days=day_offset)
            if day.weekday() >= 5:
                continue
            ts = day.astimezone(timezone.utc)
            bars.append(Bar("AAPL", ts,
                            o=100.0, h=102.0, l=98.0, c=100.0, v=100_000))

        df = _bars_to_df(bars)
        idx_et = to_et(df.index)
        df_et = df.copy()
        df_et.index = idx_et
        daily = df_et.groupby(idx_et.normalize()).agg({
            "Open": "first", "High": "max",
            "Low": "min", "Close": "last", "Volume": "sum",
        })
        # genau 15 Trading-Day-Bins, keine Wochenend-Phantome
        assert len(daily) == 15

    def test_atr_insufficient_days_returns_none(self, strategy, context):
        """Weniger als 14 vollständige Vortage → ATR = None."""
        from strategy.quick_flip import _bars_to_df

        start_et = ET.localize(datetime(2025, 3, 10, 9, 30))
        bars: list[Bar] = []
        for day_offset in range(10):   # nur 10 Handelstage
            day = start_et + timedelta(days=day_offset)
            if day.weekday() >= 5:
                continue
            ts = day.astimezone(timezone.utc)
            bars.append(Bar("AAPL", ts,
                            o=100.0, h=102.0, l=98.0, c=100.0, v=100_000))

        df = _bars_to_df(bars)
        atr_val = strategy._compute_daily_atr(df)
        assert atr_val is None


# ── Weitere Unit-Tests ────────────────────────────────────────────────────────

class TestStateMachineReset:
    def test_reset_clears_to_idle(self, strategy, context):
        """Nach reset(): _day_cache leer, state='idle'."""
        _seed_armed(strategy, direction="long")
        strategy._day_cache["state"] = "done"
        strategy.reset()
        assert strategy._day_cache.get("state") == "idle"
        assert strategy._day_cache.get("or_high") is None
        assert strategy._day_cache.get("direction") is None

    def test_done_state_is_terminal(self, strategy, context):
        """State 'done': jeder weitere Bar gibt [] zurück."""
        _seed_armed(strategy, direction="long")
        strategy._day_cache["state"] = "done"
        for m in (30, 35, 40, 45, 50, 55):
            strategy.bars.append(_bar_at(9, m))
        bar = _bar_at(10, 5, o=97.0, h=97.62, l=97.0, c=97.7)
        assert strategy._generate_signals(bar) == []

    def test_fresh_day_cache_has_required_keys(self, strategy, context):
        """_fresh_day_cache() enthält alle Pflicht-Keys."""
        cache = QuickFlipStrategy._fresh_day_cache()
        required = ["date", "state", "open_time_et", "or_high", "or_low",
                    "or_range", "or_open", "or_close", "or_color",
                    "direction", "daily_atr", "gap_checked"]
        for key in required:
            assert key in cache, f"Key '{key}' fehlt in _fresh_day_cache()"
        assert cache["state"] == "idle"


class TestOrLock:
    def test_lock_opening_range_sets_or_color(self, strategy, context):
        """_lock_opening_range setzt or_open, or_close, or_color korrekt."""
        strategy._day_cache["date"] = TEST_DATE

        # Rote OR: erstes Open > letztes Close
        or_bars = [
            _bar_at(9, 30, o=101.0, h=103.0, l=100.0, c=102.0),  # 9:30
            _bar_at(9, 35, o=102.0, h=102.5, l=100.5, c=101.0),  # 9:35
            _bar_at(9, 40, o=101.0, h=101.5, l=99.5, c=100.0),   # 9:40
        ]
        df = _make_df(or_bars)
        strategy._lock_opening_range(df)

        assert strategy._day_cache["or_open"] == pytest.approx(101.0)
        assert strategy._day_cache["or_close"] == pytest.approx(100.0)
        assert strategy._day_cache["or_color"] == "red"
        assert strategy._day_cache["or_high"] == pytest.approx(103.0)
        assert strategy._day_cache["or_low"] == pytest.approx(99.5)

    def test_lock_opening_range_green_or(self, strategy, context):
        """Grüne OR erkannt wenn or_close > or_open."""
        strategy._day_cache["date"] = TEST_DATE
        or_bars = [
            _bar_at(9, 30, o=100.0, h=103.0, l=99.0, c=101.0),
            _bar_at(9, 35, o=101.0, h=103.5, l=100.5, c=102.5),
            _bar_at(9, 40, o=102.5, h=104.0, l=101.5, c=103.0),
        ]
        df = _make_df(or_bars)
        strategy._lock_opening_range(df)
        assert strategy._day_cache["or_color"] == "green"


class TestRRFilter:
    def test_rr_below_minimum_returns_none(self, strategy, context):
        """R:R < min_rr_ratio=1.5 → kein Signal."""
        _seed_armed(strategy, direction="long", or_high=98.1, or_low=98.0)
        # Hammer mit entry≈97.67, stop≈96.95, target=or_high=98.1
        # risk=0.72, reward=0.43 → rr=0.60 < 1.5
        ts = _et_dt(2025, 3, 12, 10, 5)
        bar = Bar("AAPL", ts, o=97.6, h=97.62, l=97.0, c=97.7, v=150_000)
        df = _make_df([bar])
        sig = strategy._detect_reversal_and_build_signal(df, "long", bar)
        assert sig is None

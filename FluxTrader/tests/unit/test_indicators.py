"""Tests für core/indicators.py – kein Netzwerk."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.indicators import (
    atr,
    compute_indicator_frame,
    detect_reversal_pattern,
    ema,
    is_liquidity_candle,
    opening_range_levels,
    orb_volume_ratio,
    rolling_high_low,
    volume_time_of_day_ma,
    vwap,
)
from tests.conftest import make_ohlcv, _et_dt


class TestATR:
    def test_length(self, ohlcv_5m):
        result = atr(ohlcv_5m, period=14)
        assert len(result) == len(ohlcv_5m)
        assert result.iloc[:13].isna().all()
        assert result.iloc[13:].notna().all()

    def test_positive(self, ohlcv_5m):
        result = atr(ohlcv_5m, period=14).dropna()
        assert (result > 0).all()


class TestEMA:
    def test_length_preserved(self, ohlcv_5m):
        result = ema(ohlcv_5m["Close"], 20)
        assert len(result) == len(ohlcv_5m)

    def test_last_is_float(self, ohlcv_5m):
        result = ema(ohlcv_5m["Close"], 20)
        assert isinstance(float(result.iloc[-1]), float)


class TestVWAP:
    def test_shape(self, ohlcv_5m):
        result = vwap(ohlcv_5m)
        assert len(result) == len(ohlcv_5m)

    def test_within_range(self, ohlcv_5m):
        result = vwap(ohlcv_5m).dropna()
        assert (result >= ohlcv_5m["Low"].min()).all()
        assert (result <= ohlcv_5m["High"].max()).all()


class TestVolumeTimeOfDayMA:
    def test_length(self, ohlcv_5m):
        result = volume_time_of_day_ma(ohlcv_5m, lookback=10)
        assert len(result) == len(ohlcv_5m)

    def test_positive(self, ohlcv_5m):
        result = volume_time_of_day_ma(ohlcv_5m, lookback=10).dropna()
        assert (result > 0).all()


class TestComputeIndicatorFrame:
    def test_columns_added(self, ohlcv_5m):
        df = compute_indicator_frame(ohlcv_5m, atr_period=14, volume_lookback=10)
        assert "ATR" in df.columns
        assert "Volume_MA" in df.columns
        assert "Volume_Ratio" in df.columns


class TestRollingHighLow:
    def test_exclude_current(self, ohlcv_daily):
        hi, lo = rolling_high_low(ohlcv_daily, 50, exclude_current=True)
        # Die letzten 50 Werte sollten vorhanden sein (50 Bars + 1 shift)
        assert hi.iloc[-1] > 0
        assert lo.iloc[-1] > 0
        # Hi/Lo basieren auf shift(1) -> kein Zukunfts-Leak
        assert hi.iloc[-1] <= ohlcv_daily["High"].iloc[:-1].max()

    def test_include_current(self, ohlcv_daily):
        hi, lo = rolling_high_low(ohlcv_daily, 50, exclude_current=False)
        last_hi = hi.iloc[-1]
        window = ohlcv_daily["High"].iloc[-50:]
        assert abs(last_hi - window.max()) < 1e-9


class TestOpeningRangeLevels:
    def test_valid_orb(self):
        start = _et_dt(2025, 3, 12, 9, 30)
        df = make_ohlcv(12, base=100.0, start=start, seed=1)
        h, l, r = opening_range_levels(df, orb_minutes=30)
        assert h > 0
        assert l > 0
        assert r > 0
        assert r == pytest.approx(h - l)

    def test_empty_df(self):
        empty = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
        assert opening_range_levels(empty) == (0.0, 0.0, 0.0)


class TestOrbVolumeRatio:
    def test_returns_float(self, ohlcv_5m):
        result = orb_volume_ratio(ohlcv_5m, orb_minutes=30)
        assert isinstance(result, float)
        assert result > 0

    def test_empty(self):
        empty = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
        assert orb_volume_ratio(empty) == 1.0


# ─────────────────────────── is_liquidity_candle ────────────────────────────

def _make_single_bar(o, h, l, c):
    """Erzeugt einen Einzel-Bar als OHLCV-DataFrame (kein Timestamp nötig)."""
    return pd.DataFrame({"Open": [o], "High": [h], "Low": [l], "Close": [c], "Volume": [1000]})


class TestIsLiquidityCandle:
    def test_above_threshold(self):
        # range=1.0, atr=2.0, threshold=0.25 → 1.0 >= 0.5 → True
        assert is_liquidity_candle(high=101.0, low=100.0, daily_atr=2.0, threshold=0.25)

    def test_below_threshold(self):
        # range=0.4, atr=2.0, threshold=0.25 → 0.4 < 0.5 → False
        assert not is_liquidity_candle(high=100.4, low=100.0, daily_atr=2.0, threshold=0.25)

    def test_exact_threshold(self):
        # range=0.5, atr=2.0, threshold=0.25 → 0.5 >= 0.5 → True (Grenzwert)
        assert is_liquidity_candle(high=100.5, low=100.0, daily_atr=2.0, threshold=0.25)

    def test_zero_atr(self):
        assert not is_liquidity_candle(high=101.0, low=100.0, daily_atr=0.0)

    def test_zero_range(self):
        assert not is_liquidity_candle(high=100.0, low=100.0, daily_atr=2.0)


# ─────────────────────────── detect_reversal_pattern ────────────────────────

def _make_ohlcv_row(o, h, l, c):
    """Einzelner Bar als DataFrame-Zeile."""
    return pd.DataFrame({"Open": [o], "High": [h], "Low": [l], "Close": [c]})


def _make_two_bars(o1, h1, l1, c1, o2, h2, l2, c2):
    """Zwei Bars (älterer zuerst)."""
    return pd.DataFrame({
        "Open": [o1, o2], "High": [h1, h2],
        "Low": [l1, l2], "Close": [c1, c2],
    })


class TestDetectReversalPattern:
    # Default-Parameter

    def test_hammer_detected(self):
        # Body=0.10, lower_shadow=0.50 (>= 2.0*0.10), upper_shadow=0.02 (<= 0.3*0.10=0.03)
        df = _make_ohlcv_row(o=100.10, h=100.12, l=99.60, c=100.20)
        # body=0.10, lower=100.10-99.60=0.50, upper=100.12-100.20=<0 → abs → actually:
        # min(o,c)=100.10, max(o,c)=100.20
        # lower=100.10-99.60=0.50, upper=100.20→h=100.12 → upper=h-max(o,c)=100.12-100.20<0 → 0
        # So upper=0, lower=0.5, body=0.10 → 0.5>=2*0.1 and 0<=0.3*0.1 ✓
        result = detect_reversal_pattern(df, direction="long")
        assert result == "hammer"

    def test_inverted_hammer_detected(self):
        # Für Short-Setup: langer oberer Schatten, kleiner unterer
        # o=100, c=100.10, h=100.60, l=99.98
        # body=0.10, upper=100.60-100.10=0.50 (>=2*0.10), lower=100.0-99.98=0.02 (<=0.03) ✓
        df = _make_ohlcv_row(o=100.00, h=100.60, l=99.98, c=100.10)
        result = detect_reversal_pattern(df, direction="short")
        assert result == "inverted_hammer"

    def test_bullish_engulfing_detected(self):
        # Vorgänger: bearish (o=101, c=100, body=1.0)
        # Letzter: bullish, engulfing (o=99.5, c=101.5 → body=2.0 >= 0.6*1.0, c>o[-2]=101, o<c[-2]=100)
        df = _make_two_bars(101.0, 101.5, 99.8, 100.0,
                            99.5, 101.8, 99.3, 101.5)
        result = detect_reversal_pattern(df, direction="long")
        assert result == "bullish_engulfing"

    def test_bearish_engulfing_detected(self):
        # Vorgänger: bullish (o=99, c=100, body=1.0)
        # Letzter: bearish, engulfing (o=100.5, c=98.5 → body=2.0 >= 0.6*1.0, c<o[-2]=99, o>c[-2]=100)
        df = _make_two_bars(99.0, 100.2, 98.8, 100.0,
                            100.5, 100.8, 98.3, 98.5)
        result = detect_reversal_pattern(df, direction="short")
        assert result == "bearish_engulfing"

    def test_no_pattern_returns_none(self):
        # Neutral Doji ohne Schatten – kein Muster
        df = _make_ohlcv_row(o=100.0, h=100.01, l=99.99, c=100.0)
        assert detect_reversal_pattern(df, direction="long") is None

    def test_insufficient_bars_for_engulfing_returns_none(self):
        # Nur 1 Bar → kein Engulfing möglich; Hammer-Check schlägt auch fehl
        df = _make_ohlcv_row(o=100.5, h=100.8, l=98.0, c=100.4)
        # body=0.1, lower=2.4, upper=0.4 → 0.4>0.03 → kein Hammer; kein Engulfing (1 Bar)
        assert detect_reversal_pattern(df, direction="long") is None

    def test_empty_df_returns_none(self):
        df = pd.DataFrame(columns=["Open", "High", "Low", "Close"])
        assert detect_reversal_pattern(df, direction="long") is None

    # Parameter-Durchreichung

    def test_hammer_relaxed_ratio_accepts(self):
        # body=0.10, lower=0.15 → bei ratio=1.2: 0.15>=1.2*0.10=0.12 → akzeptiert
        # upper=0 → 0<=0.3*0.10=0.03 ✓
        df = _make_ohlcv_row(o=100.10, h=100.10, l=99.95, c=100.20)
        # body=0.10, lower=min(100.10,100.20)-99.95=100.10-99.95=0.15, upper=100.10-100.20<0→0
        assert detect_reversal_pattern(df, direction="long",
                                       hammer_shadow_ratio=1.2) == "hammer"

    def test_hammer_strict_ratio_rejects(self):
        # Gleiches Pattern wie oben, aber ratio=3.5 → 0.15 < 3.5*0.10=0.35 → abgelehnt
        df = _make_ohlcv_row(o=100.10, h=100.10, l=99.95, c=100.20)
        assert detect_reversal_pattern(df, direction="long",
                                       hammer_shadow_ratio=3.5) is None

    def test_engulfing_min_body_ratio_enforced(self):
        # Vorgänger body=1.0, letzter body=0.5 → bei min_body_ratio=0.6: 0.5<0.6→None
        df = _make_two_bars(101.0, 101.5, 99.8, 100.0,
                            99.8, 101.0, 99.6, 100.3)
        # letzter body=0.5, vorgänger body=1.0 → ratio check: 0.5 < 0.6*1.0 → None
        result = detect_reversal_pattern(df, direction="long",
                                        engulfing_min_body_ratio=0.6)
        # Prüfe auch Hammer: body=0.5, lower=99.8-99.6=0.2, upper=101.0-100.3=0.7
        # 0.2 >= 2.0*0.5=1.0? Nein → kein Hammer
        assert result is None

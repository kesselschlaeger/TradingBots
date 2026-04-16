"""Tests für core/indicators.py – kein Netzwerk."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.indicators import (
    atr,
    compute_indicator_frame,
    ema,
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

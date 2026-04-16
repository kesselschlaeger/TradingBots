"""Tests für core/risk.py – kein Netzwerk."""
from __future__ import annotations

import pytest

from core.risk import (
    atr_stop,
    dynamic_kelly,
    expected_value_r,
    fixed_fraction_size,
    kelly_fraction,
    mit_estimate_win_probability,
    orb_range_stop,
    position_size,
    target_from_r,
)


class TestPositionSize:
    def test_basic(self):
        qty = position_size(equity=100_000, risk_pct=0.01,
                            entry=50.0, stop=49.0)
        # base = 1000 shares, aber Notional-Cap greift:
        # 1000 * 50$ = 50k = 50% > max_position_value_pct 25% -> 500 shares
        assert qty == 500

    def test_basic_no_notional_cap(self):
        # Kleiner Entry-Preis -> Notional-Cap greift nicht
        qty = position_size(equity=100_000, risk_pct=0.01,
                            entry=10.0, stop=9.0,
                            max_position_value_pct=1.0)
        # budget = 1000$, risk = 1$ -> 1000 shares, notional 1000*10=10k < 100k
        assert qty == 1000

    def test_notional_cap(self):
        qty = position_size(equity=100_000, risk_pct=0.01,
                            entry=50.0, stop=49.99,
                            max_position_value_pct=0.10)
        cap = int(100_000 * 0.10 / 50.0)
        assert qty <= cap

    def test_zero_risk(self):
        assert position_size(100_000, 0.01, 50.0, 50.0) == 0

    def test_zero_equity(self):
        assert position_size(0, 0.01, 50.0, 49.0) == 0


class TestFixedFractionSize:
    def test_basic(self):
        qty = fixed_fraction_size(100_000, 50.0, 0.10)
        assert qty == 200

    def test_zero_price(self):
        assert fixed_fraction_size(100_000, 0.0, 0.10) == 0


class TestOrbRangeStop:
    def test_long_stop_below_entry(self):
        stop = orb_range_stop("long", 101.0, 101.5, 99.5, 2.0, sl_r=1.0)
        assert stop < 101.0
        assert stop >= 99.5

    def test_short_stop_above_entry(self):
        stop = orb_range_stop("short", 99.0, 101.5, 99.5, 2.0, sl_r=1.0)
        assert stop > 99.0
        assert stop <= 101.5

    def test_fallback(self):
        stop = orb_range_stop("long", 100.0, 100.0, 99.0, 1.0, sl_r=2.0)
        assert stop < 100.0


class TestAtrStop:
    def test_long(self):
        assert atr_stop("long", 100.0, 2.0, 1.5) == pytest.approx(97.0)

    def test_short(self):
        assert atr_stop("short", 100.0, 2.0, 1.5) == pytest.approx(103.0)

    def test_zero_atr(self):
        assert atr_stop("long", 100.0, 0.0) == 100.0


class TestTargetFromR:
    def test_long(self):
        t = target_from_r("long", 100.0, 99.0, 2.0)
        assert t == pytest.approx(102.0)

    def test_short(self):
        t = target_from_r("short", 100.0, 101.0, 2.0)
        assert t == pytest.approx(98.0)


class TestKellyFraction:
    def test_profitable_edge(self):
        k = kelly_fraction(0.60, 2.0, 1.0)
        assert k > 0

    def test_no_edge(self):
        k = kelly_fraction(0.30, 1.0, 1.0)
        assert k == 0.0

    def test_zero_reward(self):
        assert kelly_fraction(0.60, 0.0) == 0.0


class TestDynamicKelly:
    def test_zero_dd(self):
        assert dynamic_kelly(0.5, 0.0, 0.15) == pytest.approx(0.5)

    def test_max_dd(self):
        assert dynamic_kelly(0.5, 0.15, 0.15) == 0.0

    def test_partial_dd(self):
        result = dynamic_kelly(0.5, 0.05, 0.15)
        assert 0.0 < result < 0.5


class TestExpectedValueR:
    def test_positive_ev(self):
        ev = expected_value_r(0.60, 2.0, 1.0)
        assert ev > 0

    def test_negative_ev(self):
        ev = expected_value_r(0.30, 1.0, 1.0)
        assert ev < 0


class TestMitEstimateWinProb:
    def test_range(self):
        p = mit_estimate_win_probability("BUY", 0.7, volume_ratio=2.0,
                                         volume_confirmed=True)
        assert 0.20 <= p <= 0.80

    def test_strong_signal_higher(self):
        p_strong = mit_estimate_win_probability("BUY", 0.9)
        p_weak = mit_estimate_win_probability("BUY", 0.2)
        assert p_strong > p_weak

    def test_calibration_offset(self):
        p_base = mit_estimate_win_probability("BUY", 0.5)
        p_offset = mit_estimate_win_probability("BUY", 0.5,
                                                calibration_offset=0.05)
        assert p_offset > p_base

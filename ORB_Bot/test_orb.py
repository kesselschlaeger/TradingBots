#!/usr/bin/env python3
"""
test_orb.py – Unit-Tests für die ORB-Strategie (Single Source of Truth).

Testet alle Kernfunktionen aus orb_strategy.py sowie die Exit-Logik
aus orb_backtest.py (_manage_long, _manage_short).

Ausführung:
    python -m pytest test_orb.py -v
    python -m pytest test_orb.py -v --tb=short   # kurze Fehlermeldungen
    python -m pytest test_orb.py -k "TestManage"  # nur Exit-Tests
"""

from __future__ import annotations

import copy
from datetime import datetime, time, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import pytz
import pytest

# ── Imports aus dem Projekt ──────────────────────────────────────────────────
from orb_strategy import (
    ET,
    ORB_DEFAULT_CONFIG,
    calculate_orb_levels,
    calculate_position_size,
    calculate_stop,
    calibrate_win_probability,
    check_breakout,
    check_entry_cutoff,
    check_gap_filter,
    check_trend_filter,
    compute_orb_signals,
    dynamic_kelly,
    get_vix_term_structure_regime,
    is_market_hours,
    is_orb_period,
    is_trading_day,
    mit_apply_overlay,
    mit_compute_ev_r,
    mit_estimate_win_probability,
    mit_kelly_fraction,
    time_decay_factor,
    to_et,
    to_et_time,
)
from orb_backtest import _manage_long, _manage_short


# ══════════════════════════ Helpers ══════════════════════════════════════════

def _cfg(**overrides) -> dict:
    """Frische Config mit optionalen Overrides."""
    c = copy.deepcopy(ORB_DEFAULT_CONFIG)
    c.update(overrides)
    return c


def _make_day_5m(
    date_str: str = "2025-03-10",
    orb_high: float = 102.0,
    orb_low: float = 98.0,
    post_orb_close: float = 103.0,
    n_bars: int = 78,  # voller Tag 9:30-16:00
) -> pd.DataFrame:
    """Erstelle synthetische 5-Minuten-Bars für einen Handelstag."""
    start = pd.Timestamp(f"{date_str} 09:30", tz="America/New_York")
    idx = pd.date_range(start, periods=n_bars, freq="5min")

    orb_end_idx = 6  # 30 min = 6 Bars
    prices = np.full(n_bars, (orb_high + orb_low) / 2.0)
    # ORB-Phase: Range zwischen orb_low und orb_high
    prices[:orb_end_idx] = np.linspace(orb_low, orb_high, orb_end_idx)
    # Post-ORB: zum post_orb_close wandern
    prices[orb_end_idx:] = np.linspace(
        (orb_high + orb_low) / 2.0, post_orb_close, n_bars - orb_end_idx
    )

    high = prices + 0.5
    low = prices - 0.5
    # ORB-Bars: korrektes High/Low
    high[:orb_end_idx] = orb_high
    low[:orb_end_idx] = orb_low

    df = pd.DataFrame(
        {
            "Open": prices - 0.2,
            "High": high,
            "Low": low,
            "Close": prices,
            "Volume": np.random.randint(10000, 50000, n_bars),
        },
        index=idx,
    )
    df["Volume_MA"] = df["Volume"].rolling(6, min_periods=1).mean()
    df["Volume_Ratio"] = df["Volume"] / df["Volume_MA"].replace(0, np.nan)
    df["ATR"] = (df["High"] - df["Low"]).rolling(14, min_periods=1).mean()
    return df


def _make_position(
    side: str = "long",
    entry: float = 100.0,
    risk_per_share: float = 2.0,
    shares: int = 10,
    profit_r: float = 2.0,
    sl_r: float = 1.0,
) -> dict:
    """Erstelle Position-Dict wie im Backtest."""
    if side == "long":
        stop = entry - sl_r * risk_per_share
        target = entry + profit_r * risk_per_share
    else:
        stop = entry + sl_r * risk_per_share
        target = entry - profit_r * risk_per_share
    return {
        "side": side,
        "entry": entry,
        "stop": stop,
        "target": target,
        "shares": shares,
        "risk_per_share": risk_per_share,
        "highest": entry,
        "lowest": entry,
        "trail_stop": None,
    }


TS = pd.Timestamp("2025-03-10 11:00", tz="America/New_York")
SL = 0.0002  # slippage
CM = 0.00005  # commission


# ══════════════════════════ ORB-Levels ══════════════════════════════════════

class TestCalculateORBLevels:
    def test_basic(self):
        df = _make_day_5m(orb_high=105, orb_low=95)
        h, l, r = calculate_orb_levels(df, 30)
        assert h == pytest.approx(105.0, abs=0.01)
        assert l == pytest.approx(95.0, abs=0.01)
        assert r == pytest.approx(10.0, abs=0.01)

    def test_empty_df(self):
        df = pd.DataFrame()
        assert calculate_orb_levels(df, 30) == (0.0, 0.0, 0.0)

    def test_single_bar(self):
        start = pd.Timestamp("2025-03-10 09:30", tz="America/New_York")
        df = pd.DataFrame(
            {"Open": [100], "High": [101], "Low": [99], "Close": [100], "Volume": [10000]},
            index=pd.DatetimeIndex([start]),
        )
        assert calculate_orb_levels(df, 30) == (0.0, 0.0, 0.0)

    def test_custom_orb_minutes(self):
        df = _make_day_5m(orb_high=110, orb_low=90, n_bars=78)
        h60, l60, r60 = calculate_orb_levels(df, 60)
        # 60 Minuten = 12 Bars → mehr Bars mit High=110, Low=90
        assert r60 > 0


# ══════════════════════════ Breakout-Logik ═══════════════════════════════════

class TestCheckBreakout:
    def test_long_breakout(self):
        side, strength = check_breakout(103.0, 100.0, 95.0, 5.0, 1.0, False)
        assert side == "long"
        assert strength > 0

    def test_short_breakout(self):
        side, strength = check_breakout(93.0, 100.0, 95.0, 5.0, 1.0, False)
        assert side == "short"
        assert strength > 0

    def test_no_breakout(self):
        side, strength = check_breakout(97.5, 100.0, 95.0, 5.0, 1.0, False)
        assert side == ""
        assert strength == 0.0

    def test_volume_boost(self):
        _, s_no_vol = check_breakout(103.0, 100.0, 95.0, 5.0, 1.0, False)
        _, s_vol = check_breakout(103.0, 100.0, 95.0, 5.0, 1.0, True)
        assert s_vol >= s_no_vol

    def test_zero_range(self):
        side, _ = check_breakout(100.0, 100.0, 100.0, 0.0, 1.0, False)
        assert side == ""

    def test_multiplier(self):
        # Mit multiplier=1.5: breakout_level = 100 + 0.5*5 = 102.5
        side, _ = check_breakout(102.0, 100.0, 95.0, 5.0, 1.5, False)
        assert side == ""  # 102 < 102.5
        side2, _ = check_breakout(103.0, 100.0, 95.0, 5.0, 1.5, False)
        assert side2 == "long"

    def test_strength_capped_at_1(self):
        _, strength = check_breakout(120.0, 100.0, 95.0, 5.0, 1.0, True)
        assert strength <= 1.0


# ══════════════════════════ Stop-Loss ════════════════════════════════════════

class TestCalculateStop:
    def test_long_stop(self):
        stop = calculate_stop("long", 102.0, 100.0, 95.0, 5.0, 1.0)
        # max(orb_low=95, entry-1*range=97) = 97
        assert stop == pytest.approx(97.0)

    def test_short_stop(self):
        stop = calculate_stop("short", 93.0, 100.0, 95.0, 5.0, 1.0)
        # min(orb_high=100, entry+1*range=98) = 98
        assert stop == pytest.approx(98.0)

    def test_long_stop_clamp(self):
        # entry zu nah am orb_low → stop >= entry → Fallback 0.5*range
        stop = calculate_stop("long", 95.5, 100.0, 95.0, 5.0, 1.0)
        assert stop < 95.5

    def test_short_stop_clamp(self):
        stop = calculate_stop("short", 99.5, 100.0, 95.0, 5.0, 1.0)
        assert stop > 99.5


# ══════════════════════════ Position Sizing ══════════════════════════════════

class TestCalculatePositionSize:
    def test_basic(self):
        shares = calculate_position_size(100.0, 98.0, 10000.0, 0.005)
        assert shares > 0

    def test_zero_risk(self):
        assert calculate_position_size(100.0, 100.0, 10000.0, 0.005) == 0

    def test_zero_equity(self):
        assert calculate_position_size(100.0, 98.0, 0.0, 0.005) == 0

    def test_notional_cap(self):
        # 25% von 10k = 2500 → max 25 shares bei 100$
        shares = calculate_position_size(100.0, 99.99, 10000.0, 0.50,
                                         max_position_value_pct=0.25)
        assert shares <= 25

    def test_equity_risk_cap(self):
        shares = calculate_position_size(100.0, 99.0, 10000.0, 0.50,
                                         max_equity_at_risk=0.01)
        # max_eq_risk cap: 10000*0.01/1 = 100
        assert shares <= 100


# ══════════════════════════ Exit-Management ══════════════════════════════════

class TestManageLong:
    def test_stop_loss(self):
        pos = _make_position("long", entry=100.0, risk_per_share=2.0)
        result = _manage_long(
            "TEST", pos, price=97.0, high=98.0, low=97.0,
            bar_ts=TS, profit_r=2.0, sl_r=1.0,
            trail_after_r=1.0, trail_dist_r=0.5,
            slippage=SL, commission=CM,
        )
        assert result is not None
        assert result["trade"]["reason"] == "Stop Loss"
        assert result["pnl"] < 0

    def test_profit_target(self):
        pos = _make_position("long", entry=100.0, risk_per_share=2.0, profit_r=2.0)
        result = _manage_long(
            "TEST", pos, price=104.5, high=105.0, low=104.0,
            bar_ts=TS, profit_r=2.0, sl_r=1.0,
            trail_after_r=1.0, trail_dist_r=0.5,
            slippage=SL, commission=CM,
        )
        assert result is not None
        assert "Profit Target" in result["trade"]["reason"]
        assert result["pnl"] > 0

    def test_trailing_stop(self):
        pos = _make_position("long", entry=100.0, risk_per_share=2.0, profit_r=10.0)
        pos["highest"] = 106.0
        pos["trail_stop"] = 105.0
        result = _manage_long(
            "TEST", pos, price=104.0, high=104.5, low=104.0,
            bar_ts=TS, profit_r=10.0, sl_r=1.0,
            trail_after_r=1.0, trail_dist_r=0.5,
            slippage=SL, commission=CM,
        )
        assert result is not None
        assert result["trade"]["reason"] == "Trailing Stop"

    def test_no_exit(self):
        pos = _make_position("long", entry=100.0, risk_per_share=2.0, profit_r=10.0)
        result = _manage_long(
            "TEST", pos, price=101.0, high=101.5, low=100.5,
            bar_ts=TS, profit_r=10.0, sl_r=1.0,
            trail_after_r=100.0, trail_dist_r=0.5,
            slippage=SL, commission=CM,
        )
        assert result is None

    def test_highest_tracked(self):
        pos = _make_position("long", entry=100.0, risk_per_share=2.0, profit_r=10.0)
        result = _manage_long(
            "TEST", pos, price=105.0, high=106.0, low=104.5,
            bar_ts=TS, profit_r=10.0, sl_r=1.0,
            trail_after_r=100.0, trail_dist_r=0.5,  # Trail deaktiviert
            slippage=SL, commission=CM,
        )
        assert result is None, "Sollte nicht exiten"
        assert pos["highest"] == 105.0  # price, nicht high


class TestManageShort:
    def test_stop_loss(self):
        pos = _make_position("short", entry=100.0, risk_per_share=2.0)
        result = _manage_short(
            "TEST", pos, price=103.0, high=103.0, low=102.0,
            bar_ts=TS, profit_r=2.0, sl_r=1.0,
            trail_after_r=1.0, trail_dist_r=0.5,
            slippage=SL, commission=CM,
        )
        assert result is not None
        assert result["trade"]["reason"] == "Stop Loss"
        assert result["pnl"] < 0

    def test_profit_target(self):
        pos = _make_position("short", entry=100.0, risk_per_share=2.0, profit_r=2.0)
        result = _manage_short(
            "TEST", pos, price=96.0, high=96.5, low=95.5,
            bar_ts=TS, profit_r=2.0, sl_r=1.0,
            trail_after_r=1.0, trail_dist_r=0.5,
            slippage=SL, commission=CM,
        )
        assert result is not None
        assert "Profit Target" in result["trade"]["reason"]

    def test_trailing_stop(self):
        pos = _make_position("short", entry=100.0, risk_per_share=2.0, profit_r=10.0)
        pos["lowest"] = 94.0
        pos["trail_stop"] = 95.0
        result = _manage_short(
            "TEST", pos, price=96.0, high=96.0, low=95.5,
            bar_ts=TS, profit_r=10.0, sl_r=1.0,
            trail_after_r=1.0, trail_dist_r=0.5,
            slippage=SL, commission=CM,
        )
        assert result is not None
        assert result["trade"]["reason"] == "Trailing Stop"

    def test_no_exit(self):
        pos = _make_position("short", entry=100.0, risk_per_share=2.0, profit_r=10.0)
        result = _manage_short(
            "TEST", pos, price=99.0, high=99.5, low=98.5,
            bar_ts=TS, profit_r=10.0, sl_r=1.0,
            trail_after_r=100.0, trail_dist_r=0.5,
            slippage=SL, commission=CM,
        )
        assert result is None


# ══════════════════════════ Time-Decay ══════════════════════════════════════

class TestTimeDecayFactor:
    def test_prime_time(self):
        # 10:15 ET → 15 min nach ORB → ≤30 → 1.0
        assert time_decay_factor(time(10, 15)) == pytest.approx(1.0)

    def test_mid_morning(self):
        # 11:00 ET → 60 min → ≤90 → 0.85
        assert time_decay_factor(time(11, 0)) == pytest.approx(0.85)

    def test_mid_session(self):
        # 12:30 ET → 150 min → ≤180 → 0.65
        assert time_decay_factor(time(12, 30)) == pytest.approx(0.65)

    def test_late_session(self):
        # 14:30 ET → 270 min → >180 → 0.40
        assert time_decay_factor(time(14, 30)) == pytest.approx(0.40)

    def test_custom_brackets(self):
        cfg = {"time_decay_brackets": [(60, 1.0), (120, 0.5)], "time_decay_late_factor": 0.1}
        # 11:30 ET → 90 min → ≤120 → 0.5
        assert time_decay_factor(time(11, 30), cfg) == pytest.approx(0.5)
        # 14:00 ET → 240 min → >120 → 0.1
        assert time_decay_factor(time(14, 0), cfg) == pytest.approx(0.1)

    def test_no_config(self):
        # Soll nicht crashen mit None
        result = time_decay_factor(time(10, 15), None)
        assert result == pytest.approx(1.0)


# ══════════════════════════ Entry Cutoff ════════════════════════════════════

class TestCheckEntryCutoff:
    def test_no_cutoff(self):
        cfg = _cfg(entry_cutoff_time=None)
        dt = datetime(2025, 3, 10, 20, 0, tzinfo=pytz.UTC)  # ~15:00 ET
        assert check_entry_cutoff(dt, cfg) is True

    def test_before_cutoff(self):
        cfg = _cfg(entry_cutoff_time=time(14, 30))
        # 18:00 UTC = 14:00 ET (winter)
        dt = datetime(2025, 3, 10, 18, 0, tzinfo=pytz.UTC)
        assert check_entry_cutoff(dt, cfg) is True

    def test_after_cutoff(self):
        cfg = _cfg(entry_cutoff_time=time(14, 30))
        # 20:00 UTC = 16:00 ET → nach Cutoff
        dt = datetime(2025, 3, 10, 20, 0, tzinfo=pytz.UTC)
        assert check_entry_cutoff(dt, cfg) is False


# ══════════════════════════ Gap-Filter ══════════════════════════════════════

class TestCheckGapFilter:
    def test_small_gap(self):
        assert check_gap_filter(101.0, 100.0, 0.03) is True

    def test_large_gap(self):
        assert check_gap_filter(105.0, 100.0, 0.03) is False

    def test_zero_prev_close(self):
        assert check_gap_filter(100.0, 0.0, 0.03) is True

    def test_negative_gap(self):
        assert check_gap_filter(96.0, 100.0, 0.03) is False


# ══════════════════════════ Trend-Filter ═══════════════════════════════════

class TestCheckTrendFilter:
    def test_bullish(self):
        prices = np.linspace(95, 105, 30)
        df = pd.DataFrame({"Close": prices})
        result = check_trend_filter(df, 20)
        assert result["bullish"] is True

    def test_bearish(self):
        prices = np.linspace(105, 95, 30)
        df = pd.DataFrame({"Close": prices})
        result = check_trend_filter(df, 20)
        assert result["bearish"] is True

    def test_empty_df(self):
        result = check_trend_filter(None, 20)
        assert result["bullish"] is True
        assert result["bearish"] is True

    def test_short_df(self):
        df = pd.DataFrame({"Close": [100, 101]})
        result = check_trend_filter(df, 20)
        assert result["bullish"] is True


# ══════════════════════════ Timezone ═══════════════════════════════════════

class TestTimezone:
    def test_to_et_utc(self):
        dt = datetime(2025, 3, 10, 18, 0, tzinfo=pytz.UTC)
        result = to_et(dt)
        assert result.hour == 14  # EDT

    def test_to_et_time(self):
        dt = datetime(2025, 3, 10, 18, 0, tzinfo=pytz.UTC)
        result = to_et_time(dt)
        assert result == time(14, 0)

    def test_is_market_hours(self):
        # 14:30 UTC = 10:30 ET in March → market hours
        dt = datetime(2025, 3, 10, 14, 30, tzinfo=pytz.UTC)
        assert is_market_hours(dt) is True

    def test_outside_market_hours(self):
        # 22:00 UTC = 18:00 ET → closed
        dt = datetime(2025, 3, 10, 22, 0, tzinfo=pytz.UTC)
        assert is_market_hours(dt) is False

    def test_is_orb_period(self):
        # 13:45 UTC = 9:45 ET → in ORB
        dt = datetime(2025, 3, 10, 13, 45, tzinfo=pytz.UTC)
        assert is_orb_period(dt, 30) is True

    def test_not_orb_period(self):
        # 15:00 UTC = 11:00 ET → nach ORB
        dt = datetime(2025, 3, 10, 15, 0, tzinfo=pytz.UTC)
        assert is_orb_period(dt, 30) is False


# ══════════════════════════ MIT Overlay ═════════════════════════════════════

class TestMITOverlay:
    def _base_ctx(self) -> dict:
        return {
            "volume_ratio": 1.5,
            "volume_confirmed": True,
            "orb_range_pct": 0.5,
            "trend": {"bullish": True, "bearish": True},
        }

    def _base_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "Close": [100.0], "ATR": [2.0],
            "Volume_Ratio": [1.5], "Volume": [50000],
        })

    def test_overlay_disabled(self):
        cfg = _cfg(use_mit_probabilistic_overlay=False)
        ok, factor, reason = mit_apply_overlay("BUY", 0.5, {}, pd.DataFrame(), cfg)
        assert ok is True
        assert factor == 1.0

    def test_overlay_rejects_weak(self):
        cfg = _cfg(mit_min_strength=0.30)
        ok, _, _ = mit_apply_overlay("BUY", 0.10, self._base_ctx(),
                                     self._base_df(), cfg)
        assert ok is False

    def test_overlay_accepts_strong(self):
        cfg = _cfg()
        ok, factor, reason = mit_apply_overlay(
            "BUY", 0.80, self._base_ctx(), self._base_df(), cfg
        )
        assert ok is True
        assert factor > 0

    def test_no_signal(self):
        cfg = _cfg()
        ok, _, _ = mit_apply_overlay("HOLD", 0.5, {}, pd.DataFrame(), cfg)
        assert ok is False

    def test_ev_computation(self):
        # P=0.6, R=2, risk=1 → EV = 0.6*2 - 0.4*1 = 0.8
        assert mit_compute_ev_r(0.6, 2.0, 1.0) == pytest.approx(0.8)

    def test_kelly_fraction(self):
        # P=0.6, b=2/1=2 → kelly = (2*0.6-0.4)/2 = 0.4
        assert mit_kelly_fraction(0.6, 2.0, 1.0) == pytest.approx(0.4)

    def test_kelly_negative(self):
        # P=0.2, b=1 → kelly = (1*0.2-0.8)/1 = -0.6 → 0
        assert mit_kelly_fraction(0.2, 1.0, 1.0) == 0.0

    def test_calibration_offset(self):
        cfg = _cfg(mit_calibration_offset=0.05)
        ok1, f1, _ = mit_apply_overlay(
            "BUY", 0.80, self._base_ctx(), self._base_df(), cfg
        )
        cfg2 = _cfg(mit_calibration_offset=0.0)
        ok2, f2, _ = mit_apply_overlay(
            "BUY", 0.80, self._base_ctx(), self._base_df(), cfg2
        )
        # Mit positivem Offset sollte qty_factor >= ohne Offset
        if ok1 and ok2:
            assert f1 >= f2


# ══════════════════════════ Dynamic Kelly (DD-Scaling) ═══════════════════════

class TestDynamicKelly:
    def test_no_drawdown(self):
        assert dynamic_kelly(0.5, 0.0, 0.15) == pytest.approx(0.5)

    def test_max_drawdown(self):
        assert dynamic_kelly(0.5, 0.15, 0.15) == 0.0

    def test_over_max(self):
        assert dynamic_kelly(0.5, 0.20, 0.15) == 0.0

    def test_partial_drawdown(self):
        result = dynamic_kelly(0.5, 0.075, 0.15)
        assert 0 < result < 0.5

    def test_exponential_scaling(self):
        # Höherer DD → stärkere Reduktion (superlinear)
        r1 = dynamic_kelly(1.0, 0.03, 0.15)
        r2 = dynamic_kelly(1.0, 0.06, 0.15)
        r3 = dynamic_kelly(1.0, 0.12, 0.15)
        assert r1 > r2 > r3 > 0


# ══════════════════════════ VIX Term Structure ══════════════════════════════

class TestVIXTermStructure:
    def test_contango(self):
        regime, mult, _ = get_vix_term_structure_regime(18.0, 22.0)
        assert regime == "contango"
        assert mult == 1.0

    def test_flat(self):
        regime, mult, _ = get_vix_term_structure_regime(19.0, 20.0)
        assert regime == "flat"
        assert mult == 0.75

    def test_backwardation(self):
        regime, mult, _ = get_vix_term_structure_regime(22.0, 20.0)
        assert regime == "backwardation"
        assert mult == 0.50

    def test_extreme_backwardation(self):
        regime, mult, _ = get_vix_term_structure_regime(25.0, 20.0)
        assert regime == "extreme_backwardation"
        assert mult == 0.0

    def test_vix3m_fallback(self):
        # None → Fallback zu vix_spot * 1.02 → ratio = 20/20.4 ≈ 0.98 → flat
        regime, _, _ = get_vix_term_structure_regime(20.0, None)
        assert regime == "flat"

    def test_zero_vix(self):
        # 0 → Fallback zu 20
        regime, _, _ = get_vix_term_structure_regime(0.0, None)
        assert regime in ("contango", "flat")


# ══════════════════════════ Calibrate Win Probability ═══════════════════════

class TestCalibrateWinProbability:
    def test_basic(self):
        trades = pd.DataFrame({
            "side": ["BUY"] * 20 + ["SELL"] * 20,
            "pnl": [0] * 20 + [100] * 14 + [-50] * 6,  # 70% WR
            "strength": [0.5] * 20 + [0] * 20,
        })
        result = calibrate_win_probability(trades)
        assert "offset" in result
        assert result["n_trades"] == 20
        assert result["actual_win_rate"] == pytest.approx(0.70)

    def test_empty(self):
        result = calibrate_win_probability(pd.DataFrame())
        assert result["offset"] == 0.0

    def test_too_few_trades(self):
        trades = pd.DataFrame({
            "side": ["BUY", "SELL"],
            "pnl": [0, 100],
            "strength": [0.5, 0],
        })
        result = calibrate_win_probability(trades)
        assert "error" in result


# ══════════════════════════ Compute ORB Signals (Vectorized) ════════════════

class TestComputeORBSignals:
    def test_long_signal(self):
        df = _make_day_5m(orb_high=100, orb_low=95, post_orb_close=103)
        cfg = _cfg(min_signal_strength=0.0, volume_multiplier=0.0)
        signals = compute_orb_signals(df, 100, 95, 5.0, cfg)
        assert signals["entry_long"].any()

    def test_short_signal(self):
        df = _make_day_5m(orb_high=100, orb_low=95, post_orb_close=92)
        cfg = _cfg(min_signal_strength=0.0, volume_multiplier=0.0, allow_shorts=True)
        signals = compute_orb_signals(df, 100, 95, 5.0, cfg)
        assert signals["entry_short"].any()

    def test_shorts_disabled(self):
        df = _make_day_5m(orb_high=100, orb_low=95, post_orb_close=92)
        cfg = _cfg(min_signal_strength=0.0, volume_multiplier=0.0, allow_shorts=False)
        signals = compute_orb_signals(df, 100, 95, 5.0, cfg)
        assert not signals["entry_short"].any()

    def test_time_decay_applied(self):
        df = _make_day_5m(orb_high=100, orb_low=95, post_orb_close=103)
        cfg_decay = _cfg(use_time_decay_filter=True, min_signal_strength=0.0,
                         volume_multiplier=0.0)
        cfg_no = _cfg(use_time_decay_filter=False, min_signal_strength=0.0,
                      volume_multiplier=0.0)
        s_decay = compute_orb_signals(df, 100, 95, 5.0, cfg_decay)
        s_no = compute_orb_signals(df, 100, 95, 5.0, cfg_no)
        # Late bars should have lower strength with decay
        late_idx = s_decay.index[50:]  # well past ORB
        if s_no.loc[late_idx, "strength"].sum() > 0:
            assert s_decay.loc[late_idx, "strength"].sum() <= s_no.loc[late_idx, "strength"].sum()


# ══════════════════════════ Config ═════════════════════════════════════════

class TestConfig:
    def test_default_config_has_all_keys(self):
        required = [
            "symbols", "opening_range_minutes", "risk_per_trade",
            "profit_target_r", "stop_loss_r", "trail_after_r",
            "eod_close_time", "use_time_decay_filter",
            "time_decay_brackets", "time_decay_late_factor",
            "use_mit_probabilistic_overlay", "mit_calibration_offset",
        ]
        for key in required:
            assert key in ORB_DEFAULT_CONFIG, f"Missing config key: {key}"

    def test_eod_close_time_type(self):
        assert isinstance(ORB_DEFAULT_CONFIG["eod_close_time"], time)

    def test_cfg_helper(self):
        c = _cfg(risk_per_trade=0.01)
        assert c["risk_per_trade"] == 0.01
        assert c["symbols"] == ORB_DEFAULT_CONFIG["symbols"]


# ══════════════════════════ Win-Probability Estimation ═══════════════════════

class TestMITEstimateWinProb:
    def test_range(self):
        ctx = {"volume_ratio": 1.5, "volume_confirmed": True,
               "orb_range_pct": 0.5, "trend": {"bullish": True, "bearish": True}}
        df = pd.DataFrame({"Close": [100.0], "ATR": [2.0],
                           "Volume_Ratio": [1.5]})
        prob = mit_estimate_win_probability("BUY", 0.8, ctx, df)
        assert 0.20 <= prob <= 0.80

    def test_low_strength(self):
        ctx = {"volume_ratio": 0.5, "volume_confirmed": False,
               "orb_range_pct": 3.0, "trend": {"bullish": False, "bearish": True}}
        df = pd.DataFrame({"Close": [100.0], "ATR": [2.0],
                           "Volume_Ratio": [0.5]})
        prob = mit_estimate_win_probability("BUY", 0.1, ctx, df)
        assert prob <= 0.50


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

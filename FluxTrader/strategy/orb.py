"""ORB-Strategie (Opening Range Breakout).

Migriert 1:1 aus ORB_Bot/orb_strategy.py:
  - Opening-Range + Breakout-Multiplier (Fix #1)
  - Timezone-sichere Signal-Logik (Fix #2)
  - Trend-Filter via SPY EMA (Fix #5)
  - Tageszeit-spezifische Volume-MA (Fix #6)
  - Gap-Filter (Fix #7)
  - Stop-Loss an ORB-Range (Fix #9)
  - Short-Seite + Friday-Filter (Fix #10)
  - Time-Decay + Entry-Cutoff (Fix #13)
  - MIT Probabilistic Overlay + DD-Scaling (Fix #14/#15)
  - VIX Term Structure Regime (Fix #16)

Broker-agnostisch: kein Alpaca-Import, kein HTTP, kein I/O.

Backtest-Optimierungen:
  - Precomputed indicator frame statt O(n²) Neuberechnung
  - Day-Cache für ORB-Levels, Trend-Filter, Gap-Check
"""
from __future__ import annotations

from datetime import time
from typing import Optional

import numpy as np
import pandas as pd

from core.filters import (
    check_breakout,
    correlation_group,
    entry_cutoff_ok,
    gap_filter,
    is_market_hours,
    is_orb_period,
    mit_independence_blocked,
    time_decay_factor,
    to_et,
    to_et_time,
    trend_filter_from_spy,
    vix_term_structure_regime,
)
from core.indicators import (
    compute_indicator_frame,
    opening_range_levels,
    orb_volume_ratio,
)
from core.logging import get_logger
from core.models import Bar, Signal
from core.risk import (
    dynamic_kelly,
    expected_value_r,
    kelly_fraction,
    mit_estimate_win_probability,
    orb_range_stop,
    target_from_r,
)
from strategy.base import BaseStrategy
from strategy.registry import register

log = get_logger(__name__)


# ─────────────────────────── Default-Config ─────────────────────────────────

ORB_DEFAULT_PARAMS: dict = {
    "opening_range_minutes": 15,
    "orb_breakout_multiplier": 1.15,
    "volume_multiplier": 1.7,
    "min_signal_strength": 0.25,

    "stop_loss_r": 1.0,
    "profit_target_r": 2.0,
    "trail_after_r": 1.0,
    "trail_distance_r": 0.6,

    "allow_shorts": True,
    "avoid_fridays": False,
    "avoid_mondays": False,

    "market_open": time(9, 30),
    "market_close": time(16, 0),
    "orb_end_time": time(10, 0),
    "eod_close_time": time(15, 27),

    "use_trend_filter": True,
    "trend_ema_period": 20,
    "use_gap_filter": True,
    "max_gap_pct": 0.03,
    "volume_lookback_days": 10,

    "use_time_decay_filter": True,
    "time_decay_brackets": [(30, 1.00), (90, 0.85), (180, 0.65)],
    "time_decay_late_factor": 0.40,
    "entry_cutoff_time": None,

    # MIT Overlay
    "use_mit_probabilistic_overlay": True,
    "mit_ev_threshold_r": 0.30,
    "mit_kelly_fraction": 0.50,
    "mit_min_strength": 0.25,
    "mit_calibration_offset": 0.0317,
    "use_dynamic_kelly_dd_scaling": True,
    "dynamic_kelly_max_dd": 0.15,
    "use_mit_independence_guard": True,
    "mit_correlation_groups": {
        "index_etfs": ["SPY", "QQQ", "IWM", "DIA"],
        "semi_ai": ["NVDA", "AMD", "AVGO"],
        "mega_cap_tech": ["AAPL", "MSFT", "META", "AMZN", "GOOGL"],
        "high_beta_growth": ["TSLA", "PLTR", "NFLX"],
    },

    # VIX Regime
    "use_vix_term_structure": True,
    "vix_regime_flat_lower": 0.90,
    "vix_regime_flat_upper": 1.00,
    "vix_regime_backwd_upper": 1.15,
}


# ─────────────────────────── Hilfs-Aggregation ──────────────────────────────

def _bars_to_df(bars: list[Bar]) -> pd.DataFrame:
    """Wandele gepufferte Bars in einen DataFrame mit UTC-Index."""
    if not bars:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    idx = pd.DatetimeIndex([b.timestamp for b in bars], tz="UTC")
    return pd.DataFrame({
        "Open": [b.open for b in bars],
        "High": [b.high for b in bars],
        "Low": [b.low for b in bars],
        "Close": [b.close for b in bars],
        "Volume": [b.volume for b in bars],
    }, index=idx)


# ─────────────────────────── Strategie-Klasse ──────────────────────────────

@register("orb")
class ORBStrategy(BaseStrategy):
    """Opening Range Breakout (bar-zentriert, DI über Context-Service)."""

    def __init__(self, config: dict, context=None):
        merged = dict(ORB_DEFAULT_PARAMS)
        merged.update(config or {})
        super().__init__(merged, context=context)
        self._today_reserved_group: Optional[str] = None
        # Day-Cache: verhindert wiederholte Berechnungen innerhalb eines Tages
        self._orb_cache: dict[tuple[str, object], tuple[float, float, float]] = {}
        self._gap_cache: dict[tuple[str, object], tuple[bool, float]] = {}
        self._trend_cache: dict[object, dict] = {}
        self._vol_ratio_cache: dict[tuple[str, object], float] = {}

    @property
    def name(self) -> str:
        return "orb"

    def _is_ready(self) -> bool:
        min_bars = int(self.config.get("min_bars",
                                       self.config.get("atr_period", 14) + 6))
        # Precomputed frame: Cursor prüfen statt bars-Liste
        if self.bars:
            cursor = self.context.bar_cursor(self.bars[-1].symbol)
            if cursor >= 0:
                return cursor + 1 >= min_bars
        return len(self.bars) >= min_bars

    def reset(self) -> None:
        super().reset()
        self._orb_cache.clear()
        self._gap_cache.clear()
        self._trend_cache.clear()
        self._vol_ratio_cache.clear()

    # ── Core Signal-Logic ──────────────────────────────────────────────

    def _generate_signals(self, bar: Bar) -> list[Signal]:
        cfg = self.config
        symbol = bar.symbol
        current_time = bar.timestamp

        # ── Precomputed-Frame Fast-Path (Variante A) ──────────────────
        full_df = self.context.indicator_frame(symbol)
        if full_df is not None:
            cursor = self.context.bar_cursor(symbol)
            if cursor < 5:
                return []
            df = full_df.iloc[:cursor + 1]
        else:
            # Fallback für Live-Modus (kein precomputed frame)
            df_raw = _bars_to_df(
                [b for b in self.bars if b.symbol == symbol]
                or self.context.bars(symbol)
            )
            if df_raw.empty or len(df_raw) < 6:
                return []
            df = compute_indicator_frame(df_raw, volume_lookback=20)

        # Market-Hours Check (früh prüfen, spart Arbeit)
        if not is_market_hours(current_time):
            return []
        et_time = to_et_time(current_time)
        orb_minutes = int(cfg.get("opening_range_minutes", 30))
        orb_end = cfg.get("orb_end_time", time(10, 0))
        if is_orb_period(current_time, orb_minutes) or et_time < orb_end:
            return []

        if not entry_cutoff_ok(current_time, cfg.get("entry_cutoff_time")):
            return []

        # ── Day-Key für Caches (Variante B) ───────────────────────────
        day_et = to_et(current_time)
        day_key = day_et.date() if hasattr(day_et, "date") else day_et

        # ── ORB-Levels (day-cached) ───────────────────────────────────
        orb_cache_key = (symbol, day_key)
        cached_orb = self._orb_cache.get(orb_cache_key)
        if cached_orb is not None:
            orb_h, orb_l, orb_r = cached_orb
        else:
            orb_h, orb_l, orb_r = self._current_day_orb(df, orb_minutes)
            self._orb_cache[orb_cache_key] = (orb_h, orb_l, orb_r)
        if orb_r <= 0:
            return []

        current_price = float(df["Close"].iloc[-1])

        # ── Gap-Filter (day-cached) ───────────────────────────────────
        if cfg.get("use_gap_filter", True):
            gap_key = (symbol, day_key)
            cached_gap = self._gap_cache.get(gap_key)
            if cached_gap is not None:
                gap_ok, gap_pct = cached_gap
            else:
                gap_ok, gap_pct = self._gap_check(df, cfg.get("max_gap_pct", 0.03))
                self._gap_cache[gap_key] = (gap_ok, gap_pct)
            if not gap_ok:
                log.debug("orb.gap_block", symbol=symbol, gap_pct=gap_pct)
                return []

        # ── Volume-Ratio (day-cached) ─────────────────────────────────
        vol_key = (symbol, day_key)
        cached_vol = self._vol_ratio_cache.get(vol_key)
        if cached_vol is not None:
            vol_r = cached_vol
        else:
            vol_r = orb_volume_ratio(df, orb_minutes=orb_minutes)
            self._vol_ratio_cache[vol_key] = vol_r
        vol_mult = float(cfg.get("volume_multiplier", 1.3))
        volume_confirmed = vol_r >= vol_mult

        # ── Trend-Filter (day-cached) ─────────────────────────────────
        trend = {"bullish": True, "bearish": True}
        if cfg.get("use_trend_filter", True):
            cached_trend = self._trend_cache.get(day_key)
            if cached_trend is not None:
                trend = cached_trend
            else:
                trend = trend_filter_from_spy(
                    self.context.spy_df,
                    cfg.get("trend_ema_period", 20),
                )
                self._trend_cache[day_key] = trend

        # Breakout-Prüfung
        multiplier = float(cfg.get("orb_breakout_multiplier", 1.0))
        side, strength = check_breakout(
            current_price, orb_h, orb_l, orb_r, multiplier, volume_confirmed,
        )

        # Time-Decay
        if cfg.get("use_time_decay_filter", True):
            decay = time_decay_factor(
                et_time,
                brackets=cfg.get("time_decay_brackets"),
                late_factor=float(cfg.get("time_decay_late_factor", 0.40)),
            )
            strength *= decay

        min_strength = float(cfg.get("min_signal_strength", 0.3))
        if side == "" or strength < min_strength:
            return []

        # Trend-Gate
        if cfg.get("use_trend_filter", True):
            if side == "long" and not trend["bullish"]:
                return []
            if side == "short" and not trend["bearish"]:
                return []
        if side == "short" and not cfg.get("allow_shorts", True):
            return []

        # MIT-Independence-Guard (Cross-Symbol via Context)
        if (cfg.get("use_mit_probabilistic_overlay", False)
                and cfg.get("use_mit_independence_guard", True)):
            groups = cfg.get("mit_correlation_groups", {})
            blocked, reason = mit_independence_blocked(
                symbol,
                self.context.open_symbols,
                self.context.reserved_groups,
                groups,
            )
            if blocked:
                log.debug("orb.mit_blocked", symbol=symbol, reason=reason)
                return []

        # Stop + Target berechnen
        stop_price = orb_range_stop(
            side, current_price, orb_h, orb_l, orb_r,
            float(cfg.get("stop_loss_r", 1.0)),
        )
        target_price = target_from_r(
            side, current_price, stop_price,
            float(cfg.get("profit_target_r", 2.0)),
        )

        # MIT Overlay -> qty_factor in metadata
        qty_factor = 1.0
        overlay_reason = ""
        if cfg.get("use_mit_probabilistic_overlay", False):
            qty_factor, overlay_reason, overlay_ok = self._mit_overlay(
                side, strength, df, vol_r, volume_confirmed, orb_r, orb_l,
            )
            if not overlay_ok:
                log.debug("orb.mit_overlay_reject",
                          symbol=symbol, reason=overlay_reason)
                return []

        direction = 1 if side == "long" else -1
        reason = (f"ORB {'Breakout' if side == 'long' else 'Breakdown'}: "
                  f"{current_price:.2f} {'>' if side == 'long' else '<'} "
                  f"{orb_h if side == 'long' else orb_l:.2f}")
        if volume_confirmed:
            reason += f" +Vol {vol_r:.1f}x"
        if overlay_reason:
            reason += f" | {overlay_reason}"

        signal = Signal(
            strategy_id=self.name,
            symbol=symbol,
            direction=direction,
            strength=float(np.clip(strength, 0.0, 1.0)),
            stop_price=float(stop_price),
            target_price=float(target_price),
            timestamp=current_time,
            metadata={
                "entry_price": current_price,
                "orb_high": orb_h,
                "orb_low": orb_l,
                "orb_range": orb_r,
                "volume_ratio": vol_r,
                "volume_confirmed": volume_confirmed,
                "qty_factor": qty_factor,
                "reason": reason,
            },
        )

        # Gruppen-Reservierung (Runner ruft reserve_group; Strategie signalisiert nur)
        signal.metadata["reserve_group"] = correlation_group(
            symbol, cfg.get("mit_correlation_groups", {}),
        )
        return [signal]

    # ── Hilfsmethoden ────────────────────────────────────────────────

    @staticmethod
    def _current_day_orb(df: pd.DataFrame,
                         orb_minutes: int) -> tuple[float, float, float]:
        idx_et = to_et(df.index)
        dates = idx_et.normalize()
        unique = pd.Index(dates).unique()
        if len(unique) == 0:
            return 0.0, 0.0, 0.0
        last = unique[-1]
        day_df = df[np.asarray(dates == last, dtype=bool)]
        if len(day_df) < 2:
            return 0.0, 0.0, 0.0
        return opening_range_levels(day_df, orb_minutes)

    @staticmethod
    def _gap_check(df: pd.DataFrame, max_gap_pct: float) -> tuple[bool, float]:
        idx_et = to_et(df.index)
        dates = idx_et.normalize()
        unique = pd.Index(dates).unique()
        if len(unique) < 2:
            return True, 0.0
        today = df[np.asarray(dates == unique[-1], dtype=bool)]
        prev = df[np.asarray(dates == unique[-2], dtype=bool)]
        if today.empty or prev.empty:
            return True, 0.0
        today_open = float(today["Open"].iloc[0])
        prev_close = float(prev["Close"].iloc[-1])
        ok = gap_filter(today_open, prev_close, max_gap_pct)
        gap_pct = abs(today_open - prev_close) / max(prev_close, 1e-9)
        return ok, gap_pct

    def _mit_overlay(self,
                     side: str,
                     strength: float,
                     df: pd.DataFrame,
                     vol_r: float,
                     volume_confirmed: bool,
                     orb_range: float,
                     orb_low: float) -> tuple[float, str, bool]:
        cfg = self.config
        signal_tag = "BUY" if side == "long" else "SHORT"

        min_strength = float(cfg.get("mit_min_strength", 0.15))
        if strength < min_strength:
            return 0.0, f"Strength {strength:.2f} < {min_strength:.2f}", False

        last = df.iloc[-1]
        close = float(last["Close"])
        atr_val = float(last.get("ATR", 0.0) or 0.0)
        atr_pct = (atr_val / close * 100.0) if close > 0 and atr_val > 0 else 0.0
        orb_range_pct = (orb_range / orb_low * 100.0) if orb_low > 0 else 0.0

        trend = trend_filter_from_spy(
            self.context.spy_df,
            cfg.get("trend_ema_period", 20),
        )
        cal_offset = float(cfg.get("mit_calibration_offset", 0.0))

        p = mit_estimate_win_probability(
            signal=signal_tag,
            strength=strength,
            volume_ratio=vol_r,
            volume_confirmed=volume_confirmed,
            orb_range_pct=orb_range_pct,
            atr_pct=atr_pct,
            trend_bullish=trend["bullish"],
            trend_bearish=trend["bearish"],
            calibration_offset=cal_offset,
        )

        reward_r = float(cfg.get("profit_target_r", 2.0))
        ev = expected_value_r(p, reward_r, 1.0)
        ev_threshold = float(cfg.get("mit_ev_threshold_r", 0.08))
        if ev <= ev_threshold:
            return 0.0, f"P={p:.2f} EV={ev:+.2f}R <= {ev_threshold:.2f}", False

        k = kelly_fraction(p, reward_r, 1.0)
        frac_k = k * float(cfg.get("mit_kelly_fraction", 0.50))

        note = ""
        if cfg.get("use_dynamic_kelly_dd_scaling", False):
            max_dd = float(cfg.get("dynamic_kelly_max_dd", 0.15))
            dd = self.context.account.drawdown
            frac_k = dynamic_kelly(frac_k, dd, max_dd)
            note = f" [DD {dd:.2%} -> K={frac_k:.3f}]"

        if cfg.get("use_vix_term_structure", False):
            vix_spot, vix_3m = self.context.vix
            if vix_spot is not None:
                regime, vix_mult, regime_reason = vix_term_structure_regime(
                    vix_spot, vix_3m,
                    flat_lower=float(cfg.get("vix_regime_flat_lower", 0.90)),
                    flat_upper=float(cfg.get("vix_regime_flat_upper", 1.00)),
                    backwd_upper=float(cfg.get("vix_regime_backwd_upper", 1.15)),
                )
                if regime == "extreme_backwardation":
                    if signal_tag == "BUY":
                        return 0.0, regime_reason, False
                    vix_mult = 1.0
                frac_k *= vix_mult
                note += f" [VIX:{regime} {vix_mult:.2f}x]"

        qty_factor = float(np.clip(0.25 + frac_k, 0.25, 1.0))
        reason = f"MIT P={p:.2f} EV={ev:+.2f}R Kelly={qty_factor:.2f}x{note}"
        return qty_factor, reason, True

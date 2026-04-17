"""One-Bar-Breakout-Strategie (50-Bar High/Low Momentum auf Daily-Bars).

Regeln (exakt nach Original):
  - Long:  Close > max(High der letzten N Bars, exkl. aktueller Bar)
  - Short: Close < min(Low  der letzten N Bars, exkl. aktueller Bar)
  - Haltedauer: genau 1 Bar -> Exit am nächsten Open (wird vom TradeManager
    mit target=None, stop=None und einer "exit_next_open"-Anweisung in
    Signal.metadata abgebildet)
  - Kein SL, kein TP, keine zusätzlichen Filter

Backtest-Optimierungen:
  - Precomputed indicator frame statt O(n²) Neuberechnung
  - Rolling-High/Low einmal vorab berechnet, dann per Index abgerufen
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from core.indicators import rolling_high_low
from core.logging import get_logger
from core.models import Bar, FeatureVector, Signal
from core.risk import fixed_fraction_size, kelly_fraction
from strategy.base import BaseStrategy
from strategy.registry import register

log = get_logger(__name__)


OBB_DEFAULT_PARAMS: dict = {
    "lookback_bars": 50,
    "allow_shorts": True,
    "position_size_pct": 0.10,

    # Kelly (optional)
    "use_kelly_sizing": False,
    "kelly_fraction": 0.50,
    "kelly_lookback_trades": 50,
    "kelly_min_trades": 20,
    "kelly_payoff_ratio": 1.0,

    "max_daily_trades": 3,
    "max_concurrent_positions": 10,
}


def _bars_to_df(bars) -> pd.DataFrame:
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


@register("obb")
class OBBStrategy(BaseStrategy):
    """50-Bar-Rolling-High/Low-Breakout auf Daily Bars."""

    def __init__(self, config: dict, context=None):
        merged = dict(OBB_DEFAULT_PARAMS)
        merged.update(config or {})
        super().__init__(merged, context=context)
        # Cache für vorberechnetes rolling_high_low pro Symbol
        self._rolling_cache: dict[str, tuple[np.ndarray, np.ndarray]] = {}

    @property
    def name(self) -> str:
        return "obb"

    def _is_ready(self) -> bool:
        lookback = int(self.config.get("lookback_bars", 50))
        if self.bars:
            cursor = self.context.bar_cursor(self.bars[-1].symbol)
            if cursor >= 0:
                return cursor + 1 >= lookback + 2
        return len(self.bars) >= lookback + 2

    def reset(self) -> None:
        super().reset()
        self._rolling_cache.clear()

    def _generate_signals(self, bar: Bar) -> list[Signal]:
        cfg = self.config
        symbol = bar.symbol
        lookback = int(cfg.get("lookback_bars", 50))
        allow_shorts = bool(cfg.get("allow_shorts", True))

        # ── Precomputed-Frame Fast-Path (Variante A) ──────────────────
        full_df = self.context.indicator_frame(symbol)
        if full_df is not None:
            cursor = self.context.bar_cursor(symbol)
            if cursor < lookback + 1:
                return []

            # Rolling-High/Low einmal vorab berechnen und cachen
            if symbol not in self._rolling_cache:
                hi, lo = rolling_high_low(full_df, lookback,
                                          exclude_current=True)
                self._rolling_cache[symbol] = (hi.values, lo.values)

            hi_arr, lo_arr = self._rolling_cache[symbol]
            lookback_high = float(hi_arr[cursor])
            lookback_low = float(lo_arr[cursor])
            if np.isnan(lookback_high) or np.isnan(lookback_low):
                return []
            current_close = float(full_df["Close"].values[cursor])
        else:
            # Fallback für Live-Modus (kein precomputed frame)
            bars = [b for b in self.bars if b.symbol == symbol] \
                or self.context.bars(symbol)
            if len(bars) < lookback + 2:
                return []

            df = _bars_to_df(bars)
            hi, lo = rolling_high_low(df, lookback, exclude_current=True)
            lookback_high = float(hi.iloc[-1])
            lookback_low = float(lo.iloc[-1])
            if np.isnan(lookback_high) or np.isnan(lookback_low):
                return []
            current_close = float(df["Close"].iloc[-1])

        if current_close > lookback_high:
            side, direction = "long", 1
            reason = (f"OBB Long: Close {current_close:.2f} > "
                      f"{lookback}-Bar-High {lookback_high:.2f}")
        elif allow_shorts and current_close < lookback_low:
            side, direction = "short", -1
            reason = (f"OBB Short: Close {current_close:.2f} < "
                      f"{lookback}-Bar-Low {lookback_low:.2f}")
        else:
            return []

        strength = min(
            abs(current_close - (lookback_high if side == "long" else lookback_low))
            / max(current_close, 1e-9) * 100.0,
            1.0,
        )

        qty_hint = self._qty_hint(current_close)

        signal = Signal(
            strategy=self.name,
            symbol=symbol,
            features=FeatureVector(),
            direction=direction,
            strength=float(strength),
            stop_price=0.0,                 # kein SL (OBB-Regel)
            target_price=None,              # kein TP
            timestamp=bar.timestamp,
            metadata={
                "entry_price": current_close,
                "lookback_high": lookback_high,
                "lookback_low": lookback_low,
                "qty_hint": qty_hint,
                "exit_next_open": True,     # TradeManager-Hook
                "reason": reason,
            },
        )
        return [signal]

    # ── Hilfsmethoden ────────────────────────────────────────────────

    def _qty_hint(self, price: float) -> int:
        cfg = self.config
        equity = self.context.account.equity
        if equity <= 0:
            equity = float(cfg.get("initial_capital", 10_000.0))

        if cfg.get("use_kelly_sizing", False):
            win_rate = self._rolling_win_rate_from_context()
            if win_rate is not None:
                b = float(cfg.get("kelly_payoff_ratio", 1.0))
                p = float(np.clip(win_rate, 0.05, 0.95))
                k_full = kelly_fraction(p, b, 1.0)
                frac = k_full * float(cfg.get("kelly_fraction", 0.50))
                frac = min(frac, float(cfg.get("position_size_pct", 0.10)))
                return fixed_fraction_size(equity, price, frac)
        return fixed_fraction_size(
            equity, price, float(cfg.get("position_size_pct", 0.10)),
        )

    def _rolling_win_rate_from_context(self) -> Optional[float]:
        return None

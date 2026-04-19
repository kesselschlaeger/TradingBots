"""Risiko-Management: Position Sizing, Stops, Kelly, DD-Scaling."""
from __future__ import annotations

import numpy as np


# ─────────────────────────── Position Sizing ───────────────────────────────

def position_size(
    equity: float,
    risk_pct: float,
    entry: float,
    stop: float,
    max_equity_at_risk: float = 0.05,
    max_position_value_pct: float = 0.25,
) -> int:
    """R-basiertes Sizing mit Notional-Cap.

    Anteil von equity * risk_pct als Verlust-Budget je Trade.
    Zusätzlich hart gedeckelt durch max_equity_at_risk und
    max_position_value_pct * equity / entry.
    """
    risk = abs(entry - stop)
    if risk <= 0 or entry <= 0 or equity <= 0:
        return 0
    base = int((equity * risk_pct) / risk)
    cap_risk = int((equity * max_equity_at_risk) / risk)
    cap_notional = int((equity * max_position_value_pct) / entry)
    return max(0, min(base, cap_risk, cap_notional))


def partial_qty(total_qty: int, partial_pct: float) -> int:
    """Anzahl Anteile für Partial-Profit-Verkauf.

    Gibt mindestens 1 zurück (sofern total_qty > 0), damit der
    Partial-Close immer einen realen Trade auslöst.
    """
    if total_qty <= 0 or partial_pct <= 0:
        return 0
    return max(1, int(total_qty * partial_pct))


def fixed_fraction_size(equity: float, price: float, fraction: float) -> int:
    """Fixer Anteil am Eigenkapital (OBB-Style ohne Stop)."""
    if price <= 0 or equity <= 0 or fraction <= 0:
        return 0
    return max(0, int((equity * fraction) / price))


# ─────────────────────────── Stops ──────────────────────────────────────────

def orb_range_stop(
    side: str,
    entry_price: float,
    orb_high: float,
    orb_low: float,
    orb_range: float,
    sl_r: float = 1.0,
) -> float:
    """Stop gebunden an ORB-Range statt ATR.

    long:  stop = max(orb_low,  entry - sl_r * orb_range)
    short: stop = min(orb_high, entry + sl_r * orb_range)
    Fallback auf 0.5 * orb_range wenn Stop die falsche Seite des Entry trifft.
    """
    if side == "long":
        stop = max(orb_low, entry_price - sl_r * orb_range)
        if stop >= entry_price:
            stop = entry_price - 0.5 * orb_range
        return stop
    stop = min(orb_high, entry_price + sl_r * orb_range)
    if stop <= entry_price:
        stop = entry_price + 0.5 * orb_range
    return stop


def atr_stop(side: str, entry: float, atr_value: float, mult: float = 1.5) -> float:
    """Einfacher ATR-Stop (für Strategien, die ATR statt Range nutzen)."""
    if atr_value <= 0:
        return entry
    if side == "long":
        return entry - mult * atr_value
    return entry + mult * atr_value


def order_block_stop(
    side: str,
    ob_high: float,
    ob_low: float,
    mult: float = 0.75,
) -> float:
    """Stop-Loss außerhalb der Order-Block-Zone.

    long:  stop = ob_low  − mult × OB-Range
    short: stop = ob_high + mult × OB-Range
    """
    ob_range = ob_high - ob_low
    if ob_range <= 0:
        return ob_low if side == "long" else ob_high
    if side == "long":
        return ob_low - mult * ob_range
    return ob_high + mult * ob_range


def target_from_r(side: str, entry: float, stop: float, r_multiple: float) -> float:
    """Take-Profit-Level als Vielfaches der Entry-Stop-Distanz."""
    risk = abs(entry - stop)
    if side == "long":
        return entry + r_multiple * risk
    return entry - r_multiple * risk


# ─────────────────────────── Kelly / MIT Overlay ────────────────────────────

def kelly_fraction(win_prob: float, reward_r: float, risk_r: float = 1.0) -> float:
    """Kelly-Fraction f* = (p*b - q) / b mit b = reward/risk."""
    if reward_r <= 0 or risk_r <= 0:
        return 0.0
    b = reward_r / risk_r
    q = 1.0 - win_prob
    return max(0.0, ((b * win_prob) - q) / b)


def dynamic_kelly(base_kelly: float, current_dd: float,
                  max_dd: float = 0.15) -> float:
    """Kelly graduell mit Drawdown skalieren.

    current_dd >= max_dd -> 0
    sonst: base * max(0, 1 - (dd/max_dd)^1.5)
    """
    if current_dd >= max_dd:
        return 0.0
    scale = max(0.0, 1.0 - (current_dd / max_dd) ** 1.5)
    return base_kelly * scale


def expected_value_r(win_prob: float, reward_r: float, risk_r: float = 1.0) -> float:
    """Expected Value in R-Multiples."""
    return (win_prob * reward_r) - ((1.0 - win_prob) * risk_r)


def mit_estimate_win_probability(
    signal: str,
    strength: float,
    volume_ratio: float = 1.0,
    volume_confirmed: bool = False,
    orb_range_pct: float = 0.0,
    atr_pct: float = 0.0,
    trend_bullish: bool = True,
    trend_bearish: bool = True,
    calibration_offset: float = 0.0,
) -> float:
    """Heuristische Win-Probability-Schätzung (geclippt [0.20, 0.80]).

    Migriert 1:1 aus orb_strategy.mit_estimate_win_probability.
    """
    trend_aligned = (
        signal == "BUY" and trend_bullish
    ) or (
        signal == "SHORT" and trend_bearish
    )

    p = 0.40
    p += 0.25 * float(np.clip(strength, 0.0, 1.0))
    p += 0.04 * float(np.clip(volume_ratio - 1.0, 0.0, 1.5))
    if volume_confirmed:
        p += 0.03
    if 0.25 <= orb_range_pct <= 1.20:
        p += 0.03
    elif orb_range_pct > 2.00:
        p -= 0.04
    if atr_pct > 0 and orb_range_pct > 0:
        ratio = orb_range_pct / max(atr_pct, 1e-9)
        if 0.35 <= ratio <= 1.25:
            p += 0.03
        elif ratio > 1.75:
            p -= 0.05
    if trend_aligned:
        p += 0.03
    else:
        p -= 0.05
    return float(np.clip(p + calibration_offset, 0.20, 0.80))

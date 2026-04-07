#!/usr/bin/env python3
"""
orb_signals.py – ORB-Signalgenerierung für ORB Bot v2.

Berechnet Opening-Range-Levels aus 5-Minuten-Bars und erzeugt
Entry-Signale (Long + Short) für den Backtest und Live-Betrieb.

Signallogik basiert auf orb_bot_alpaca.py → ORBStrategy.generate_signal().

Nutzung:
    from orb_signals import calculate_orb_levels, compute_orb_signals, prepare_orb_day
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import pytz


# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------

ET = pytz.timezone("America/New_York")


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def _to_et(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Konvertiere beliebigen DatetimeIndex nach Eastern Time."""
    if idx.tz is None:
        return idx.tz_localize("UTC").tz_convert(ET)
    return idx.tz_convert(ET)


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range."""
    hl = df["High"] - df["Low"]
    hc = np.abs(df["High"] - df["Close"].shift())
    lc = np.abs(df["Low"] - df["Close"].shift())
    return pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(period).mean()


def compute_indicators(df: pd.DataFrame, volume_sma: int = 20) -> pd.DataFrame:
    """Berechne ATR + Volume MA auf 5m-Bars."""
    df = df.copy()
    df["ATR"] = calculate_atr(df)
    df["Volume_MA"] = df["Volume"].rolling(volume_sma).mean()
    df["Volume_Ratio"] = df["Volume"] / df["Volume_MA"].replace(0, np.nan)
    return df


# ---------------------------------------------------------------------------
# ORB-Levels
# ---------------------------------------------------------------------------

def calculate_orb_levels(
    day_df: pd.DataFrame,
    orb_minutes: int = 30,
) -> Tuple[float, float, float]:
    """
    Berechne Opening-Range-Levels aus 5-Minuten-Bars eines Tages.

    Parameter
    ---------
    day_df      : DataFrame mit 5m-Bars eines einzigen Handelstages
    orb_minutes : Dauer der Opening Range in Minuten (9:30 – 9:30+orb_minutes)

    Rückgabe
    --------
    (orb_high, orb_low, orb_range)
    """
    if day_df.empty or len(day_df) < 2:
        return 0.0, 0.0, 0.0

    idx_et = _to_et(day_df.index)
    hhmm = idx_et.hour * 60 + idx_et.minute   # Minuten ab Mitternacht

    orb_start = 9 * 60 + 30   # 9:30
    orb_end = orb_start + orb_minutes

    orb_mask = (hhmm >= orb_start) & (hhmm < orb_end)
    orb_bars = day_df[np.asarray(orb_mask, dtype=bool)]

    if len(orb_bars) < 2:
        return 0.0, 0.0, 0.0

    orb_high = float(orb_bars["High"].max())
    orb_low = float(orb_bars["Low"].min())
    orb_range = orb_high - orb_low

    return orb_high, orb_low, orb_range


# ---------------------------------------------------------------------------
# Signal-Generierung
# ---------------------------------------------------------------------------

def compute_orb_signals(
    day_df: pd.DataFrame,
    orb_high: float,
    orb_low: float,
    orb_range: float,
    cfg: dict,
) -> pd.DataFrame:
    """
    Erzeuge Entry-Signale für einen ganzen Handelstag (post-ORB-Bars).

    Rückgabe-DataFrame (gleicher Index wie day_df) mit Spalten:
      entry_long    – bool: Long-Breakout-Signal
      entry_short   – bool: Short-Breakdown-Signal
      strength      – float 0.0–1.0: Signal-Stärke
      volume_ok     – bool: Volume-Bestätigung
      side          – str: "long" | "short" | ""
    """
    out = pd.DataFrame(index=day_df.index)
    n = len(day_df)

    out["entry_long"] = False
    out["entry_short"] = False
    out["strength"] = 0.0
    out["volume_ok"] = False
    out["side"] = ""

    if orb_range <= 0 or n < 2:
        return out

    idx_et = _to_et(day_df.index)
    hhmm = idx_et.hour * 60 + idx_et.minute

    orb_end_min = 9 * 60 + 30 + int(cfg.get("opening_range_minutes", 30))
    post_orb = np.asarray(hhmm >= orb_end_min, dtype=bool)

    close = day_df["Close"]
    vol_mult = float(cfg.get("volume_multiplier", 1.3))
    min_strength = float(cfg.get("min_signal_strength", 0.3))
    allow_shorts = bool(cfg.get("allow_shorts", False))
    breakout_mult = float(cfg.get("orb_breakout_multiplier", 1.0))

    # Volume check
    if "Volume_Ratio" in day_df.columns:
        vol_ok = day_df["Volume_Ratio"] >= vol_mult
    elif "Volume_MA" in day_df.columns:
        vol_ok = day_df["Volume"] >= vol_mult * day_df["Volume_MA"]
    else:
        vol_ok = pd.Series(True, index=day_df.index)

    out["volume_ok"] = vol_ok.values

    # Breakout threshold (multiplier on ORB range)
    breakout_level = orb_high + (breakout_mult - 1.0) * orb_range
    breakdown_level = orb_low - (breakout_mult - 1.0) * orb_range

    # Long Breakout
    long_break = (close > breakout_level) & post_orb
    long_strength = ((close - orb_high) / orb_range).clip(0.0, 1.0)
    # Volume bonus (+20%, capped at 1.0)
    long_strength = np.where(vol_ok, np.minimum(long_strength * 1.2, 1.0), long_strength)
    long_valid = long_break & (long_strength >= min_strength)

    out.loc[long_valid, "entry_long"] = True
    out["strength"] = np.where(long_break, long_strength, out["strength"])
    out.loc[long_valid, "side"] = "long"

    # Short Breakdown
    if allow_shorts:
        short_break = (close < breakdown_level) & post_orb
        short_strength = ((orb_low - close) / orb_range).clip(0.0, 1.0)
        short_strength = np.where(vol_ok, np.minimum(short_strength * 1.2, 1.0), short_strength)
        short_valid = short_break & (short_strength >= min_strength)

        out.loc[short_valid, "entry_short"] = True
        out["strength"] = np.where(
            short_break & ~long_break,
            short_strength,
            out["strength"],
        )
        out.loc[short_valid & ~long_valid, "side"] = "short"

    return out


# ---------------------------------------------------------------------------
# Convenience: Tages-Aufbereitung
# ---------------------------------------------------------------------------

def prepare_orb_day(
    day_5m_df: pd.DataFrame,
    cfg: dict,
) -> Optional[Dict]:
    """
    Berechne ORB-Levels und Signale für einen Handelstag.

    Rückgabe
    --------
    Dict mit Keys: orb_high, orb_low, orb_range, signals_df
    oder None wenn ungenügend Daten.
    """
    if day_5m_df.empty or len(day_5m_df) < 8:
        return None

    orb_minutes = int(cfg.get("opening_range_minutes", 30))
    orb_high, orb_low, orb_range = calculate_orb_levels(day_5m_df, orb_minutes)

    if orb_range <= 0:
        return None

    signals = compute_orb_signals(day_5m_df, orb_high, orb_low, orb_range, cfg)

    return {
        "orb_high": orb_high,
        "orb_low": orb_low,
        "orb_range": orb_range,
        "signals_df": signals,
    }

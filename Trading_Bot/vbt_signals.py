#!/usr/bin/env python3
"""
vbt_signals.py – Vektorisierte Indikator- und Signal-Berechnungen für trader_v7.

Signallogik exakt identisch zu trader_v6.py → Strategy.signal_row().
Vollständig vektorisiert (keine Zeilen-Schleifen) für vectorbt-Kompatibilität.

Alle Signalpfade:
  gc        – Golden Cross  (SMA20 kreuzt SMA50 von unten)
  fc        – Fast Cross    (EMA9 kreuzt EMA21 von unten, konfigurierbar)
  early_gc  – Early Golden Cross (SMA20 < SMA50, aber Annäherung + MACD + RSI)
  pullback  – Pullback-to-SMA20/EMA nach einem Cross
  orb_proxy – ORB-Proxy: bullische Kerze + Vol-Surge nach Cross
  mr        – Mean Reversion / Bollinger-Band-Unterbrechung
  death     – Death Cross (Exit-Signal)
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Hilfsfunktionen (intern)
# ---------------------------------------------------------------------------

def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    hl = high - low
    hc = (high - close.shift()).abs()
    lc = (low - close.shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    up   = high.diff()
    down = low.diff()
    pdm  = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=high.index)
    mdm  = pd.Series(np.where((down > up) & (down > 0), -down, 0.0), index=high.index)
    atr  = _atr(high, low, close, period)
    pdi  = 100 * pdm.rolling(period, min_periods=1).mean() / atr.replace(0, np.nan)
    mdi  = 100 * mdm.rolling(period, min_periods=1).mean() / atr.replace(0, np.nan)
    dx   = (np.abs(pdi - mdi) / (pdi + mdi + 1e-9)) * 100
    return dx.rolling(period, min_periods=1).mean()


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    return 100 - (100 / (1 + gain / loss.replace(0, np.nan)))


# ---------------------------------------------------------------------------
# Public: Indikator-Berechnung
# ---------------------------------------------------------------------------

def compute_indicators(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Berechne alle technischen Indikatoren die für die Signalgenerierung benötigt werden.

    Parameter
    ---------
    df  : DataFrame mit Spalten Open, High, Low, Close, Volume
    cfg : Konfigurations-Dict (aus CONFIG in trader_v6.py)

    Rückgabe
    --------
    DataFrame mit allen Originalpalten + berechneten Indikatoren.
    """
    df = df.copy()

    # --- Trend MAs -----------------------------------------------------------
    df["SMA20"] = df["Close"].rolling(cfg["sma_short"]).mean()
    df["SMA50"] = df["Close"].rolling(cfg["sma_long"]).mean()

    # --- Volatilität & Trend-Stärke -----------------------------------------
    df["ATR"] = _atr(df["High"], df["Low"], df["Close"], cfg["atr_period"])
    df["ADX"] = _adx(df["High"], df["Low"], df["Close"], cfg["adx_period"])

    # --- RSI -----------------------------------------------------------------
    df["RSI"] = _rsi(df["Close"], 14)

    # --- MACD ----------------------------------------------------------------
    ema_f = df["Close"].ewm(span=cfg["macd_fast"], adjust=False).mean()
    ema_s = df["Close"].ewm(span=cfg["macd_slow"], adjust=False).mean()
    df["MACD"]        = ema_f - ema_s
    df["MACD_signal"] = df["MACD"].ewm(span=cfg["macd_signal_period"], adjust=False).mean()
    df["MACD_hist"]   = df["MACD"] - df["MACD_signal"]

    # --- Bollinger Bands -----------------------------------------------------
    bb_sma = df["Close"].rolling(cfg["bb_period"]).mean()
    bb_std = df["Close"].rolling(cfg["bb_period"]).std()
    df["BB_upper"] = bb_sma + cfg["bb_std"] * bb_std
    df["BB_lower"] = bb_sma - cfg["bb_std"] * bb_std
    df["BB_mid"]   = bb_sma

    # --- Volume SMA ----------------------------------------------------------
    df["Vol_SMA"] = df["Volume"].rolling(cfg["volume_sma_period"]).mean()

    # --- Fast Cross MAs (EMA9/21 oder SMA10/30) ------------------------------
    if cfg.get("use_fast_cross", False):
        ft = cfg.get("fast_cross_type", "EMA")
        fs = cfg.get("fast_cross_short", 9)
        fl = cfg.get("fast_cross_long", 21)
        if ft == "EMA":
            df["FC_FAST"] = df["Close"].ewm(span=fs, adjust=False).mean()
            df["FC_SLOW"] = df["Close"].ewm(span=fl, adjust=False).mean()
        else:
            df["FC_FAST"] = df["Close"].rolling(fs).mean()
            df["FC_SLOW"] = df["Close"].rolling(fl).mean()

    # --- EMA für Pullback-Entry ---------------------------------------------
    pb_ema = cfg.get("pullback_daily_ema", 20)
    df[f"EMA{pb_ema}"] = df["Close"].ewm(span=pb_ema, adjust=False).mean()

    # --- EMA Proxy für MTF-Simulation ---------------------------------------
    mtf_ema = cfg.get("lower_ema_period", 20)
    if f"EMA{mtf_ema}" not in df.columns:
        df[f"EMA{mtf_ema}"] = df["Close"].ewm(span=mtf_ema, adjust=False).mean()

    return df


# ---------------------------------------------------------------------------
# Public: Vektorisierte Cross-Erkennung
# ---------------------------------------------------------------------------

def recent_cross_series(
    sma20: pd.Series,
    sma50: pd.Series,
    fc_fast: Optional[pd.Series],
    fc_slow: Optional[pd.Series],
    lookback: int,
) -> pd.Series:
    """
    Gibt eine bool-Series zurück: True an Index-Position i wenn in den
    letzten `lookback` Bars ein Golden Cross oder Fast Cross stattfand.

    Implementierung: für jeden Bar prüfen ob in der Rolling-Lookback-Fensterfläche
    ein Kreuzen von unten nach oben vorhanden ist.
    """
    # Golden Cross in Lookback-Fenster
    gc_today = (sma20.shift(1) <= sma50.shift(1)) & (sma20 > sma50)
    # Rolling: gab es einen GC in den letzten `lookback` Bars?
    gc_recent = gc_today.rolling(lookback, min_periods=1).max().astype(bool)

    if fc_fast is not None and fc_slow is not None:
        fc_today  = (fc_fast.shift(1) <= fc_slow.shift(1)) & (fc_fast > fc_slow)
        fc_recent = fc_today.rolling(lookback, min_periods=1).max().astype(bool)
        return gc_recent | fc_recent

    return gc_recent


# ---------------------------------------------------------------------------
# Public: Entry- und Exit-Signale
# ---------------------------------------------------------------------------

def compute_signals(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Berechne alle Entry- und Exit-Signale vektorisiert.

    Rückgabe-DataFrame hat Spalten:
      entry        – bool: irgendein BUY-Signal
      exit         – bool: Death Cross oder MR-Exit
      signal_type  – str: welcher Pfad hat getriggert (für Tracing)
      is_mr        – bool: Mean-Reversion Entry (für separate MR-Exit-Logik)
    """
    out = pd.DataFrame(index=df.index)

    close  = df["Close"]
    high   = df["High"]
    low    = df["Low"]
    volume = df["Volume"]
    sma20  = df["SMA20"]
    sma50  = df["SMA50"]
    rsi    = df["RSI"]
    macd_h = df["MACD_hist"]
    vol_sma = df["Vol_SMA"]
    bb_low  = df["BB_lower"]
    bb_mid  = df["BB_mid"]
    atr     = df["ATR"]
    adx     = df["ADX"]

    prev_close  = close.shift(1)
    prev_open   = df["Open"].shift(1)
    prev_sma20  = sma20.shift(1)
    prev_sma50  = sma50.shift(1)
    prev_macd_h = macd_h.shift(1)

    fc_fast = df.get("FC_FAST")
    fc_slow = df.get("FC_SLOW")

    # --- Gemeinsame Hilfsvariablen ------------------------------------------
    uptrend      = sma20 > sma50
    adx_ok       = adx.isna() | (adx >= cfg["adx_threshold"])
    vol_ok       = volume > vol_sma
    macd_pos     = macd_h > 0
    macd_rising  = macd_h > prev_macd_h
    rsi_in_range = (rsi >= cfg["rsi_buy_min"]) & (rsi <= cfg["rsi_buy_max"])

    # --- Death Cross (Exit) -------------------------------------------------
    death_cross = (prev_sma20 >= prev_sma50) & (sma20 < sma50)
    out["exit_death"] = death_cross.fillna(False)

    # --- MR Exit: Close >= BB_mid (nur für MR-Positionen) ------------------
    out["exit_mr"] = (close >= bb_mid).fillna(False)

    # ------------------------------------------------------------------ ENTRIES

    # --- 1. Golden Cross ---------------------------------------------------
    gc_cross = (prev_sma20 <= prev_sma50) & (sma20 > sma50)
    if cfg.get("use_rsi_filter", True):
        gc_rsi = rsi_in_range
    else:
        gc_rsi = pd.Series(True, index=df.index)
    if cfg.get("use_macd_filter", True):
        gc_macd = macd_pos
    else:
        gc_macd = pd.Series(True, index=df.index)
    if cfg.get("use_volume_filter", True):
        gc_vol = vol_ok
    else:
        gc_vol = pd.Series(True, index=df.index)
    sig_gc = gc_cross & gc_rsi & gc_macd & gc_vol & adx_ok

    # --- 2. Fast Cross (EMA9/21 o.ä.) ---------------------------------------
    sig_fc = pd.Series(False, index=df.index)
    if cfg.get("use_fast_cross", False) and fc_fast is not None and fc_slow is not None:
        prev_ff = fc_fast.shift(1)
        prev_fs = fc_slow.shift(1)
        fc_cross = (prev_ff <= prev_fs) & (fc_fast > fc_slow)
        sig_fc = fc_cross & rsi_in_range & macd_pos & (vol_ok if cfg.get("use_volume_filter", True) else pd.Series(True, index=df.index))

    # --- 3. Early Golden Cross ---------------------------------------------
    sig_early_gc = pd.Series(False, index=df.index)
    if cfg.get("use_early_golden_cross", False):
        prox     = cfg.get("early_gc_proximity_pct", 0.02)
        rsi_min  = cfg.get("early_gc_rsi_min", 55)
        sma_dist = (sma50 - sma20) / (sma50 + 1e-9)
        approaching = (sma20 < sma50) & (sma_dist <= prox)
        converging  = (sma20 - prev_sma20) > (sma50 - prev_sma50)
        early_rsi   = rsi >= rsi_min
        macd_strong = macd_pos & macd_rising
        if cfg.get("use_volume_filter", True):
            e_vol = vol_ok
        else:
            e_vol = pd.Series(True, index=df.index)
        sig_early_gc = approaching & converging & early_rsi & macd_strong & e_vol

    # --- 4. Pullback-Entry nach Cross ---------------------------------------
    sig_pullback = pd.Series(False, index=df.index)
    if cfg.get("use_pullback_entry_daily", False):
        lookback = cfg.get("pullback_daily_lookback", 15)
        prox     = cfg.get("pullback_daily_proximity", 0.015)
        pb_ema_p = cfg.get("pullback_daily_ema", 20)
        ema_col  = f"EMA{pb_ema_p}"

        rc = recent_cross_series(sma20, sma50, fc_fast, fc_slow, lookback)

        # Low touchiert SMA20 oder EMA von unten mit Toleranz
        touched_sma = low <= sma20 * (1 + prox)
        ema_vals    = df.get(ema_col, sma20)  # Fallback auf SMA20
        touched_ema = low <= ema_vals * (1 + prox)
        touched     = touched_sma | touched_ema

        # Bullische Umkehrkerze oder Erholung
        bullish = (close > df["Open"]) | (close > prev_close)

        pb_rsi = rsi >= cfg.get("pullback_daily_rsi_min", 50)
        if cfg.get("use_macd_filter", True):
            pb_macd = macd_pos
        else:
            pb_macd = pd.Series(True, index=df.index)
        if cfg.get("use_volume_filter", True):
            pb_vol = vol_ok
        else:
            pb_vol = pd.Series(True, index=df.index)

        sig_pullback = rc & uptrend & touched & bullish & pb_rsi & pb_macd & pb_vol

    # --- 5. ORB-Proxy (Daily) -----------------------------------------------
    sig_orb = pd.Series(False, index=df.index)
    if cfg.get("use_orb", False):
        lookback  = cfg.get("orb_lookback_bars", 3)
        min_body  = cfg.get("orb_min_body_pct",  0.003)
        surge_vol = cfg.get("orb_surge_volume",  1.3)
        rc_orb    = recent_cross_series(sma20, sma50, fc_fast, fc_slow, lookback)

        # Nicht am selben Tag wie GC (den übernimmt sig_gc)
        not_gc_today = ~gc_cross

        body_pct  = (close - df["Open"]) / (df["Open"] + 1e-9)
        strong_candle = body_pct >= min_body
        vol_surge = volume >= surge_vol * vol_sma
        orb_rsi   = rsi >= cfg.get("rsi_buy_min", 40)

        sig_orb = rc_orb & not_gc_today & uptrend & strong_candle & vol_surge & orb_rsi

    # --- 6. Mean Reversion (Bollinger unteres Band) -------------------------
    sig_mr = pd.Series(False, index=df.index)
    if cfg.get("use_mean_reversion", False):
        bb_touch = close <= bb_low
        mr_rsi   = rsi < cfg["mr_rsi_max"]
        mr_vol   = vol_ok
        sig_mr   = bb_touch & mr_rsi & mr_vol

    # --- Kombiniertes Entry-Signal ------------------------------------------
    # Priorität: MR → Fast Cross → Early GC → Pullback → ORB → GC
    # Death Cross überstimmt immer alles (kein Entry wenn Death Cross heute)
    no_death = ~death_cross.fillna(False)

    entry = (sig_gc | sig_fc | sig_early_gc | sig_pullback | sig_orb | sig_mr) & no_death

    # Signal-Typ-Label (für Analyse; letzter Truthy-Typ gewinnt)
    signal_type = pd.Series("", index=df.index)
    for mask, label in [
        (sig_gc,       "gc"),
        (sig_fc,       "fc"),
        (sig_early_gc, "early_gc"),
        (sig_pullback, "pullback"),
        (sig_orb,      "orb_proxy"),
        (sig_mr,       "mr"),
    ]:
        signal_type = signal_type.where(~mask.fillna(False), label)

    out["sig_gc"]       = sig_gc.fillna(False)
    out["sig_fc"]       = sig_fc.fillna(False)
    out["sig_early_gc"] = sig_early_gc.fillna(False)
    out["sig_pullback"] = sig_pullback.fillna(False)
    out["sig_orb"]      = sig_orb.fillna(False)
    out["sig_mr"]       = sig_mr.fillna(False)
    out["entry"]        = entry.fillna(False)
    out["exit"]         = out["exit_death"]
    out["is_mr"]        = sig_mr.fillna(False)
    out["signal_type"]  = signal_type

    return out


# ---------------------------------------------------------------------------
# Public: Batch-Verarbeitung für mehrere Symbole
# ---------------------------------------------------------------------------

def prepare_all(
    data_dict: Dict[str, pd.DataFrame],
    cfg: dict,
) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Berechne Indikatoren und Signale für alle Symbole.

    Rückgabe
    --------
    { symbol: (indicators_df, signals_df) }
    """
    result: Dict[str, Tuple[pd.DataFrame, pd.DataFrame]] = {}
    min_bars = cfg["sma_long"] + 30

    for sym, raw_df in data_dict.items():
        if len(raw_df) < min_bars:
            print(f"  [vbt_signals] {sym}: zu wenige Bars ({len(raw_df)}) → übersprungen")
            continue
        ind = compute_indicators(raw_df, cfg)
        sig = compute_signals(ind, cfg)
        result[sym] = (ind, sig)

    return result

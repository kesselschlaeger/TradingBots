"""Reine Indikatoren-Funktionen. Keine I/O, keine Seiteneffekte."""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
import pytz

ET = pytz.timezone("America/New_York")


def _ensure_et_index(df: pd.DataFrame) -> pd.DatetimeIndex:
    idx = pd.to_datetime(df.index)
    if idx.tz is None:
        return idx.tz_localize("UTC").tz_convert(ET)
    return idx.tz_convert(ET)


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range.

    Erwartet Spalten High, Low, Close.
    """
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    hl = high - low
    hc = (high - close.shift()).abs()
    lc = (low - close.shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    """Simple Moving Average."""
    return series.rolling(period).mean()


def ema(series: pd.Series, period: int) -> pd.Series:
    """Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    return 100 - (100 / (1 + gain / loss.replace(0, np.nan)))


def adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average Directional Index. Erwartet Spalten High, Low, Close."""
    up = df["High"].diff()
    down = df["Low"].diff()
    pdm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    mdm = pd.Series(np.where((down > up) & (down > 0), -down, 0.0), index=df.index)
    tr = atr(df, period)
    pdi = 100 * pdm.rolling(period, min_periods=1).mean() / tr.replace(0, np.nan)
    mdi = 100 * mdm.rolling(period, min_periods=1).mean() / tr.replace(0, np.nan)
    dx = (np.abs(pdi - mdi) / (pdi + mdi + 1e-9)) * 100
    return dx.rolling(period, min_periods=1).mean()


def macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """MACD. Returns (macd_line, signal_line, histogram)."""
    ema_f = series.ewm(span=fast, adjust=False).mean()
    ema_s = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_f - ema_s
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def bollinger_bands(
    series: pd.Series,
    period: int = 20,
    std: float = 2.0,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Bollinger Bands. Returns (upper, mid, lower)."""
    mid = series.rolling(period).mean()
    band = series.rolling(period).std() * std
    return mid + band, mid, mid - band


def vwap(df: pd.DataFrame) -> pd.Series:
    """Volume-Weighted Average Price (intraday cumulative)."""
    typical = (df["High"] + df["Low"] + df["Close"]) / 3.0
    cum_vol = df["Volume"].cumsum().replace(0, np.nan)
    return (typical * df["Volume"]).cumsum() / cum_vol


def volume_time_of_day_ma(df: pd.DataFrame, lookback: int = 20) -> pd.Series:
    """Tageszeitspezifische Volume-Moving-Average.

    Vergleicht 10:00-Volumen gegen frühere 10:00-Bars, nicht mit
    mittäglicher Flaute vermischt. Fallback auf global rolling, bis
    pro Time-of-Day-Bucket genug Bars vorhanden sind.
    """
    idx_et = _ensure_et_index(df)
    tod = (idx_et.hour * 100 + idx_et.minute).astype(int)

    work = pd.DataFrame({"Volume": df["Volume"].values, "_tod": np.asarray(tod)},
                        index=df.index)
    min_global = max(3, lookback // 4)
    global_ma = work["Volume"].rolling(lookback, min_periods=min_global).mean()
    tod_ma = work.groupby("_tod", sort=False)["Volume"].transform(
        lambda s: s.rolling(lookback, min_periods=3).mean()
    )
    return tod_ma.fillna(global_ma)


def compute_indicator_frame(df: pd.DataFrame,
                            atr_period: int = 14,
                            volume_lookback: int = 20) -> pd.DataFrame:
    """Fügt ATR, Volume_MA, Volume_Ratio zu OHLCV-Frame hinzu."""
    out = df.copy()
    out["ATR"] = atr(out, atr_period)
    vol_ma = volume_time_of_day_ma(out, volume_lookback)
    out["Volume_MA"] = vol_ma
    out["Volume_Ratio"] = out["Volume"] / vol_ma.replace(0, np.nan)
    return out


def rolling_high_low(
    df: pd.DataFrame,
    lookback: int,
    exclude_current: bool = True,
) -> tuple[pd.Series, pd.Series]:
    """Rolling High/Low über die letzten N Bars.

    Mit exclude_current=True (Standard) wird der aktuelle Bar durch
    shift(1) ausgeschlossen -> kein Look-Ahead-Bias (wichtig für OBB).
    """
    src_high = df["High"].shift(1) if exclude_current else df["High"]
    src_low = df["Low"].shift(1) if exclude_current else df["Low"]
    hi = src_high.rolling(lookback, min_periods=lookback).max()
    lo = src_low.rolling(lookback, min_periods=lookback).min()
    return hi, lo


def opening_range_levels(day_df: pd.DataFrame,
                         orb_minutes: int = 30) -> tuple[float, float, float]:
    """Berechne (orb_high, orb_low, orb_range) aus Intraday-Bars eines Tages.

    Erwartet 5m-Bars mit DatetimeIndex. Gibt (0,0,0) zurück, wenn zu wenig
    Bars im ORB-Fenster.
    """
    if day_df.empty or len(day_df) < 2:
        return 0.0, 0.0, 0.0

    idx_et = _ensure_et_index(day_df)
    hhmm = idx_et.hour * 60 + idx_et.minute
    orb_start = 9 * 60 + 30
    orb_end = orb_start + orb_minutes

    mask = np.asarray((hhmm >= orb_start) & (hhmm < orb_end), dtype=bool)
    orb_bars = day_df[mask]
    if len(orb_bars) < 2:
        return 0.0, 0.0, 0.0

    hi = float(orb_bars["High"].max())
    lo = float(orb_bars["Low"].min())
    return hi, lo, hi - lo


def orb_volume_ratio(
    day_df: pd.DataFrame,
    historical_dfs: Optional[list[pd.DataFrame]] = None,
    orb_minutes: int = 30,
) -> float:
    """Tageszeitspezifische Volume-Ratio für das ORB-Fenster (9:30–10:00 ET).

    Vergleicht das ORB-Kumulativvolumen des aktuellen Tages gegen den
    Durchschnitt der gleichen Zeitfenster der letzten N Tage.
    """
    if day_df.empty or "Volume" not in day_df.columns:
        return 1.0

    idx_et = _ensure_et_index(day_df)
    hhmm = idx_et.hour * 60 + idx_et.minute
    start = 9 * 60 + 30
    end = start + orb_minutes
    mask = np.asarray((hhmm >= start) & (hhmm < end), dtype=bool)
    today = day_df[mask]
    if today.empty:
        return 1.0

    today_vol = float(today["Volume"].sum())

    if not historical_dfs:
        if "Volume_MA" in day_df.columns:
            last_ma = day_df["Volume_MA"].dropna()
            if not last_ma.empty:
                avg_bar = float(last_ma.iloc[-1])
                if avg_bar > 0:
                    return today_vol / (avg_bar * max(len(today), 1))
        return 1.0

    hist_vols: list[float] = []
    for hdf in historical_dfs:
        if hdf.empty or "Volume" not in hdf.columns:
            continue
        h_idx_et = _ensure_et_index(hdf)
        h_hhmm = h_idx_et.hour * 60 + h_idx_et.minute
        h_mask = np.asarray((h_hhmm >= start) & (h_hhmm < end), dtype=bool)
        h_orb = hdf[h_mask]
        if not h_orb.empty:
            hist_vols.append(float(h_orb["Volume"].sum()))
    if not hist_vols:
        return 1.0
    avg = float(np.mean(hist_vols))
    return today_vol / avg if avg > 0 else 1.0

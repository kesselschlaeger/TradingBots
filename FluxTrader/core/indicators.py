"""Reine Indikatoren-Funktionen. Keine I/O, keine Seiteneffekte.

Ausnahme: KalmanSpreadEstimator hat internen State (eine Instanz pro
Pair-Strategie). Lebt hier, weil es ein Indikator ist.
"""
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


# ─────────────────────────── OHLCV Resampling ───────────────────────────────

_TF_MAP = {
    "1M": "1min", "5M": "5min", "15M": "15min",
    "30M": "30min", "1H": "1h", "4H": "4h", "1D": "1D",
}


def resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resample OHLCV DataFrame to a higher timeframe.

    Accepts timeframe strings like '15M', '1H', '4H'.
    Expects columns: Open, High, Low, Close, Volume.
    """
    freq = _TF_MAP.get(timeframe.upper(), timeframe)
    resampled = df.resample(freq).agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }).dropna(subset=["Open"])
    return resampled


def ensure_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Stelle sicher, dass ein OHLCV-DataFrame Daily-Bars enthält.

    Intraday-Frames werden zu Daily aggregiert; bereits-Daily-Frames
    bleiben idempotent. Nötig z.B. für den SPY-Trend-Filter, dessen
    EMA-Periode (z.B. 20) sich auf Handelstage bezieht.
    """
    if df is None or df.empty:
        return df
    if not isinstance(df.index, pd.DatetimeIndex):
        return df
    return resample_ohlcv(df, "1D")


# ─────────────────────────── ICT / Smart Money Helpers ───────────────────────


def swing_highs(high: pd.Series, lookback: int = 5) -> pd.Series:
    """Boolean series marking swing highs (local maxima over 2*lookback+1)."""
    return high == high.rolling(2 * lookback + 1, center=True).max()


def swing_lows(low: pd.Series, lookback: int = 5) -> pd.Series:
    """Boolean series marking swing lows (local minima over 2*lookback+1)."""
    return low == low.rolling(2 * lookback + 1, center=True).min()


def fair_value_gaps(df: pd.DataFrame) -> list[dict]:
    """Detect Fair Value Gaps (3-candle imbalance).

    Bullish FVG: candle[i+1].Low > candle[i-1].High (gap up).
    Bearish FVG: candle[i+1].High < candle[i-1].Low (gap down).
    """
    gaps: list[dict] = []
    if len(df) < 3:
        return gaps

    highs = df["High"].values
    lows = df["Low"].values
    timestamps = df.index

    for i in range(1, len(df) - 1):
        if lows[i + 1] > highs[i - 1]:
            gaps.append({
                "type": "bullish",
                "high": float(lows[i + 1]),
                "low": float(highs[i - 1]),
                "timestamp": timestamps[i],
                "idx": i,
            })
        if highs[i + 1] < lows[i - 1]:
            gaps.append({
                "type": "bearish",
                "high": float(lows[i - 1]),
                "low": float(highs[i + 1]),
                "timestamp": timestamps[i],
                "idx": i,
            })
    return gaps


def detect_order_blocks(
    df: pd.DataFrame,
    atr_period: int = 14,
    displacement_mult: float = 1.8,
    swing_lookback: int = 5,
) -> list[dict]:
    """Detect valid ICT Order Blocks.

    A valid OB requires all three conditions:
    1. Liquidity sweep – price took out a prior swing point.
    2. Displacement – impulsive candle body > displacement_mult × ATR.
    3. Imbalance/FVG – 3-candle gap left behind.

    Bullish OB: last bearish candle before strong upward displacement
    after sweeping a swing low.
    Bearish OB: last bullish candle before strong downward displacement
    after sweeping a swing high.

    Returns list of dicts: type, high, low, mid, timestamp, idx,
    displacement_strength.
    """
    n = len(df)
    min_required = max(swing_lookback * 2 + 3, atr_period + 2)
    if n < min_required:
        return []

    blocks: list[dict] = []
    atr_vals = atr(df, atr_period)

    highs = df["High"].values
    lows = df["Low"].values
    opens = df["Open"].values
    closes = df["Close"].values
    timestamps = df.index

    sw_hi = swing_highs(df["High"], swing_lookback)
    sw_lo = swing_lows(df["Low"], swing_lookback)

    swing_high_levels = [(i, highs[i]) for i in range(n) if sw_hi.iloc[i]]
    swing_low_levels = [(i, lows[i]) for i in range(n) if sw_lo.iloc[i]]

    # Pre-compute FVG indices (middle candle of 3-candle gap)
    fvg_set: set[int] = set()
    for i in range(1, n - 1):
        if lows[i + 1] > highs[i - 1]:
            fvg_set.add(i)
        if highs[i + 1] < lows[i - 1]:
            fvg_set.add(i)

    for i in range(min_required, n):
        atr_val = atr_vals.iloc[i]
        if pd.isna(atr_val) or atr_val <= 0:
            continue

        body = closes[i] - opens[i]
        if abs(body) <= displacement_mult * atr_val:
            continue

        # FVG near displacement (displacement candle ± 1)
        has_fvg = (i in fvg_set
                   or (i - 1) in fvg_set
                   or (i + 1 < n and (i + 1) in fvg_set))
        if not has_fvg:
            continue

        search_start = max(0, i - swing_lookback * 4)

        if body > 0:
            # ── Bullish displacement ──
            last_sl = None
            for si, sl in reversed(swing_low_levels):
                if si < i:
                    last_sl = (si, sl)
                    break
            if last_sl is None:
                continue

            swept = any(
                lows[j] < last_sl[1]
                for j in range(last_sl[0] + 1, i + 1)
            )
            if not swept:
                continue

            # OB = last bearish candle before displacement
            ob_idx = None
            for k in range(i - 1, max(search_start - 1, -1), -1):
                if closes[k] < opens[k]:
                    ob_idx = k
                    break
            if ob_idx is not None:
                blocks.append({
                    "type": "bullish",
                    "high": float(opens[ob_idx]),
                    "low": float(closes[ob_idx]),
                    "mid": float((opens[ob_idx] + closes[ob_idx]) / 2),
                    "timestamp": timestamps[ob_idx],
                    "idx": ob_idx,
                    "displacement_strength": float(abs(body) / atr_val),
                })

        else:
            # ── Bearish displacement ──
            last_sh = None
            for si, sh in reversed(swing_high_levels):
                if si < i:
                    last_sh = (si, sh)
                    break
            if last_sh is None:
                continue

            swept = any(
                highs[j] > last_sh[1]
                for j in range(last_sh[0] + 1, i + 1)
            )
            if not swept:
                continue

            # OB = last bullish candle before displacement
            ob_idx = None
            for k in range(i - 1, max(search_start - 1, -1), -1):
                if closes[k] > opens[k]:
                    ob_idx = k
                    break
            if ob_idx is not None:
                blocks.append({
                    "type": "bearish",
                    "high": float(closes[ob_idx]),
                    "low": float(opens[ob_idx]),
                    "mid": float((opens[ob_idx] + closes[ob_idx]) / 2),
                    "timestamp": timestamps[ob_idx],
                    "idx": ob_idx,
                    "displacement_strength": float(abs(body) / atr_val),
                })

    return blocks


def detect_structure_break(df: pd.DataFrame, lookback: int = 5) -> dict:
    """Detect the most recent Break of Structure / Change of Character.

    BOS Bullish:  price breaks above prior swing high in an uptrend.
    BOS Bearish:  price breaks below prior swing low in a downtrend.
    CHOCH Bullish: break above swing high after a downtrend (reversal).
    CHOCH Bearish: break below swing low after an uptrend (reversal).
    Higher Low / Lower High: structural confirmation without BOS.

    Returns dict with keys: type, level, timestamp.
    """
    _none = {"type": "none", "level": 0.0, "timestamp": None}
    if len(df) < lookback * 4:
        return _none

    sw_hi = swing_highs(df["High"], lookback)
    sw_lo = swing_lows(df["Low"], lookback)

    highs = df["High"].values
    lows = df["Low"].values
    closes = df["Close"].values
    timestamps = df.index

    recent_sh = [(i, highs[i]) for i in range(len(df)) if sw_hi.iloc[i]]
    recent_sl = [(i, lows[i]) for i in range(len(df)) if sw_lo.iloc[i]]

    if len(recent_sh) < 2 or len(recent_sl) < 2:
        return _none

    last_sh = recent_sh[-1][1]
    prev_sh = recent_sh[-2][1]
    last_sl = recent_sl[-1][1]
    prev_sl = recent_sl[-2][1]

    prior_uptrend = last_sh > prev_sh and last_sl > prev_sl
    prior_downtrend = last_sh < prev_sh and last_sl < prev_sl

    current_close = closes[-1]

    if current_close > last_sh:
        if prior_uptrend:
            return {"type": "bos_bullish", "level": float(last_sh),
                    "timestamp": timestamps[-1]}
        return {"type": "choch_bullish", "level": float(last_sh),
                "timestamp": timestamps[-1]}

    if current_close < last_sl:
        if prior_downtrend:
            return {"type": "bos_bearish", "level": float(last_sl),
                    "timestamp": timestamps[-1]}
        return {"type": "choch_bearish", "level": float(last_sl),
                "timestamp": timestamps[-1]}

    # Structural HL / LH (no breakout yet)
    if last_sl > prev_sl:
        return {"type": "higher_low", "level": float(last_sl),
                "timestamp": timestamps[recent_sl[-1][0]]}
    if last_sh < prev_sh:
        return {"type": "lower_high", "level": float(last_sh),
                "timestamp": timestamps[recent_sh[-1][0]]}

    return _none


# ─────────────────────────── Kalman Spread Estimator ─────────────────────────


class KalmanSpreadEstimator:
    """Inkrementeller Kalman-Filter für Spread-Zeitreihen.

    State lebt in der Instanz – pro Pair-Strategie eine Instanz erstellen.
    Mathematik übernommen aus Trading_Bot/trader_v6.py (kalman_estimate).
    """

    def __init__(self, q: float = 1e-5, r: float = 0.01):
        self._x: Optional[float] = None   # Schätzung
        self._p: float = 1.0              # Fehler-Kovarianz
        self._q = q                        # Prozessrauschen
        self._r = r                        # Messrauschen

    def update(self, value: float) -> tuple[float, float]:
        """Gibt (mean_estimate, variance) zurück. Thread-safe: nein."""
        if self._x is None:
            self._x = value
        p_pred = self._p + self._q
        k = p_pred / (p_pred + self._r)
        self._x = self._x + k * (value - self._x)
        self._p = (1 - k) * p_pred
        return self._x, self._p

    def z_score(self, value: float, rolling_std: float) -> float:
        """Convenience: update + Z-Score in einem Schritt."""
        mean, _ = self.update(value)
        return (value - mean) / (rolling_std + 1e-9)

    def reset(self) -> None:
        self._x = None
        self._p = 1.0

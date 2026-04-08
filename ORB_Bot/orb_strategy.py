#!/usr/bin/env python3
"""
orb_strategy.py – Single Source of Truth für die ORB-Strategielogik.

Wird von orb_bot_alpaca.py (Live/Paper) und orb_backtest.py (Backtest)
gleichermaßen importiert. Änderungen hier wirken auf beide Seiten.

Fixes gegenüber alter Implementierung:
  #1  orb_breakout_multiplier wird angewendet (war toter Code)
  #2  Timezone-sichere Signallogik (alles über ET)
  #5  Übergeordneter Trendfilter (SPY EMA-20)
  #6  Tageszeitspezifische Volume-MA
  #7  Gap-Filter (Overnight-Gaps)
  #9  Stop-Loss an ORB-Range statt ATR
  #10 Shorts/Freitag Default-Änderungen
"""

from __future__ import annotations

import copy
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz


ET = pytz.timezone("America/New_York")


# ============================= Default-Konfiguration ========================

ORB_DEFAULT_CONFIG: dict = {
    # ── Symbole ────────────────────────────────────────────────────────────
    "symbols": [
        "SPY", "QQQ", "IWM", "DIA",           # ETFs – stabiler Kern
        "NVDA", "TSLA", "AMD", "AVGO",        # High-Vol AI/Chips (2026 besonders stark)
        "AAPL", "MSFT", "META", "AMZN",       # Big-Tech Klassiker
        "PLTR", "GOOGL", "NFLX",              # Volatile Growth-Namen
    ],
    "symbols_watchonly": ["ES=F", "NQ=F", "MES=F", "MNQ=F"],

    # ── ORB-Parameter ──────────────────────────────────────────────────────
    "opening_range_minutes": 30,
    "orb_breakout_multiplier": 1.0,   # Fix #1: wird jetzt angewendet
    "volume_multiplier": 1.1, ## vorher 1.3,
    # Reduziert, damit frische Breakouts nicht als zu "schwach" verworfen werden.
    # Qualität kommt primär über Volume-/Trend-/Gap-Filter.
    "min_signal_strength": 0.08,

    # ── Risiko-Management ──────────────────────────────────────────────────
    "risk_per_trade":    0.005,
    "max_daily_trades":  15,
    "max_concurrent_positions": 3,
    "max_equity_at_risk": 0.05,
    "max_position_value_pct": 0.25,

    # ── Trade-Management (R-basiert) ───────────────────────────────────────
    "profit_target_r": 2.0,
    "stop_loss_r":     1.0,
    "trail_after_r":   1.0,
    "trail_distance_r": 0.5,

    # ── Short-Seite – Fix #10: Shorts standardmäßig an ────────────────────
    "allow_shorts": True,

    # ── Marktzeiten (ET) ───────────────────────────────────────────────────
    "market_open":  time(9, 30),
    "market_close": time(16, 0),
    "orb_end_time": time(10, 0),

    # ── Guards ─────────────────────────────────────────────────────────────
    "use_vix_filter":     True,
    "vix_high_threshold": 30,
    "vix_size_reduction": 0.5,
    "max_drawdown_pct":   0.15,
    "max_volume_pct":     0.01,

    # ── Filter – Fix #10: Freitag nicht mehr gemieden ──────────────────────
    "avoid_fridays": False,
    "avoid_mondays": False,

    # ── Trend-Filter – Fix #5 ─────────────────────────────────────────────
    "use_trend_filter":  True,
    "trend_ema_period":  20,

    # ── Gap-Filter – Fix #7 ───────────────────────────────────────────────
    "use_gap_filter": True,
    "max_gap_pct":    0.03,

    # ── Volume – Fix #6 ───────────────────────────────────────────────────
    "volume_lookback_days": 10,

    # ── MIT Probabilistic Overlay ─────────────────────────────────────────
    "use_mit_probabilistic_overlay": False,
    "mit_ev_threshold_r": 0.08,
    "mit_kelly_fraction": 0.50,
    "mit_min_strength": 0.15,
    "use_mit_independence_guard": True,
    "mit_correlation_groups": {
        "index_etfs": ["SPY", "QQQ", "IWM", "DIA"],
        "semi_ai": ["NVDA", "AMD", "AVGO"],
        "mega_cap_tech": ["AAPL", "MSFT", "META", "AMZN", "GOOGL"],
        "high_beta_growth": ["TSLA", "PLTR", "NFLX"],
    },

    # ── Data Freshness – Fix #4 ───────────────────────────────────────────
    "max_bar_delay_minutes": 20,

    # ── Kosten ─────────────────────────────────────────────────────────────
    "commission_pct": 0.00005,
    "slippage_pct":   0.0002,

    # ── Kapital & Ausgabe ──────────────────────────────────────────────────
    "initial_capital": 10_000.0,
    "currency": "USD",

    # ── Alpaca ─────────────────────────────────────────────────────────────
    "alpaca_paper":     True,
    # IEX = kostenlos aber 15 Min. verzögert; SIP = Echtzeit, kostenpflichtig
    # Für Live-Trading unbedingt SIP verwenden!
    "alpaca_data_feed": "iex",

    # ── Benchmark ──────────────────────────────────────────────────────────
    "benchmark": "SPY",
}


# ============================= Timezone-Helper ==============================

def to_et(dt_or_idx):
    """Konvertiere datetime oder DatetimeIndex nach Eastern Time."""
    if isinstance(dt_or_idx, pd.DatetimeIndex):
        if dt_or_idx.tz is None:
            return dt_or_idx.tz_localize("UTC").tz_convert(ET)
        return dt_or_idx.tz_convert(ET)
    if isinstance(dt_or_idx, datetime):
        if dt_or_idx.tzinfo is None:
            return pytz.utc.localize(dt_or_idx).astimezone(ET)
        return dt_or_idx.astimezone(ET)
    return dt_or_idx


def to_et_time(dt_obj: datetime) -> time:
    """Konvertiere datetime nach ET und gib .time() zurück. Fix #2 & #8."""
    return to_et(dt_obj).time()


def is_market_hours(dt_obj: datetime) -> bool:
    """Prüfe ob in US-Handelszeiten (9:30–16:00 ET)."""
    try:
        et_time = to_et_time(dt_obj)
        return time(9, 30) <= et_time < time(16, 0)
    except Exception:
        return True


def is_trading_day(dt_obj: datetime = None) -> bool:
    """Prüfe ob Werktag (Mo–Fr)."""
    return (dt_obj or datetime.now(pytz.UTC)).weekday() < 5


def is_orb_period(dt_obj: datetime, orb_minutes: int = 30) -> bool:
    """Prüfe ob in der Opening-Range-Periode (9:30–9:30+orb_minutes ET)."""
    try:
        et_time = to_et_time(dt_obj)
        orb_end_h, orb_end_m = divmod(9 * 60 + 30 + orb_minutes, 60)
        return time(9, 30) <= et_time < time(orb_end_h, orb_end_m)
    except Exception:
        return False


# ============================= Indikatoren ==================================

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range."""
    hl = df["High"] - df["Low"]
    hc = np.abs(df["High"] - df["Close"].shift())
    lc = np.abs(df["Low"]  - df["Close"].shift())
    return pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(period).mean()


def compute_indicators(df: pd.DataFrame, volume_sma: int = 20) -> pd.DataFrame:
    """Berechne ATR + Volume MA auf 5m-Bars."""
    df = df.copy()
    df["ATR"]          = calculate_atr(df)

    # Bugfix: tageszeitspezifische Volume-MA statt globalem rolling(20).
    # So wird 10:00-Volumen gegen frühere 10:00-Bars verglichen und nicht mit
    # mittäglicher Flaute/Opening-Spikes vermischt.
    idx_et = to_et(pd.to_datetime(df.index))
    tod_bucket = (idx_et.hour * 100 + idx_et.minute).astype(int)
    df["_tod_bucket"] = np.asarray(tod_bucket, dtype=int)

    # Fallback-MA für frühe Historie, wenn pro Tageszeit-Bucket noch zu wenig Daten da sind.
    min_global = max(3, volume_sma // 4)
    global_ma = df["Volume"].rolling(volume_sma, min_periods=min_global).mean()

    tod_ma = df.groupby("_tod_bucket", sort=False)["Volume"].transform(
        lambda s: s.rolling(volume_sma, min_periods=3).mean()
    )

    df["Volume_MA"] = tod_ma.fillna(global_ma)
    df["Volume_Ratio"] = df["Volume"] / df["Volume_MA"].replace(0, np.nan)
    df.drop(columns=["_tod_bucket"], inplace=True)
    return df


# ============================= ORB-Levels ===================================

def calculate_orb_levels(
    day_df: pd.DataFrame,
    orb_minutes: int = 30,
) -> Tuple[float, float, float]:
    """
    Berechne Opening-Range-Levels aus 5min-Bars eines Tages.

    Returns: (orb_high, orb_low, orb_range)
    """
    if day_df.empty or len(day_df) < 2:
        return 0.0, 0.0, 0.0

    idx_et = to_et(day_df.index)
    hhmm = idx_et.hour * 60 + idx_et.minute

    orb_start = 9 * 60 + 30
    orb_end   = orb_start + orb_minutes

    orb_mask = (hhmm >= orb_start) & (hhmm < orb_end)
    orb_bars = day_df[np.asarray(orb_mask, dtype=bool)]

    if len(orb_bars) < 2:
        return 0.0, 0.0, 0.0

    orb_high  = float(orb_bars["High"].max())
    orb_low   = float(orb_bars["Low"].min())
    orb_range = orb_high - orb_low

    return orb_high, orb_low, orb_range


def get_opening_range(df: pd.DataFrame) -> Tuple[float, float, float]:
    """
    Berechne ORB aus einem multi-day DataFrame (letzter Tag).
    Kompatibilitäts-Wrapper für den Live-Bot.
    """
    if df.empty or len(df) < 6:
        return 0.0, 0.0, 0.0

    df = df.copy()
    df.index = pd.to_datetime(df.index)
    idx_et = to_et(df.index)

    # Robuster als `.date` auf DatetimeIndex und vermeidet pandas/date-Konvertierungsprobleme.
    dates = idx_et.normalize()
    unique_dates = pd.Index(dates).unique()

    if len(unique_dates) == 0:
        return 0.0, 0.0, 0.0

    last_date = unique_dates[-1]
    day_df = df[np.asarray(dates == last_date, dtype=bool)]

    if len(day_df) < 2:
        b = df.iloc[-1]
        return b["High"], b["Low"], b["High"] - b["Low"]

    orb_h, orb_l, orb_r = calculate_orb_levels(day_df)
    if orb_r <= 0:
        b = day_df.iloc[-1]
        return b["High"], b["Low"], b["High"] - b["Low"]

    return orb_h, orb_l, orb_r


# ============================= Breakout-Logik ===============================

def check_breakout(
    price: float,
    orb_high: float,
    orb_low: float,
    orb_range: float,
    multiplier: float = 1.0,
    volume_ok: bool = False,
) -> Tuple[str, float]:
    """
    Kern-Breakout-Prüfung.  Fix #1: ``multiplier`` wird angewendet.

    Returns: (side, strength) – side ist ``"long"``, ``"short"`` oder ``""``.
    """
    if orb_range <= 0:
        return "", 0.0

    breakout_level  = orb_high + (multiplier - 1.0) * orb_range
    breakdown_level = orb_low  - (multiplier - 1.0) * orb_range

    if price > breakout_level:
        strength = min((price - orb_high) / orb_range, 1.0)
        if volume_ok:
            strength = min(strength * 1.2, 1.0)
        return "long", strength

    if price < breakdown_level:
        strength = min((orb_low - price) / orb_range, 1.0)
        if volume_ok:
            strength = min(strength * 1.2, 1.0)
        return "short", strength

    return "", 0.0


# ============================= Stop-Loss – Fix #9 ==========================

def calculate_stop(
    side: str,
    entry_price: float,
    orb_high: float,
    orb_low: float,
    orb_range: float,
    sl_r: float = 1.0,
) -> float:
    """
    Stop-Loss gebunden an ORB-Range statt ATR (Fix #9).

    Long:  stop = max(orb_low,  entry - sl_r × orb_range)
    Short: stop = min(orb_high, entry + sl_r × orb_range)
    """
    if side == "long":
        stop = max(orb_low, entry_price - sl_r * orb_range)
        if stop >= entry_price:
            stop = entry_price - 0.5 * orb_range
        return stop
    else:
        stop = min(orb_high, entry_price + sl_r * orb_range)
        if stop <= entry_price:
            stop = entry_price + 0.5 * orb_range
        return stop


# ============================= Position Sizing ==============================

def calculate_position_size(
    entry: float,
    stop: float,
    equity: float,
    risk_per_trade: float = 0.005,
    max_equity_at_risk: float = 0.05,
    max_position_value_pct: float = 0.25,
) -> int:
    """R-basierte Positionsgröße mit zusätzlichem Notional-Cap."""
    risk = abs(entry - stop)
    if risk <= 0 or entry <= 0 or equity <= 0:
        return 0
    shares = int((equity * risk_per_trade) / risk)
    max_sh = int((equity * max_equity_at_risk) / risk)
    max_notional = equity * max_position_value_pct
    max_shares_by_value = int(max_notional / entry)
    return max(0, min(shares, max_sh, max_shares_by_value))


# ============================= Filter =======================================

def check_trend_filter(
    spy_df: pd.DataFrame,
    ema_period: int = 20,
) -> Dict[str, bool]:
    """
    Fix #5: Übergeordneter Trendfilter basierend auf SPY EMA.

    Returns
    -------
    {"bullish": bool, "bearish": bool}
      - bullish=True  → Longs erlaubt
      - bearish=True  → Shorts erlaubt
    """
    if spy_df is None or spy_df.empty or len(spy_df) < ema_period:
        return {"bullish": True, "bearish": True}

    close = spy_df["Close"]
    ema   = close.ewm(span=ema_period, adjust=False).mean()

    last_close = float(close.iloc[-1])
    last_ema   = float(ema.iloc[-1])

    return {
        "bullish": last_close > last_ema,
        "bearish": last_close < last_ema,
    }


def check_gap_filter(
    today_open: float,
    prev_close: float,
    max_gap_pct: float = 0.03,
) -> bool:
    """Fix #7: Gibt True zurück wenn Trading erlaubt (Gap klein genug)."""
    if prev_close <= 0:
        return True
    gap = abs(today_open - prev_close) / prev_close
    return gap <= max_gap_pct


def compute_orb_volume_ratio(
    day_df: pd.DataFrame,
    historical_dfs: Optional[List[pd.DataFrame]] = None,
    orb_minutes: int = 30,
) -> float:
    """
    Fix #6: Tageszeitspezifische Volume-Ratio.

    Vergleicht ORB-Volumen (9:30–10:00) des aktuellen Tages mit dem
    Durchschnitt der gleichen Zeitfenster der letzten N Tage.
    """
    idx_et = to_et(day_df.index)
    hhmm = idx_et.hour * 60 + idx_et.minute
    orb_start = 9 * 60 + 30
    orb_end   = orb_start + orb_minutes

    orb_mask = np.asarray((hhmm >= orb_start) & (hhmm < orb_end), dtype=bool)
    today_orb = day_df[orb_mask]

    if today_orb.empty or "Volume" not in today_orb.columns:
        return 1.0

    today_vol = float(today_orb["Volume"].sum())

    if not historical_dfs:
        # Fallback: einfache rolling ratio
        if "Volume_MA" in day_df.columns:
            last_ma = day_df["Volume_MA"].dropna()
            if not last_ma.empty:
                avg_bar = float(last_ma.iloc[-1])
                return today_vol / (avg_bar * max(len(today_orb), 1)) if avg_bar > 0 else 1.0
        return 1.0

    hist_vols: List[float] = []
    for hdf in historical_dfs:
        if hdf.empty:
            continue
        h_idx_et = to_et(hdf.index)
        h_hhmm   = h_idx_et.hour * 60 + h_idx_et.minute
        h_mask   = np.asarray((h_hhmm >= orb_start) & (h_hhmm < orb_end), dtype=bool)
        h_orb    = hdf[h_mask]
        if not h_orb.empty and "Volume" in h_orb.columns:
            hist_vols.append(float(h_orb["Volume"].sum()))

    if not hist_vols:
        return 1.0

    avg_hist = np.mean(hist_vols)
    return today_vol / avg_hist if avg_hist > 0 else 1.0


# ============================= Signal-Generierung (Live) ====================

def generate_signal(
    df: pd.DataFrame,
    cfg: dict,
    spy_df: Optional[pd.DataFrame] = None,
) -> Tuple[str, float, str, dict]:
    """
    Signal-Generierung für den Live-Bot (einzelnes Signal für aktuelle Bar).

    Fixes:  #1 multiplier, #2 timezone, #5 trend, #7 gap.

    Returns: (signal, strength, reason, context)
    """
    if len(df) < 2:
        return "HOLD", 0.0, "Insufficient data", {}

    orb_high, orb_low, orb_range = get_opening_range(df)
    if orb_range <= 0:
        return "HOLD", 0.0, "Invalid ORB range", {}

    # Volume-Kontext: zeitfensterkorrekte ORB-Volume-Ratio (Fix #6)
    # compute_orb_volume_ratio() summiert das ORB-Fenster (9:30–10:00) statt
    # nur den letzten Bar gegen rolling(20) zu vergleichen, das ORB- und
    # post-ORB-Bars mischt.
    vol_r = compute_orb_volume_ratio(df)
    vol_mult = float(cfg.get("volume_multiplier", 1.3))
    volume_confirmed = vol_r >= vol_mult

    ctx: dict = {
        "volume_ratio":     vol_r,
        "volume_confirmed": volume_confirmed,
        "orb_range_pct":    (orb_range / orb_low * 100) if orb_low > 0 else 0,
        "orb_high": orb_high,
        "orb_low":  orb_low,
        "orb_range": orb_range,
    }

    current_price = float(df["Close"].iloc[-1])
    current_time  = (df.index[-1].to_pydatetime()
                     if hasattr(df.index[-1], "to_pydatetime")
                     else datetime.now(pytz.UTC))

    if not is_market_hours(current_time):
        return "HOLD", 0.0, "Outside market hours", ctx

    # Fix #2: Timezone korrekt nach ET konvertieren
    et_time = to_et_time(current_time)
    orb_end = cfg.get("orb_end_time", time(10, 0))

    if is_orb_period(current_time) or et_time < orb_end:
        return "HOLD", 0.0, "Waiting for ORB period to end", ctx

    # Fix #7: Gap-Filter
    if cfg.get("use_gap_filter", True):
        idx_et = to_et(df.index)
        dates = idx_et.normalize()
        unique_dates = pd.Index(dates).unique()
        if len(unique_dates) >= 2:
            last_date = unique_dates[-1]
            prev_date = unique_dates[-2]
            today_bars = df[np.asarray(dates == last_date, dtype=bool)]
            prev_bars  = df[np.asarray(dates == prev_date, dtype=bool)]
            if not today_bars.empty and not prev_bars.empty:
                today_open = float(today_bars["Open"].iloc[0])
                prev_close = float(prev_bars["Close"].iloc[-1])
                max_gap    = float(cfg.get("max_gap_pct", 0.03))
                if not check_gap_filter(today_open, prev_close, max_gap):
                    gap_pct = abs(today_open - prev_close) / prev_close * 100
                    return "HOLD", 0.0, f"Gap zu groß ({gap_pct:.1f}%)", ctx

    # Fix #5: Trendfilter
    trend = {"bullish": True, "bearish": True}
    if cfg.get("use_trend_filter", True):
        trend = check_trend_filter(spy_df, cfg.get("trend_ema_period", 20))
    ctx["trend"] = trend

    # Fix #1: multiplier anwenden
    multiplier = float(cfg.get("orb_breakout_multiplier", 1.0))
    side, strength = check_breakout(
        current_price, orb_high, orb_low, orb_range,
        multiplier, volume_confirmed,
    )

    min_strength = float(cfg.get("min_signal_strength", 0.3))

    if side == "long" and strength >= min_strength:
        if cfg.get("use_trend_filter", True) and not trend["bullish"]:
            return "HOLD", strength, "Trend bearish – Long unterdrückt", ctx
        reason = f"ORB Breakout: {current_price:.2f} > {orb_high:.2f}"
        if volume_confirmed:
            reason += f" +Vol {vol_r:.1f}x"
        return "BUY", strength, reason, ctx

    if side == "short" and strength >= min_strength:
        if cfg.get("use_trend_filter", True) and not trend["bearish"]:
            return "HOLD", strength, "Trend bullish – Short unterdrückt", ctx
        reason = f"ORB Breakdown: {current_price:.2f} < {orb_low:.2f}"
        if volume_confirmed:
            reason += f" +Vol {vol_r:.1f}x"
        if cfg.get("allow_shorts", True):
            return "SHORT", strength, reason, ctx
        return "HOLD", strength, f"[SHORT disabled] {reason}", ctx

    return "HOLD", 0.0, "Waiting for ORB breakout", ctx


# ============================= Signal-Generierung (Backtest) ================

def compute_orb_signals(
    day_df: pd.DataFrame,
    orb_high: float,
    orb_low: float,
    orb_range: float,
    cfg: dict,
) -> pd.DataFrame:
    """
    Signale für alle Bars eines Handelstages (Backtest).

    Rückgabe-DataFrame mit Spalten:
      entry_long, entry_short, strength, volume_ok, side
    """
    out = pd.DataFrame(index=day_df.index)
    n = len(day_df)

    out["entry_long"]  = False
    out["entry_short"] = False
    out["strength"]    = 0.0
    out["volume_ok"]   = False
    out["side"]        = ""

    if orb_range <= 0 or n < 2:
        return out

    idx_et = to_et(day_df.index)
    hhmm   = idx_et.hour * 60 + idx_et.minute

    orb_end_min  = 9 * 60 + 30 + int(cfg.get("opening_range_minutes", 30))
    post_orb     = np.asarray(hhmm >= orb_end_min, dtype=bool)

    close        = day_df["Close"]
    vol_mult     = float(cfg.get("volume_multiplier", 1.3))
    min_strength = float(cfg.get("min_signal_strength", 0.3))
    allow_shorts = bool(cfg.get("allow_shorts", True))
    breakout_mult = float(cfg.get("orb_breakout_multiplier", 1.0))

    # Volume check
    if "Volume_Ratio" in day_df.columns:
        vol_ok = day_df["Volume_Ratio"] >= vol_mult
    elif "Volume_MA" in day_df.columns:
        vol_ok = day_df["Volume"] >= vol_mult * day_df["Volume_MA"]
    else:
        vol_ok = pd.Series(True, index=day_df.index)

    out["volume_ok"] = vol_ok.values

    # Fix #1: Breakout-Level mit multiplier
    breakout_level  = orb_high + (breakout_mult - 1.0) * orb_range
    breakdown_level = orb_low  - (breakout_mult - 1.0) * orb_range

    # Long Breakout
    long_break    = (close > breakout_level) & post_orb
    long_strength = ((close - orb_high) / orb_range).clip(0.0, 1.0)
    long_strength = np.where(vol_ok, np.minimum(long_strength * 1.2, 1.0), long_strength)
    long_valid    = long_break & (long_strength >= min_strength)

    out.loc[long_valid, "entry_long"] = True
    out["strength"] = np.where(long_break, long_strength, out["strength"])
    out.loc[long_valid, "side"] = "long"

    # Short Breakdown
    if allow_shorts:
        short_break    = (close < breakdown_level) & post_orb
        short_strength = ((orb_low - close) / orb_range).clip(0.0, 1.0)
        short_strength = np.where(vol_ok, np.minimum(short_strength * 1.2, 1.0),
                                  short_strength)
        short_valid    = short_break & (short_strength >= min_strength)

        out.loc[short_valid, "entry_short"] = True
        out["strength"] = np.where(
            short_break & ~long_break,
            short_strength,
            out["strength"],
        )
        out.loc[short_valid & ~long_valid, "side"] = "short"

    return out


def prepare_orb_day(
    day_5m_df: pd.DataFrame,
    cfg: dict,
) -> Optional[Dict]:
    """Convenience: ORB-Levels + Signals für einen Tag."""
    if day_5m_df.empty or len(day_5m_df) < 8:
        return None

    orb_minutes = int(cfg.get("opening_range_minutes", 30))
    orb_high, orb_low, orb_range = calculate_orb_levels(day_5m_df, orb_minutes)

    if orb_range <= 0:
        return None

    signals = compute_orb_signals(day_5m_df, orb_high, orb_low, orb_range, cfg)

    return {
        "orb_high":   orb_high,
        "orb_low":    orb_low,
        "orb_range":  orb_range,
        "signals_df": signals,
    }

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

# NYSE-Feiertagskalender – erkennt MLK Day, Presidents Day, Good Friday etc.
try:
    import exchange_calendars as xcals
    _NYSE_CAL = xcals.get_calendar("XNYS")
    _XCALS_AVAILABLE = True
except ImportError:
    _NYSE_CAL = None
    _XCALS_AVAILABLE = False


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
    "eod_close_time": time(15, 27),  # Fix #8: EOD-Close Zeit konfigurierbar

    # ── Guards ─────────────────────────────────────────────────────────────
    "use_vix_filter":     True,
    "vix_high_threshold": 30,
    "vix_size_reduction": 0.5,
    # ── VIX Term Structure Regime – Fix #16 ─────────────────────────────
    "use_vix_term_structure": True, # aktiviert nach Einbau,  # Aktiviere VIX/VIX3M Regime-Filter
    "vix_regime_flat_lower":  0.90,   # Contango cutoff
    "vix_regime_flat_upper":  1.00,   # Flat → Backwardation
    "vix_regime_backwd_upper": 1.15,  # Extreme Backwardation cutoff
    # VIX Regime Size-Multiplikatoren (konfigurierbar):
    #   "contango" (<0.90):           1.00  (100%)
    #   "flat" (0.90-1.00):           0.75  (75%)
    #   "backwardation" (1.00-1.15):  0.50  (50%)
    #   "extreme_backwardation" (>1.15): 0.00 (Shorts only)
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
    "use_mit_probabilistic_overlay": True, ##aktiviert nach Einbau False,
    "mit_ev_threshold_r": 0.08,
    "mit_kelly_fraction": 0.50,
    "mit_min_strength": 0.15,
    "mit_calibration_offset": 0.0317,# 0.0,  # Fix #14: Kalibrierungsoffset (aus calibrate_win_probability aufgerufen mit: orb_bot_v2.py --mode calibrate)
    # ── DD-Scaling für dynamic Kelly (Fix #15) ────────────────────────────
    "use_dynamic_kelly_dd_scaling": True,  # vorher false # Aktiviere DD-basiertes Sizing
    "dynamic_kelly_max_dd": 0.15,          # Max DD-Schwelle (default 15%)
    # Bei DD ≥ 15%: kelly_fraction → 0.0
    # Bei DD < 15%: Exponentielle Skalierung mit Exponent 1.5
    "use_mit_independence_guard": True,
    "mit_correlation_groups": {
        "index_etfs": ["SPY", "QQQ", "IWM", "DIA"],
        "semi_ai": ["NVDA", "AMD", "AVGO"],
        "mega_cap_tech": ["AAPL", "MSFT", "META", "AMZN", "GOOGL"],
        "high_beta_growth": ["TSLA", "PLTR", "NFLX"],
    },
    # ── Data Freshness – Fix #4 ───────────────────────────────────────────
    "max_bar_delay_minutes": 20,

    # ── Time-Decay-Filter – Fix #13 ───────────────────────────────────────
    "use_time_decay_filter": True,
    # Konfigurierbare Decay-Brackets (minutes_since_orb → weight)
    # Default: Prime-Time (≤30) → 1.0, dann abnehmend bis Late-Session → 0.40
    "time_decay_brackets": [
        (30,  1.00),  # ≤30 min nach ORB (10:00-10:30): volles Gewicht
        (90,  0.85),  # ≤90 min (10:30-11:30): gut, aber abnehmend
        (180, 0.65),  # ≤180 min (11:30-13:00): Mid-Session
    ],
    "time_decay_late_factor": 0.40,  # Alles danach (13:00+)
    # Absolute Entry-Sperre: keine neuen Trades nach dieser ET-Zeit
    "entry_cutoff_time": None,  # None = kein Cutoff, oder time(14, 30) etc.

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
        # Bei Fehler konservativ: kein Trading statt blindes Handeln
        return False


def is_trading_day(dt_obj: datetime = None) -> bool:
    """
    Prüfe ob Handelstag an der NYSE.

    Nutzt exchange_calendars wenn verfügbar (erkennt alle US-Feiertage:
    MLK Day, Presidents Day, Good Friday, Memorial Day, Juneteenth,
    Independence Day, Labor Day, Thanksgiving, Christmas).
    Fallback: einfache Werktags-Prüfung (Mo–Fr).
    """
    d = (dt_obj or datetime.now(pytz.UTC))
    if d.weekday() >= 5:
        return False
    if _XCALS_AVAILABLE:
        try:
            return _NYSE_CAL.is_session(pd.Timestamp(d.date()))
        except Exception:
            pass
    return True


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


def check_entry_cutoff(
    dt_obj: datetime,
    cfg: dict,
) -> bool:
    """
    Fix #13: Absoluter Entry-Cutoff – keine neuen Trades nach konfigurierter Zeit.

    Ergänzt time_decay_factor() (Strength-Gewichtung) um eine harte Grenze.
    Strength-Degradation über die Tageszeit wird bereits durch time_decay_factor()
    in compute_orb_signals() und generate_signal() angewendet.

    Returns: True wenn Entry erlaubt, False wenn zu spät.
    """
    cutoff = cfg.get("entry_cutoff_time")
    if cutoff is None:
        return True

    et_time = to_et_time(dt_obj)
    return et_time < cutoff


def get_vix_term_structure_regime(
    vix_spot: float,
    vix_3m: Optional[float] = None,
    cfg: Optional[dict] = None,
) -> Tuple[str, float, str]:
    """
    Fix #16: VIX Term Structure Regime – Regime-Indikator basierend auf VIX/VIX3M Ratio.

    Die Term Structure erfasst die Änderungsrate der Angst:
      - Contango (<0.90): Normal, langfristige Risiken höher. → Volle Positionsgrößen
      - Flach (0.90-1.00): Warnung, kurzfristige≈langfristige Risiken. → 75%
      - Backwardation (1.00-1.15): Panik, kurzfristige Risiken höher. → 50%
      - Extreme Backwardation (>1.15): Existenzielle Risiken. → Nur Shorts erlaubt

    Diese Filter verbessert die Trade Independence, weil er korrelierte Drawdowns
    im gesamten Portfolio reduziert.

    Parameters
    ----------
    vix_spot : float
        Spot VIX Level (z.B. 25.5)
    vix_3m : Optional[float]
        3-Monats VIX (z.B. aus Daten oder Fallback zu vix_spot * 1.02)
    cfg : Optional[dict]
        Konfiguration mit Schwellen (z.B. cfg["vix_regime_thresholds"])

    Returns
    -------
    (regime_name, size_multiplier, reason_string)
      - regime_name: "contango", "flat", "backwardation", "extreme_backwardation"
      - size_multiplier: [1.0, 0.75, 0.50, 0.0]  # Sizing-Anpassung
      - reason_string: Beschreibung für Logs
    """
    if vix_spot <= 0:
        vix_spot = 20.0  # Fallback zu VIX Default

    # VIX3M aus Daten oder gerechtfertiger Fallback
    if vix_3m is None or vix_3m <= 0:
        # Typischerweise: VIX3M ≈ 1-2% höher als VIX in normalen Zeiten
        vix_3m = vix_spot * 1.02

    vix_ratio = vix_spot / vix_3m if vix_3m > 0 else 1.0

    # Schwellen aus Config (oder Defaults)
    if cfg is not None:
        flat_lower    = float(cfg.get("vix_regime_flat_lower", 0.90))
        flat_upper    = float(cfg.get("vix_regime_flat_upper", 1.00))
        backwd_upper  = float(cfg.get("vix_regime_backwd_upper", 1.15))
    else:
        flat_lower    = 0.90
        flat_upper    = 1.00
        backwd_upper  = 1.15

    if vix_ratio < flat_lower:
        regime = "contango"
        multiplier = 1.00
        reason = f"Contango ({vix_ratio:.2f}): VIX normal, volle Positionsgrößen"
    elif vix_ratio < flat_upper:
        regime = "flat"
        multiplier = 0.75
        reason = f"Flat ({vix_ratio:.2f}): Term Structure ausgeflacht, 75% Position"
    elif vix_ratio < backwd_upper:
        regime = "backwardation"
        multiplier = 0.50
        reason = f"Backwardation ({vix_ratio:.2f}): Panik, 50% Position"
    else:
        regime = "extreme_backwardation"
        multiplier = 0.00
        reason = f"Extreme Backwardation ({vix_ratio:.2f}): Nur Shorts erlaubt"

    return regime, multiplier, reason


def time_decay_factor(bar_time_et: time, cfg: Optional[dict] = None) -> float:
    """
    Fix #13: Konfigurierbarer Gewichtungsfaktor basierend auf Tageszeit (ET).

    ORB-Breakouts kurz nach der Opening Range (10:00-10:30) haben empirisch
    höhere Completion-Rates, da institutionelle Flows zum Opening-Print noch
    aktiv sind. Spätere Breakouts können Noise/Rotation sein.

    Brackets konfigurierbar via cfg["time_decay_brackets"]:
      Default:
        ≤  30 min  (10:00–10:30): 1.00  – Prime-Time, volles Gewicht
        ≤  90 min  (10:30–11:30): 0.85  – Gut, aber abnehmend
        ≤ 180 min  (11:30–13:00): 0.65  – Mid-Session, erhöhter Rauschanteil
        >  180 min (13:00+):      0.40  – Late-Session, schwache Breakouts
    """
    minutes_since_orb = (bar_time_et.hour * 60 + bar_time_et.minute) - 600

    if cfg is not None:
        brackets = cfg.get("time_decay_brackets", [(30, 1.0), (90, 0.85), (180, 0.65)])
        late_factor = float(cfg.get("time_decay_late_factor", 0.40))
    else:
        brackets = [(30, 1.0), (90, 0.85), (180, 0.65)]
        late_factor = 0.40

    for threshold, weight in brackets:
        if minutes_since_orb <= threshold:
            return weight
    return late_factor


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

    # Time-Decay Filter: Strength-Gewichtung nach Tageszeit (Fix #13: konfigurierbar)
    if cfg.get("use_time_decay_filter", True):
        decay = time_decay_factor(et_time, cfg)
        strength *= decay
        ctx["time_decay_factor"] = decay
    else:
        ctx["time_decay_factor"] = 1.0

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
    use_time_decay = bool(cfg.get("use_time_decay_filter", True))

    # Volume check
    if "Volume_Ratio" in day_df.columns:
        vol_ok = day_df["Volume_Ratio"] >= vol_mult
    elif "Volume_MA" in day_df.columns:
        vol_ok = day_df["Volume"] >= vol_mult * day_df["Volume_MA"]
    else:
        vol_ok = pd.Series(True, index=day_df.index)

    out["volume_ok"] = vol_ok.values

    # Fix #13: Time-Decay (konfigurierbare Brackets, vektorisiert)
    if use_time_decay:
        minutes_since_orb = np.array(idx_et.hour * 60 + idx_et.minute) - 600
        brackets = cfg.get("time_decay_brackets", [(30, 1.0), (90, 0.85), (180, 0.65)])
        late_factor = float(cfg.get("time_decay_late_factor", 0.40))
        decay_factors = np.full(len(day_df), late_factor)
        for threshold, weight in reversed(brackets):
            decay_factors = np.where(minutes_since_orb <= threshold, weight, decay_factors)
    else:
        decay_factors = np.ones(len(day_df))

    # Fix #1: Breakout-Level mit multiplier
    breakout_level  = orb_high + (breakout_mult - 1.0) * orb_range
    breakdown_level = orb_low  - (breakout_mult - 1.0) * orb_range

    # Long Breakout
    long_break    = (close > breakout_level) & post_orb
    long_strength = ((close - orb_high) / orb_range).clip(0.0, 1.0)
    long_strength = np.where(vol_ok, np.minimum(long_strength * 1.2, 1.0), long_strength)
    long_strength = long_strength * decay_factors
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
        short_strength = short_strength * decay_factors
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


# ============================= MIT Probabilistic Overlay ======================
# Single Source of Truth – wird von Live-Bot und Backtest gleichermaßen genutzt.

def mit_estimate_win_probability(
    signal: str,
    strength: float,
    ctx: dict,
    df: pd.DataFrame,
) -> float:
    """
    Heuristische Schätzung der Win-Probability basierend auf Signalqualität.

    Inputs über ctx:
      - volume_ratio, volume_confirmed, orb_range_pct, trend
    Inputs über df (letzter Bar):
      - Close, ATR, Volume_Ratio

    Returns: geclippte Probability [0.20, 0.80]
    """
    last = df.iloc[-1] if not df.empty else pd.Series(dtype=float)
    volume_ratio = float(ctx.get("volume_ratio", last.get("Volume_Ratio", 1.0) or 1.0))
    orb_range_pct = float(ctx.get("orb_range_pct", 0.0) or 0.0)
    close = float(last.get("Close", 0.0) or 0.0)
    atr_val = float(last.get("ATR", 0.0) or 0.0)
    atr_pct = (atr_val / close * 100.0) if close > 0 and atr_val > 0 else 0.0
    trend = ctx.get("trend", {"bullish": True, "bearish": True})
    trend_aligned = (
        signal == "BUY" and trend.get("bullish", True)
    ) or (
        signal == "SHORT" and trend.get("bearish", True)
    )

    win_prob = 0.40
    win_prob += 0.25 * float(np.clip(strength, 0.0, 1.0))
    win_prob += 0.04 * float(np.clip(volume_ratio - 1.0, 0.0, 1.5))
    if ctx.get("volume_confirmed", False):
        win_prob += 0.03
    if 0.25 <= orb_range_pct <= 1.20:
        win_prob += 0.03
    elif orb_range_pct > 2.00:
        win_prob -= 0.04
    if atr_pct > 0 and orb_range_pct > 0:
        range_vs_atr = orb_range_pct / max(atr_pct, 1e-9)
        if 0.35 <= range_vs_atr <= 1.25:
            win_prob += 0.03
        elif range_vs_atr > 1.75:
            win_prob -= 0.05
    if trend_aligned:
        win_prob += 0.03
    else:
        win_prob -= 0.05
    return float(np.clip(win_prob, 0.20, 0.80))


def mit_compute_ev_r(win_prob: float, reward_r: float, risk_r: float = 1.0) -> float:
    """Expected Value in R-Multiples."""
    return (win_prob * reward_r) - ((1.0 - win_prob) * risk_r)


def mit_kelly_fraction(
    win_prob: float,
    reward_r: float,
    risk_r: float = 1.0,
) -> float:
    """Kelly-Fraction für gegebene Win-Probability und Payoff-Ratio."""
    if reward_r <= 0 or risk_r <= 0:
        return 0.0
    b = reward_r / risk_r
    q = 1.0 - win_prob
    return max(0.0, ((b * win_prob) - q) / b)


def dynamic_kelly(
    base_kelly: float,
    current_dd: float,
    max_dd: float = 0.15,
) -> float:
    """
    Fix #15: Kelly-Fraction skaliert mit aktuellem Drawdown (DD-Scaling).

    Statt eines binären Drawdown-Breakers (alles oder nichts beim max_dd)
    wird die Kelly-Fraction graduell reduziert, wenn der Drawdown steigt.

    Diese Strategie:
      1. Reduziert Exposure graduell statt abrupt
      2. Nutzt nach dem Law of Large Numbers auch in DD-Phasen positive
         EV-Situationen mit reduzierter Größe
      3. Ermöglicht Recovery-Opportunitäten während des Drawdown

    Die exponentielle Skalierung (Exponent 1.5) reduziert aggressiver bei
    tieferen Drawdowns, während flache Drawdowns (~0–5%) minimal impactieren.

    Parameters
    ----------
    base_kelly : float
        Die Basis Kelly-Fraction (z.B. aus mit_kelly_fraction oder
        fractional Kelly nach Kelly-Fraction).
    current_dd : float
        Aktueller Drawdown als Dezimal (z.B. 0.08 für 8%).
    max_dd : float, default 0.15
        Maximaler Drawdown-Schwelle (default 15%). Bei current_dd >= max_dd
        wird base_kelly auf 0.0 gesetzt.

    Returns
    -------
    float
        Skalierte Kelly-Fraction im Bereich [0.0, base_kelly].
        Bei current_dd >= max_dd: 0.0
        Bei current_dd < max_dd: base_kelly × scale, wobei
          scale = max(0.0, 1.0 - (current_dd / max_dd) ** 1.5)
    """
    if current_dd >= max_dd:
        return 0.0
    scale = max(0.0, 1.0 - (current_dd / max_dd) ** 1.5)
    return base_kelly * scale


def mit_group_for_symbol(symbol: str, cfg: dict) -> str:
    """Finde die MIT-Korrelationsgruppe für ein Symbol."""
    groups = cfg.get("mit_correlation_groups", {})
    for group_name, members in groups.items():
        if symbol in members:
            return group_name
    return ""


def mit_apply_overlay(
    signal: str,
    strength: float,
    ctx: dict,
    df: pd.DataFrame,
    cfg: dict,
    current_drawdown: float = 0.0,
    vix_spot: Optional[float] = None,
    vix_3m: Optional[float] = None,
) -> Tuple[bool, float, str]:
    """
    MIT Probabilistic Overlay – Gate für Trade-Ausführung.

    Prüft ob ein Signal positiven Expected Value hat und berechnet
    die Kelly-basierte Positionsgrößen-Skalierung mit DD-Scaling (Fix #15)
    und optional VIX Term Structure Regime-Filtering (Fix #16).

    Returns: (should_trade, qty_factor, reason_string)
    """
    if not cfg.get("use_mit_probabilistic_overlay", False):
        return True, 1.0, "MIT Overlay deaktiviert"
    if signal not in ("BUY", "SHORT"):
        return False, 0.0, "Kein ORB-Signal"

    min_strength = float(cfg.get("mit_min_strength", 0.15))
    if strength < min_strength:
        return False, 0.0, f"MIT Overlay reject: Strength {strength:.2f} < {min_strength:.2f}"

    reward_r = float(cfg.get("profit_target_r", 2.0))
    win_prob = mit_estimate_win_probability(signal, strength, ctx, df)

    # Fix #14: Kalibrierungsoffset anwenden wenn kalibriert
    if cfg.get("mit_calibration_offset", 0.0) != 0.0:
        cal_offset = float(cfg.get("mit_calibration_offset", 0.0))
        win_prob = float(np.clip(win_prob + cal_offset, 0.20, 0.80))

    ev_r = mit_compute_ev_r(win_prob, reward_r, 1.0)
    ev_threshold = float(cfg.get("mit_ev_threshold_r", 0.08))
    if ev_r <= ev_threshold:
        return False, 0.0, f"MIT Overlay reject: P={win_prob:.2f} EV={ev_r:+.2f}R"

    raw_kelly = mit_kelly_fraction(win_prob, reward_r, 1.0)
    fractional_kelly = raw_kelly * float(cfg.get("mit_kelly_fraction", 0.50))

    # Fix #15: DD-Scaling – graduelles Sizing statt binärem Breaker
    dd_scaling_reason = ""
    if cfg.get("use_dynamic_kelly_dd_scaling", False):
        max_dd = float(cfg.get("dynamic_kelly_max_dd", 0.15))
        dd_scaled_kelly = dynamic_kelly(fractional_kelly, current_drawdown, max_dd)
        dd_scaling_reason = f" [DD {current_drawdown:.2%} → Kelly {dd_scaled_kelly:.3f}]"
        fractional_kelly = dd_scaled_kelly

    # Fix #16: VIX Term Structure Regime – Sizing-Anpassung nach Marktstruktur
    vix_scaling_reason = ""
    if cfg.get("use_vix_term_structure", False) and vix_spot is not None:
        regime, vix_multiplier, regime_reason = get_vix_term_structure_regime(vix_spot, vix_3m, cfg)
        
        # Extreme Backwardation: Shorts erlauben, Longs verbieten
        if regime == "extreme_backwardation":
            if signal == "BUY":
                return False, 0.0, f"VIX Regime reject: {regime_reason}"
            # Shorts sind erlaubt, aber mit voller Kelly (nicht skaliert)
            vix_multiplier = 1.0
        
        fractional_kelly *= vix_multiplier
        vix_scaling_reason = f" [VIX-Regime: {regime} {vix_multiplier:.2f}x]"

    qty_factor = float(np.clip(0.25 + fractional_kelly, 0.25, 1.0))
    return True, qty_factor, f"MIT Overlay: P={win_prob:.2f} EV={ev_r:+.2f}R Kelly={qty_factor:.2f}x{dd_scaling_reason}{vix_scaling_reason}"


def calibrate_win_probability(
    trades_df: pd.DataFrame,
    signal_strength_col: str = "strength",
    cfg: Optional[dict] = None,
) -> dict:
    """
    Fix #14: Kalibriere Win-Probability gegen echte Backtest-Ergebnisse.

    Vergleicht die geschätzte Probability (aus mit_estimate_win_probability)
    mit der echten Win-Rate aus den Backtest-Trades und berechnet
    einen Kalibrierungsoffset.

    Parameter
    ----------
    trades_df : DataFrame mit Trades (muss 'pnl' und optional 'strength' enthalten)
    signal_strength_col : Name der Strength-Spalte
    cfg : Optional – für Kontextt-Info

    Returns
    -------
    {"offset": float, "actual_win_rate": float, "estimated_avg": float, "n_trades": int}
    """
    if trades_df is None or trades_df.empty:
        return {"offset": 0.0, "error": "No trades"}

    sells = trades_df[trades_df["side"].isin(["SELL", "COVER"])]
    if len(sells) < 10:
        return {"offset": 0.0, "error": f"Not enough trades ({len(sells)} < 10)"}

    # Echte Win-Rate
    wins = sells[sells["pnl"] > 0]
    actual_wr = len(wins) / len(sells)

    # Durchschnittliche geschätzte Strength aus den Entry-Trades
    # (Diese ist eine Proxy für die heuristische Probability)
    if signal_strength_col in trades_df.columns:
        entries = trades_df[trades_df["side"].isin(["BUY", "SHORT"])]
        if not entries.empty:
            avg_strength = float(entries[signal_strength_col].mean())
        else:
            avg_strength = 0.5
    else:
        avg_strength = 0.5

    # Heuristische Baseline: mit_estimate_win_probability gibt oft ~0.40–0.65
    # Je höher die Strength, desto näher an 0.65
    estimated_baseline = 0.40 + (avg_strength * 0.25)

    # Offset = actual_wr - estimated_baseline
    offset = actual_wr - estimated_baseline

    return {
        "offset": round(offset, 4),
        "actual_win_rate": round(actual_wr, 4),
        "estimated_baseline": round(estimated_baseline, 4),
        "avg_entry_strength": round(avg_strength, 4),
        "n_trades": len(sells),
    }

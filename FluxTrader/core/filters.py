"""Trade-Filter: Trend, Gap, Volume, VIX-Regime, Time-of-Day, Cutoff."""
from __future__ import annotations

from datetime import datetime, time
from typing import Optional

import numpy as np
import pandas as pd
import pytz

from core.indicators import ema

ET = pytz.timezone("America/New_York")


# ─────────────────────────── Timezone / Market ──────────────────────────────

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
    return to_et(dt_obj).time()


def is_market_hours(dt_obj: datetime) -> bool:
    try:
        t = to_et_time(dt_obj)
        return time(9, 30) <= t < time(16, 0)
    except Exception:
        return False


def is_trading_day(dt_obj: Optional[datetime] = None) -> bool:
    """NYSE-Handelstag. Nutzt exchange_calendars wenn verfügbar."""
    d = dt_obj or datetime.now(pytz.UTC)
    if d.weekday() >= 5:
        return False
    try:
        import exchange_calendars as xcals
        cal = xcals.get_calendar("XNYS")
        return cal.is_session(pd.Timestamp(d.date()))
    except Exception:
        return True


def is_orb_period(dt_obj: datetime, orb_minutes: int = 30) -> bool:
    try:
        t = to_et_time(dt_obj)
        end_h, end_m = divmod(9 * 60 + 30 + orb_minutes, 60)
        return time(9, 30) <= t < time(end_h, end_m)
    except Exception:
        return False


# ─────────────────────────── Trend / Gap ───────────────────────────────────

def trend_filter_from_spy(spy_df: pd.DataFrame,
                          ema_period: int = 20) -> dict:
    """Übergeordneter Trendfilter via SPY EMA.

    Return: {"bullish": bool, "bearish": bool}.
    Wenn spy_df leer: beide True (neutral -> alles erlaubt).
    """
    if spy_df is None or spy_df.empty or len(spy_df) < ema_period:
        return {"bullish": True, "bearish": True}
    close = spy_df["Close"]
    e = ema(close, ema_period)
    last_close = float(close.iloc[-1])
    last_ema = float(e.iloc[-1])
    return {"bullish": last_close > last_ema,
            "bearish": last_close < last_ema}


def gap_filter(today_open: float, prev_close: float,
               max_gap_pct: float = 0.03) -> bool:
    """True wenn Gap klein genug (Trading erlaubt)."""
    if prev_close <= 0:
        return True
    return abs(today_open - prev_close) / prev_close <= max_gap_pct


def volume_confirmed(volume_ratio: float, threshold: float = 1.3) -> bool:
    return volume_ratio >= threshold


# ─────────────────────────── Time-Decay / Cutoff ────────────────────────────

_DEFAULT_DECAY_BRACKETS = [(30, 1.00), (90, 0.85), (180, 0.65)]


def time_decay_factor(bar_time_et: time,
                      brackets: Optional[list[tuple[int, float]]] = None,
                      late_factor: float = 0.40) -> float:
    """Gewichtung abhängig von Minuten seit Opening (9:30 ET).

    Default-Brackets: <=30m -> 1.00, <=90m -> 0.85, <=180m -> 0.65,
    alles danach -> late_factor (0.40).
    """
    minutes_since_open = (bar_time_et.hour * 60 + bar_time_et.minute) - 570
    # 9:30 ET = 570 Minuten. Breakouts > ORB-End (z.B. 10:00) -> 30 Min.
    brackets = brackets if brackets else _DEFAULT_DECAY_BRACKETS
    for threshold, weight in brackets:
        if minutes_since_open - 30 <= threshold:  # minus ORB-Länge (Prime Window startet nach ORB-Ende)
            return weight
    return late_factor


def entry_cutoff_ok(dt_obj: datetime, cutoff: Optional[time]) -> bool:
    """True wenn neuer Entry erlaubt (keine Sperre oder vor Cutoff)."""
    if cutoff is None:
        return True
    return to_et_time(dt_obj) < cutoff


# ─────────────────────────── VIX Term Structure ─────────────────────────────

def vix_term_structure_regime(
    vix_spot: float,
    vix_3m: Optional[float] = None,
    flat_lower: float = 0.90,
    flat_upper: float = 1.00,
    backwd_upper: float = 1.15,
) -> tuple[str, float, str]:
    """VIX/VIX3M-Ratio -> Regime-Klassifikation.

    Returns (regime, size_multiplier, reason).
    """
    if vix_spot <= 0:
        vix_spot = 20.0
    if vix_3m is None or vix_3m <= 0:
        vix_3m = vix_spot * 1.02
    ratio = vix_spot / vix_3m if vix_3m > 0 else 1.0

    if ratio < flat_lower:
        return "contango", 1.00, f"Contango ({ratio:.2f}): volle Positionsgrößen"
    if ratio < flat_upper:
        return "flat", 0.75, f"Flat ({ratio:.2f}): 75%"
    if ratio < backwd_upper:
        return "backwardation", 0.50, f"Backwardation ({ratio:.2f}): 50%"
    return "extreme_backwardation", 0.00, f"Extreme Backwardation ({ratio:.2f}): nur Shorts"


# ─────────────────────────── Botti: VIX / Drawdown / Sector / Volume ────────

def vix_size_factor(vix: float, threshold: float = 30.0) -> float:
    """Positionsgrößen-Multiplikator abhängig vom VIX-Level.

    Gibt 0.5 zurück wenn vix > threshold, sonst 1.0.
    """
    return 0.5 if vix > threshold else 1.0


def drawdown_breaker(equity: float, peak_equity: float,
                     max_dd_pct: float = 0.15) -> bool:
    """True wenn Drawdown die Schwelle überschreitet (Trading gesperrt)."""
    if peak_equity <= 0:
        return False
    dd = (peak_equity - equity) / peak_equity
    return dd >= max_dd_pct


def sector_cluster_ok(
    symbol: str,
    open_symbols: list[str],
    sector_map: dict[str, list[str]],
    max_per_sector: int = 2,
) -> bool:
    """True wenn Symbol keinen Sektor-Klumpen erzeugt.

    Prüft ob bereits max_per_sector Positionen im gleichen Sektor offen sind.
    """
    sector = ""
    for name, members in sector_map.items():
        if symbol in members:
            sector = name
            break
    if not sector:
        return True
    count = sum(1 for s in open_symbols if s in sector_map.get(sector, []))
    return count < max_per_sector


def volume_guard_ok(volume: float, vol_sma: float,
                    max_vol_pct: float = 0.01) -> bool:
    """True wenn das geplante Ordervolumen nicht zu groß relativ zum SMA ist.

    Prüft: volume * max_vol_pct als Guard. In der Praxis wird geprüft, ob
    Volume > Vol_SMA (Mindest-Liquidität vorhanden).
    """
    if vol_sma <= 0:
        return False
    return volume <= vol_sma * (1.0 / max_vol_pct) if max_vol_pct > 0 else True


# ─────────────────────────── Breakout / Breakdown ───────────────────────────

def check_breakout(
    price: float,
    orb_high: float,
    orb_low: float,
    orb_range: float,
    multiplier: float = 1.0,
    volume_ok: bool = False,
) -> tuple[str, float]:
    """Kern-Breakout-Prüfung für ORB.

    Returns (side, strength). side in {"long","short",""}.
    """
    if orb_range <= 0:
        return "", 0.0
    up = orb_high + (multiplier - 1.0) * orb_range
    dn = orb_low - (multiplier - 1.0) * orb_range

    if price > up:
        strength = min((price - orb_high) / orb_range, 1.0)
        if volume_ok:
            strength = min(strength * 1.2, 1.0)
        return "long", strength
    if price < dn:
        strength = min((orb_low - price) / orb_range, 1.0)
        if volume_ok:
            strength = min(strength * 1.2, 1.0)
        return "short", strength
    return "", 0.0


# ─────────────────────────── MIT Independence Guard ─────────────────────────

def correlation_group(symbol: str, groups: dict[str, list[str]]) -> str:
    for name, members in groups.items():
        if symbol in members:
            return name
    return ""


def mit_independence_blocked(
    symbol: str,
    open_symbols: list[str],
    reserved_groups: set[str],
    groups: dict[str, list[str]],
) -> tuple[bool, str]:
    """True wenn Symbol blockiert ist, weil eine korrelierte Gruppe bereits
    offen/reserviert ist."""
    group = correlation_group(symbol, groups)
    if not group:
        return False, ""
    for s in open_symbols:
        if correlation_group(s, groups) == group:
            return True, f"MIT Independence: Gruppe {group} bereits offen"
    if group in reserved_groups:
        return True, f"MIT Independence: Gruppe {group} heute bereits genutzt"
    return False, ""


# ─────────────────────────── Intraday Trade Window ──────────────────────────


def is_within_trade_window(
    now: datetime,
    open_time: datetime,
    window_minutes: int = 90,
) -> bool:
    """True wenn 'now' innerhalb der ersten window_minutes nach open_time liegt.

    Genutzt z.B. von der Quick-Flip-Strategie, die nur innerhalb der ersten
    90 Min nach Market-Open Trades eingehen darf. Akzeptiert naive und
    timezone-aware datetimes (ET-Konvertierung intern).
    """
    if window_minutes <= 0:
        return False
    now_et = to_et(now)
    open_et = to_et(open_time)
    delta_min = (now_et - open_et).total_seconds() / 60.0
    return 0.0 <= delta_min <= float(window_minutes)


def _cfg_time(cfg: dict | object, key: str, default: time) -> time:
    """Lese Zeitfeld robust aus dict/Objekt; akzeptiert time oder 'HH:MM'."""
    val = None
    if isinstance(cfg, dict):
        val = cfg.get(key, default)
    else:
        val = getattr(cfg, key, default)
    if isinstance(val, time):
        return val
    if isinstance(val, str) and ":" in val:
        hh, mm = val.split(":", 1)
        return time(int(hh), int(mm))
    return default


def _cfg_int(cfg: dict | object, key: str, default: int) -> int:
    if isinstance(cfg, dict):
        val = cfg.get(key, default)
    else:
        val = getattr(cfg, key, default)
    try:
        return int(val)
    except Exception:
        return int(default)


def is_before_premarket(cfg: dict | object, now: datetime) -> bool:
    """True wenn aktuelle Zeit vor premarket_time liegt (ET)."""
    now_et = to_et(now).time()
    premarket_t = _cfg_time(cfg, "premarket_time", time(9, 0))
    return now_et < premarket_t


def is_after_entry_cutoff(cfg: dict | object, now: datetime) -> bool:
    """True wenn aktuelle Zeit nach Entry-Cutoff liegt (ET)."""
    now_et = to_et(now).time()
    cutoff_t = _cfg_time(cfg, "entry_cutoff_time", time(15, 0))
    return now_et >= cutoff_t


def is_after_eod_close(cfg: dict | object, now: datetime) -> bool:
    """True wenn aktuelle Zeit nach EOD-Close liegt (ET)."""
    now_et = to_et(now).time()
    close_t = _cfg_time(cfg, "eod_close_time", time(15, 27))
    return now_et >= close_t


def in_regular_trade_window(cfg: dict | object, now: datetime) -> bool:
    """True wenn 'now' zwischen Market-Open und Entry-Cutoff liegt (ET)."""
    now_et = to_et(now)
    open_t = _cfg_time(cfg, "market_open_time", time(9, 30))
    open_dt = now_et.replace(
        hour=open_t.hour,
        minute=open_t.minute,
        second=0,
        microsecond=0,
    )
    minutes = _cfg_int(cfg, "trade_window_minutes", 390)
    return is_within_trade_window(
        now=now_et,
        open_time=open_dt,
        window_minutes=minutes,
    )

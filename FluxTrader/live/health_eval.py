"""Gemeinsame Auswertung fuer Liveness-/Health-Zustaende."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Any, Optional

from core.filters import (
    is_after_entry_cutoff,
    is_after_eod_close,
    is_always_on_market,
    is_before_premarket,
    is_within_trade_window,
    timeframe_to_seconds,
    to_et,
)


def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def next_expected_bar_at(*,
                         last_bar_ts: Optional[datetime],
                         bar_timeframe_seconds: int,
                         provider_poll_interval_s: int,
                         stale_tolerance_s: int) -> Optional[datetime]:
    if last_bar_ts is None:
        return None
    return last_bar_ts + timedelta(
        seconds=max(1, bar_timeframe_seconds)
        + max(0, provider_poll_interval_s)
        + max(0, stale_tolerance_s)
    )


def trade_window_phase(strategy_cfg: dict[str, Any], now: datetime) -> str:
    if is_always_on_market(strategy_cfg):
        return "in_window"
    now_utc = now.astimezone(timezone.utc)
    if is_before_premarket(strategy_cfg, now_utc):
        return "before_premarket"
    if is_after_eod_close(strategy_cfg, now_utc):
        return "after_eod"
    if is_after_entry_cutoff(strategy_cfg, now_utc):
        return "after_cutoff"

    now_et = to_et(now_utc)
    open_t = _cfg_time(
        strategy_cfg, "market_open_time", time(9, 30), aliases=("market_open",),  # market_open = legacy-Alias älterer Configs
    )
    open_dt = now_et.replace(
        hour=open_t.hour,
        minute=open_t.minute,
        second=0,
        microsecond=0,
    )
    cutoff_t = _cfg_time(strategy_cfg, "entry_cutoff_time", time(15, 0))
    window_minutes = max(
        1,
        (cutoff_t.hour * 60 + cutoff_t.minute) - (open_t.hour * 60 + open_t.minute),
    )
    if is_within_trade_window(now_et, open_dt, window_minutes=window_minutes):
        return "in_window"
    return "out_of_window"


def evaluate_liveness(*,
                      row: dict[str, Any],
                      strategy_cfg: dict[str, Any],
                      monitoring_cfg: Any,
                      now: datetime) -> dict[str, Any]:
    # bar_timeframe_seconds: expliziter Wert (>0) gewinnt, sonst aus
    # strategy.timeframe ableiten. Stale-Tolerance fällt auf 25 % des
    # Bar-Intervalls (min. 60 s) zurück, wenn nicht explizit gesetzt.
    explicit_bar_tf = int(
        strategy_cfg.get("bar_timeframe_seconds")
        or getattr(monitoring_cfg, "bar_timeframe_seconds", 0)
        or 0
    )
    if explicit_bar_tf > 0:
        bar_timeframe_seconds = explicit_bar_tf
    else:
        tf_label = str(strategy_cfg.get("timeframe") or "5Min")
        bar_timeframe_seconds = int(timeframe_to_seconds(tf_label))
    provider_poll_interval_s = int(
        strategy_cfg.get("provider_poll_interval_s")
        or getattr(monitoring_cfg, "provider_poll_interval_s", 30)
    )
    explicit_stale = int(
        strategy_cfg.get("stale_tolerance_s")
        or getattr(monitoring_cfg, "stale_tolerance_s", 0)
        or 0
    )
    if explicit_stale > 0:
        stale_tolerance_s = explicit_stale
    else:
        stale_tolerance_s = max(60, int(bar_timeframe_seconds * 0.25))
    grace_period_s = int(getattr(monitoring_cfg, "grace_period_s", 90))
    watchdog_interval_s = int(getattr(monitoring_cfg, "watchdog_interval_s", 15))

    phase = trade_window_phase(strategy_cfg, now)
    in_trade_window = (phase == "in_window") or _phase_alert_enabled(phase, monitoring_cfg)

    last_watchdog_ts = parse_iso(row.get("last_watchdog_ts"))
    last_bar_ts = parse_iso(row.get("last_bar_ts"))
    expected = next_expected_bar_at(
        last_bar_ts=last_bar_ts,
        bar_timeframe_seconds=bar_timeframe_seconds,
        provider_poll_interval_s=provider_poll_interval_s,
        stale_tolerance_s=stale_tolerance_s,
    )

    process_alive = True
    if last_watchdog_ts is not None:
        process_alive = (now - last_watchdog_ts).total_seconds() < (3 * max(1, watchdog_interval_s))

    data_flowing = True
    if in_trade_window:
        if expected is None:
            data_flowing = False
        else:
            data_flowing = now <= (expected + timedelta(seconds=max(0, grace_period_s)))

    if bool(row.get("circuit_breaker", False)):
        overall = "CIRCUIT_BREAK"
    elif not process_alive:
        overall = "PROCESS_DEAD"
    elif not in_trade_window:
        overall = "IDLE_OUT_OF_WINDOW"
    elif not data_flowing:
        overall = "DATA_STALE"
    else:
        overall = "OK"

    if is_always_on_market(strategy_cfg):
        trade_window_payload = {
            "start": "00:00",
            "end": "24:00",
            "phase": phase,
        }
    else:
        start_t = _cfg_time(
            strategy_cfg, "market_open_time", time(9, 30), aliases=("market_open",),  # market_open = legacy-Alias älterer Configs
        )
        end_t = _cfg_time(strategy_cfg, "entry_cutoff_time", time(15, 0))
        trade_window_payload = {
            "start": f"{start_t.hour:02d}:{start_t.minute:02d}",
            "end": f"{end_t.hour:02d}:{end_t.minute:02d}",
            "phase": phase,
        }
    seconds_to_next = None
    if expected is not None:
        seconds_to_next = int((expected - now).total_seconds())

    return {
        "overall_state": overall,
        "process_alive": process_alive,
        "data_flowing": data_flowing,
        "in_trade_window": in_trade_window,
        "last_watchdog_ts": last_watchdog_ts.isoformat() if last_watchdog_ts else None,
        "last_bar_ts": last_bar_ts.isoformat() if last_bar_ts else None,
        "next_expected_bar_at": expected.isoformat() if expected else None,
        "seconds_to_next_bar": seconds_to_next,
        "trade_window": trade_window_payload,
    }


def _cfg_time(cfg: dict[str, Any],
              key: str,
              default: time,
              aliases: tuple[str, ...] = ()) -> time:
    raw = None
    for candidate in (key, *aliases):
        if candidate in cfg:
            raw = cfg.get(candidate)
            break
    if raw is None:
        return default
    if isinstance(raw, time):
        return raw
    if isinstance(raw, str) and ":" in raw:
        hh, mm = raw.split(":", 1)
        return time(int(hh), int(mm))
    return default


def _phase_alert_enabled(phase: str, monitoring_cfg: Any) -> bool:
    phases = getattr(monitoring_cfg, "trade_window_phases", None)
    if phases is None:
        return False
    mapping = {
        "before_premarket": "premarket_alert",
        "after_cutoff": "after_cutoff_alert",
        "after_eod": "after_eod_alert",
    }
    attr = mapping.get(phase)
    if attr is None:
        return False
    return bool(getattr(phases, attr, False))

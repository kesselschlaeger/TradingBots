#!/usr/bin/env python3
"""
orb_backtest.py – Backtest-Engine für ORB Bot v2.

Intraday-Backtest auf 5-Minuten-Bars mit:
  - Long-Breakout + Short-Breakdown
  - R-basiertes Exit-System (Profit Target, Stop Loss, Trailing Stop)
  - Guards: VIX-Regime, Drawdown Circuit Breaker, Volume-Guard, Max Daily Trades
  - Trend-Filter (SPY EMA-20) – Fix #5
  - Gap-Filter (Overnight-Gaps) – Fix #7
  - Tageszeitspezifische Volume-Ratio – Fix #6
  - Stop-Loss an ORB-Range – Fix #9
  - Metriken lokal berechnet (kein v7-Import)
  - Trades als Excel (.xlsx) statt CSV

Nutzung:
    from orb_backtest import load_orb_data, run_orb_backtest, print_orb_report

    data, vix, vix3m = load_orb_data(symbols, "2024-01-01", "2025-04-01", alpaca)
    pf, report = run_orb_backtest(data, vix, ORB_CONFIG, vix3m_series=vix3m)
    print_orb_report(report, output_dir=Path("orb_trading_data"))
"""

from __future__ import annotations

import warnings
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

warnings.filterwarnings("ignore")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.dates import DateFormatter, AutoDateLocator
    MPL_AVAILABLE = True
except ImportError:
    MPL_AVAILABLE = False

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False

try:
    import openpyxl  # noqa: F401
    XLSX_AVAILABLE = True
except ImportError:
    XLSX_AVAILABLE = False

# Shared strategy module – Single Source of Truth
from orb_strategy import (
    ET,
    check_entry_cutoff,
    calculate_orb_levels,
    calculate_stop,
    check_gap_filter,
    check_trend_filter,
    compute_indicators,
    compute_orb_signals,
    compute_orb_volume_ratio,
    mit_apply_overlay as _mit_apply_overlay,
    mit_group_for_symbol as _mit_group_for_symbol,
    to_et,
    to_et_time,
)


# ---------------------------------------------------------------------------
# 1. Datenladen
# ---------------------------------------------------------------------------

def load_orb_data(
    symbols: List[str],
    start_date: str,
    end_date: str,
    alpaca=None,
) -> Tuple[Dict[str, pd.DataFrame], pd.Series, pd.Series]:
    """
    Lade 5-Minuten-Bars für alle Symbole + VIX/VIX3M-Tageskurse.

    SPY wird automatisch mitgeladen (für Trendfilter),
    auch wenn nicht in der Symbol-Liste.

    Parameter
    ---------
    symbols    : Ticker-Liste
    start_date : "YYYY-MM-DD"
    end_date   : "YYYY-MM-DD"
    alpaca     : AlpacaClient-Instanz (aus orb_bot_alpaca.py)

    Rückgabe
    --------
    (data_dict, vix_series, vix3m_series)
    """
    data_dict: Dict[str, pd.DataFrame] = {}

    # SPY automatisch hinzufügen für Trendfilter
    all_symbols = list(dict.fromkeys(symbols))  # preserve order, deduplicate
    if "SPY" not in all_symbols:
        all_symbols.append("SPY")

    if alpaca is not None:
        print(f"[orb_backtest] Lade {len(all_symbols)} Symbole via Alpaca (5m) ...")
        raw = alpaca.fetch_bars_bulk(all_symbols, start_date, end_date)
        for sym, df in raw.items():
            if not df.empty and len(df) > 100:
                data_dict[sym] = compute_indicators(df)
                idx = data_dict[sym].index
                if idx.tz is None:
                    idx_et = idx.tz_localize("UTC").tz_convert(ET)
                else:
                    idx_et = idx.tz_convert(ET)
                min_d = idx_et.min().date()
                max_d = idx_et.max().date()
                print(f"  [range] {sym}: {min_d} -> {max_d} ({len(data_dict[sym])} Bars)")
    else:
        print("[orb_backtest] Kein Alpaca-Client -> kein Datenabruf moeglich.")

    # VIX laden (daily)
    vix_series = pd.Series(dtype=float, name="VIX")
    if YF_AVAILABLE:
        try:
            days = (datetime.strptime(end_date, "%Y-%m-%d") -
                    datetime.strptime(start_date, "%Y-%m-%d")).days + 100
            vix_df = yf.Ticker("^VIX").history(period=f"{days}d")
            vix_series = vix_df["Close"].rename("VIX")
            # Timezone normalisieren
            if vix_series.index.tz is not None:
                vix_series.index = vix_series.index.tz_localize(None)
        except Exception as e:
            print(f"[orb_backtest] VIX-Load Fehler: {e}")

    # Fix #16: VIX3M laden für Term Structure Regime
    vix3m_series = pd.Series(dtype=float, name="VIX3M")
    if YF_AVAILABLE:
        try:
            days = (datetime.strptime(end_date, "%Y-%m-%d") -
                    datetime.strptime(start_date, "%Y-%m-%d")).days + 100
            vix3m_df = yf.Ticker("^VIX3M").history(period=f"{days}d")
            if not vix3m_df.empty:
                vix3m_series = vix3m_df["Close"].rename("VIX3M")
                if vix3m_series.index.tz is not None:
                    vix3m_series.index = vix3m_series.index.tz_localize(None)
                print(f"[orb_backtest] VIX3M geladen: {len(vix3m_series)} Tage")
            else:
                print("[orb_backtest] VIX3M: keine Daten (Fallback auf VIX×1.02)")
        except Exception as e:
            print(f"[orb_backtest] VIX3M-Load Fehler (Fallback aktiv): {e}")

    print(f"[orb_backtest] {len(data_dict)} Symbole geladen.")
    return data_dict, vix_series, vix3m_series


# ---------------------------------------------------------------------------
# 2. Backtest-Simulation
# ---------------------------------------------------------------------------

def run_orb_backtest(
    data_dict: Dict[str, pd.DataFrame],
    vix_series: pd.Series,
    cfg: dict,
    vix3m_series: Optional[pd.Series] = None,
) -> Tuple[None, dict]:
    """
    Führe den ORB-Backtest aus (Bar-by-Bar auf 5m-Daten).

    Guards:
      - VIX-Regime (Positionsgrößen halbieren wenn VIX ≥ threshold)
      - Drawdown Circuit Breaker (keine neuen Trades bei DD > max_drawdown_pct)
      - Volume-Guard (max_volume_pct × avg_volume)
      - Max Daily Trades

    Rückgabe
    --------
    (None, report_dict)   – kein vbt.Portfolio-Objekt (Intraday-Logik)
    """
    if not data_dict:
        raise ValueError("Keine Daten für Backtest.")

    # ── Config-Parameter ──────────────────────────────────────────────────
    capital       = float(cfg.get("initial_capital", 10_000.0))
    commission    = float(cfg.get("commission_pct", 0.00005))
    slippage      = float(cfg.get("slippage_pct", 0.0002))
    risk_pt       = float(cfg.get("risk_per_trade", 0.01))
    max_daily     = int(cfg.get("max_daily_trades", 3))
    profit_r      = float(cfg.get("profit_target_r", 2.0))
    sl_r          = float(cfg.get("stop_loss_r", 1.0))
    trail_after_r = float(cfg.get("trail_after_r", 1.0))
    trail_dist_r  = float(cfg.get("trail_distance_r", 0.5))
    max_dd        = float(cfg.get("max_drawdown_pct", 0.15))
    dd_cooldown_days = int(cfg.get("dd_cooldown_days", 10))
    vix_thresh    = float(cfg.get("vix_high_threshold", 30))
    vix_reduce    = float(cfg.get("vix_size_reduction", 0.5))
    max_vol_pct   = float(cfg.get("max_volume_pct", 0.01))
    allow_shorts  = bool(cfg.get("allow_shorts", False))
    orb_minutes   = int(cfg.get("opening_range_minutes", 30))
    min_strength  = float(cfg.get("min_signal_strength", 0.3))
    avoid_fri     = bool(cfg.get("avoid_fridays", True))
    avoid_mon     = bool(cfg.get("avoid_mondays", False))
    eod_close     = cfg.get("eod_close_time", time(15, 27))  # Fix #8

    print("[orb_backtest] Starte Simulation ...")

    # ── Alle einzigartigen Handelstage ermitteln ──────────────────────────
    all_dates = set()
    for df in data_dict.values():
        idx = df.index
        if idx.tz is not None:
            idx_et = idx.tz_convert(ET)
        else:
            idx_et = idx.tz_localize("UTC").tz_convert(ET)
        all_dates.update(idx_et.date)
    trading_days = sorted(all_dates)

    # ── VIX als Dict { date → float } ────────────────────────────────────
    vix_dict: Dict = {}
    if not vix_series.empty:
        for ts, val in vix_series.items():
            d = ts.date() if hasattr(ts, "date") else ts
            vix_dict[d] = float(val)

    # Fix #16: VIX3M als Dict { date → float }
    vix3m_dict: Dict = {}
    if vix3m_series is not None and not vix3m_series.empty:
        for ts, val in vix3m_series.items():
            d = ts.date() if hasattr(ts, "date") else ts
            vix3m_dict[d] = float(val)

    # ── Feature-Status ausgeben ───────────────────────────────────────────
    _dd_scaling = cfg.get('use_dynamic_kelly_dd_scaling', False)
    _vix_ts = cfg.get('use_vix_term_structure', False)
    _mit_overlay = cfg.get('use_mit_probabilistic_overlay', False)
    print(f"[orb_backtest] MIT-Overlay: {'AN' if _mit_overlay else 'AUS'}"
          f" | DD-Scaling(#15): {'AN' if _dd_scaling else 'AUS'}"
          f" | VIX-Regime(#16): {'AN' if _vix_ts else 'AUS'}"
          f" (VIX3M-Daten: {len(vix3m_dict)} Tage)")

    # ── Filter-Config ─────────────────────────────────────────────────────
    use_trend  = bool(cfg.get("use_trend_filter", True))
    use_gap    = bool(cfg.get("use_gap_filter", True))
    max_gap    = float(cfg.get("max_gap_pct", 0.03))
    ema_period = int(cfg.get("trend_ema_period", 20))
    vol_lb     = int(cfg.get("volume_lookback_days", 10))

    # SPY-Daten für Trendfilter (wird pro Tag gesliced)
    spy_df_full = data_dict.get("SPY")

    # ── Simulation ────────────────────────────────────────────────────────
    cash = capital
    positions: Dict[str, dict] = {}       # sym → position-dict
    trades_list: List[dict] = []
    equity_daily: List[dict] = []         # [{"date": timestamp, "equity": float}, ...]
    peak_equity = capital
    dd_pause_until = None
    r_multiples: List[float] = []
    holding_bars_list: List[int] = []

    symbols = [s for s in data_dict.keys() if s != "SPY" or "SPY" in [cfg.get("benchmark")]]

    for day in trading_days:
        weekday = day.weekday()
        if weekday >= 5:
            continue
        if avoid_fri and weekday == 4:
            continue
        if avoid_mon and weekday == 0:
            continue

        trades_today = 0
        reserved_mit_groups = set()

        # VIX für heute (ffill: nächstliegenden Wert nehmen)
        vix_val = vix_dict.get(day, 20.0)
        if vix_val == 0:
            vix_val = 20.0
        size_mult = vix_reduce if vix_val >= vix_thresh else 1.0

        # Fix #16: VIX3M für heute (None wenn nicht verfügbar → Fallback in Funktion)
        vix3m_val = vix3m_dict.get(day) if vix3m_dict else None

        # Equity & DD check
        unrealized = 0.0
        for sym, pos in positions.items():
            df_sym = data_dict.get(sym)
            if df_sym is not None and not df_sym.empty:
                last_p = _last_price_on_day(df_sym, day)
                if last_p > 0:
                    if pos["side"] == "long":
                        unrealized += pos["shares"] * last_p
                    else:
                        unrealized -= pos["shares"] * last_p

        equity = cash + unrealized
        if equity > peak_equity:
            peak_equity = equity

        # Drawdown Circuit Breaker mit Cooldown statt permanentem Lockout.
        # Fix #15: Berechne current_dd für dynamic_kelly DD-Scaling
        dd_active = False
        current_dd = 0.0
        if dd_pause_until is not None and day <= dd_pause_until:
            dd_active = True
            current_dd = (peak_equity - equity) / (peak_equity + 1e-9)
        else:
            current_dd = (peak_equity - equity) / (peak_equity + 1e-9)
            if current_dd >= max_dd:
                dd_active = True
                dd_pause_until = day + timedelta(days=max(dd_cooldown_days, 0))
                # Nach Trigger den Referenz-Peak auf aktuelle Equity setzen,
                # damit der Breaker den Backtest nicht dauerhaft stilllegt.
                peak_equity = equity

        # ── Trendfilter für den Tag (einmal, nicht pro Symbol) ───────────
        trend = {"bullish": True, "bearish": True}
        if use_trend and spy_df_full is not None:
            spy_day = _extract_day(spy_df_full, day)
            # Verwende letzte ~20 Tage an SPY-Daten für EMA
            if spy_day is not None:
                spy_idx = spy_df_full.index
                spy_up_to = spy_idx[to_et(spy_idx).date <= day]
                if len(spy_up_to) >= ema_period:
                    spy_recent = spy_df_full.loc[spy_up_to[-ema_period * 80:]]
                    trend = check_trend_filter(spy_recent, ema_period)

        # ── Für jedes Symbol: Tages-5m-Bars verarbeiten ──────────────────
        for sym in symbols:
            df_full = data_dict[sym]
            day_df = _extract_day(df_full, day)
            if day_df is None or len(day_df) < 8:
                continue

            # Fix #7: Gap-Filter
            if use_gap:
                prev_day_idx = [d for d in trading_days if d < day]
                if prev_day_idx:
                    prev = _extract_day(df_full, prev_day_idx[-1])
                    if prev is not None and not prev.empty and not day_df.empty:
                        today_open = float(day_df["Open"].iloc[0])
                        prev_close = float(prev["Close"].iloc[-1])
                        if not check_gap_filter(today_open, prev_close, max_gap):
                            continue

            # ORB-Levels berechnen
            orb_high, orb_low, orb_range = calculate_orb_levels(day_df, orb_minutes)
            if orb_range <= 0:
                continue

            # Signals berechnen
            signals = compute_orb_signals(day_df, orb_high, orb_low, orb_range, cfg)

            # Post-ORB-Bars
            idx_et = day_df.index
            if idx_et.tz is None:
                idx_et = idx_et.tz_localize("UTC").tz_convert(ET)
            else:
                idx_et = idx_et.tz_convert(ET)
            orb_end_min = 9 * 60 + 30 + orb_minutes
            hhmm = idx_et.hour * 60 + idx_et.minute
            post_orb_mask = hhmm >= orb_end_min

            entered_today = False
            bar_count = 0
            last_bar_ts = None

            for i, (bar_ts, bar) in enumerate(day_df.iterrows()):
                if not post_orb_mask[i]:
                    continue

                price = float(bar["Close"])
                high = float(bar["High"])
                low = float(bar["Low"])
                bar_count += 1
                last_bar_ts = bar_ts  # Speichere letzten Timestamp des Tages

                # ── Fix #8: EOD-Close – Position schließen bei eod_close_time ──
                bar_et_time = to_et_time(bar_ts)
                if sym in positions and bar_et_time >= eod_close:
                    pos = positions[sym]
                    pos["bars_held"] += 1
                    exit_p = price * (1 - slippage) if pos["side"] == "long" else price * (1 + slippage)
                    if pos["side"] == "long":
                        pnl = (exit_p - pos["entry"]) * pos["shares"]
                        cash += exit_p * pos["shares"] * (1 - commission)
                    else:
                        pnl = (pos["entry"] - exit_p) * pos["shares"]
                        cash -= exit_p * pos["shares"] * (1 + commission)
                    r_mult = pnl / (pos["risk_per_share"] * pos["shares"]) if pos["risk_per_share"] > 0 else 0
                    trades_list.append({
                        "date": bar_ts, "symbol": sym,
                        "side": "SELL" if pos["side"] == "long" else "COVER",
                        "price": exit_p, "shares": pos["shares"],
                        "pnl": pnl, "reason": f"EOD Close ({eod_close.strftime('%H:%M')})",
                        "r_mult": round(r_mult, 2),
                    })
                    r_multiples.append(r_mult)
                    holding_bars_list.append(pos["bars_held"])
                    del positions[sym]
                    continue

                # ── Bestehende Position managen ───────────────────────────
                if sym in positions:
                    pos = positions[sym]
                    pos["bars_held"] += 1

                    if pos["side"] == "long":
                        closed = _manage_long(
                            sym, pos, price, high, low, bar_ts,
                            profit_r, sl_r, trail_after_r, trail_dist_r,
                            slippage, commission,
                        )
                    else:
                        closed = _manage_short(
                            sym, pos, price, high, low, bar_ts,
                            profit_r, sl_r, trail_after_r, trail_dist_r,
                            slippage, commission,
                        )

                    if closed:
                        pnl = closed["pnl"]
                        cash += closed["proceeds"]
                        trades_list.append(closed["trade"])
                        r_multiples.append(closed.get("r_mult", 0.0))
                        holding_bars_list.append(pos["bars_held"])
                        del positions[sym]
                    continue

                # ── Neuer Entry? ──────────────────────────────────────────
                if entered_today or trades_today >= max_daily or dd_active:
                    continue
                if sym in positions:
                    continue

                sig_row = signals.iloc[i]
                is_long = bool(sig_row["entry_long"])
                is_short = bool(sig_row["entry_short"]) and allow_shorts
                strength = float(sig_row["strength"])

                if not is_long and not is_short:
                    continue
                if strength < min_strength:
                    continue

                # Fix #5: Trendfilter
                if is_long and use_trend and not trend["bullish"]:
                    continue
                if is_short and use_trend and not trend["bearish"]:
                    continue

                # Fix #13: Entry-Cutoff (Strength-Decay via compute_orb_signals)
                if not check_entry_cutoff(bar_ts, cfg):
                    continue

                signal = "BUY" if is_long else "SHORT"
                qty_factor = 1.0
                overlay_reason = ""
                if cfg.get("use_mit_probabilistic_overlay", False):
                    bars_so_far = day_df.iloc[: i + 1]
                    volume_ratio = float(bar.get("Volume_Ratio", 1.0) or 1.0)
                    orb_range_pct = (orb_range / orb_low * 100.0) if orb_low > 0 else 0.0
                    ctx = {
                        "volume_ratio": volume_ratio,
                        "volume_confirmed": volume_ratio >= float(cfg.get("volume_multiplier", 1.3)),
                        "orb_range_pct": orb_range_pct,
                        "trend": trend,
                    }
                    should_trade, qty_factor, overlay_reason = _mit_apply_overlay(
                        signal, strength, ctx, bars_so_far, cfg, current_dd, vix_val, vix3m_val
                    )
                    if not should_trade:
                        continue

                    if cfg.get("use_mit_independence_guard", True):
                        group = _mit_group_for_symbol(sym, cfg)
                        if group:
                            open_groups = {
                                _mit_group_for_symbol(open_sym, cfg)
                                for open_sym in positions.keys()
                            }
                            if group in open_groups or group in reserved_mit_groups:
                                continue

                # Volume guard
                vol_ma = float(bar.get("Volume_MA", 0))
                vol = float(bar.get("Volume", 0))
                if vol_ma > 0 and max_vol_pct > 0:
                    max_shares_vol = int(max_vol_pct * vol_ma)
                else:
                    max_shares_vol = 999_999

                side = "long" if is_long else "short"

                # Fix #9: Stop-Loss an ORB-Range statt ATR
                if side == "long":
                    entry_p = price * (1 + slippage)
                    stop = calculate_stop("long", entry_p, orb_high, orb_low,
                                         orb_range, sl_r)
                    risk_per_share = entry_p - stop
                else:
                    entry_p = price * (1 - slippage)
                    stop = calculate_stop("short", entry_p, orb_high, orb_low,
                                         orb_range, sl_r)
                    risk_per_share = stop - entry_p

                if risk_per_share <= 0:
                    continue

                # Position sizing (R-basiert)
                risk_amount = equity * risk_pt * size_mult
                shares = int(risk_amount / risk_per_share)
                shares = min(shares, max_shares_vol)
                shares = max(0, int(shares * max(qty_factor, 0.0)))

                if shares <= 0:
                    continue

                cost = entry_p * shares * (1 + commission)
                if side == "long" and cost > cash:
                    shares = int(cash / (entry_p * (1 + commission)))
                    if shares <= 0:
                        continue

                if side == "long":
                    cash -= entry_p * shares * (1 + commission)
                else:
                    cash += entry_p * shares * (1 - commission)  # Short-Erlös

                target = entry_p + profit_r * risk_per_share if side == "long" \
                    else entry_p - profit_r * risk_per_share

                positions[sym] = {
                    "side": side,
                    "entry": entry_p,
                    "shares": shares,
                    "stop": stop,
                    "target": target,
                    "risk_per_share": risk_per_share,
                    "trail_stop": None,
                    "highest": entry_p if side == "long" else entry_p,
                    "lowest": entry_p if side == "short" else entry_p,
                    "bars_held": 0,
                    "entry_date": day,
                    "entry_ts": bar_ts,
                    "reason": (
                        f"ORB {'Breakout' if side == 'long' else 'Breakdown'} [{strength:.2f}]"
                        + (f" | {overlay_reason}" if overlay_reason else "")
                    ),
                }

                trades_list.append({
                    "date": bar_ts, "symbol": sym,
                    "side": "BUY" if side == "long" else "SHORT",
                    "price": entry_p, "shares": shares,
                    "pnl": 0.0,
                    "reason": positions[sym]["reason"],
                    "r_mult": 0.0,
                    "strength": strength,  # Fix #14: für Kalibrierung
                })

                entered_today = True
                trades_today += 1
                if cfg.get("use_mit_probabilistic_overlay", False):
                    group = _mit_group_for_symbol(sym, cfg)
                    if group:
                        reserved_mit_groups.add(group)

        # ── Equity Snapshot (Positionen werden via Fix #8 EOD-Close geschlossen) ──
        eod_unrealized = 0.0
        for sym, pos in positions.items():
            lp = _last_price_on_day(data_dict.get(sym), day)
            if lp > 0:
                if pos["side"] == "long":
                    eod_unrealized += pos["shares"] * lp
                else:
                    eod_unrealized -= pos["shares"] * lp
        # Verwende letzten Bar-Timestamp des Tages für equity_curve
        # Fallback auf EOD-Timestamp (16:00 ET), falls kein Trade stattfand
        eod_ts = last_bar_ts if last_bar_ts is not None else pd.Timestamp(day, tz=ET).replace(hour=16, minute=0, second=0)
        equity_daily.append({
            "date": eod_ts,
            "equity": cash + eod_unrealized,
        })

    # ── Offene Positionen am Ende schließen ───────────────────────────────
    for sym, pos in list(positions.items()):
        lp = _last_price_all(data_dict.get(sym))
        if lp <= 0:
            continue
        if pos["side"] == "long":
            pnl = (lp - pos["entry"]) * pos["shares"]
            cash += lp * pos["shares"] * (1 - commission - slippage)
        else:
            pnl = (pos["entry"] - lp) * pos["shares"]
            cash -= lp * pos["shares"] * (1 + commission + slippage)
        r_mult = pnl / (pos["risk_per_share"] * pos["shares"]) if pos["risk_per_share"] > 0 else 0
        trades_list.append({
            "date": pd.Timestamp(trading_days[-1]),
            "symbol": sym,
            "side": "SELL" if pos["side"] == "long" else "COVER",
            "price": lp, "shares": pos["shares"],
            "pnl": pnl, "reason": "End of Test",
            "r_mult": round(r_mult, 2),
        })
        r_multiples.append(r_mult)
        holding_bars_list.append(pos["bars_held"])
    positions.clear()

    # ── Report bauen ──────────────────────────────────────────────────────
    trades_df = pd.DataFrame(trades_list)

    if equity_daily:
        eq_df = pd.DataFrame(equity_daily)
        eq_df["date"] = pd.to_datetime(eq_df["date"])
        eq_series = eq_df.set_index("date")["equity"]
    else:
        eq_series = pd.Series(dtype=float, name="equity")

    report = _compute_metrics(eq_series, trades_df, capital)

    report["equity_curve"] = eq_series
    report["equity_curve_df"] = eq_df  # Speichere auch DataFrame für Excel-Export
    report["trades"] = trades_df

    # ORB-spezifische Extras
    if r_multiples:
        report["avg_r_multiple"] = round(np.mean(r_multiples), 2)
    if holding_bars_list:
        report["avg_holding_bars"] = round(np.mean(holding_bars_list), 1)
        report["avg_holding_minutes"] = round(np.mean(holding_bars_list) * 5, 0)

    print(f"[orb_backtest] Simulation abgeschlossen: {len(trades_df)} Trades")
    return None, report


# ---------------------------------------------------------------------------
# 3. Position-Management (Long & Short)
# ---------------------------------------------------------------------------

def _manage_long(
    sym, pos, price, high, low, bar_ts,
    profit_r, sl_r, trail_after_r, trail_dist_r,
    slippage, commission,
) -> Optional[dict]:
    """Prüfe Exit für Long-Position. Gibt dict bei Schließung, sonst None."""
    entry = pos["entry"]
    risk = pos["risk_per_share"]
    stop = pos["stop"]
    target = pos["target"]
    shares = pos["shares"]

    # Stop Loss
    if low <= stop:
        exit_p = stop * (1 - slippage)
        pnl = (exit_p - entry) * shares
        r_mult = (exit_p - entry) / risk if risk > 0 else 0
        return {
            "pnl": pnl,
            "proceeds": exit_p * shares * (1 - commission),
            "r_mult": round(r_mult, 2),
            "trade": {
                "date": bar_ts, "symbol": sym, "side": "SELL",
                "price": exit_p, "shares": shares, "pnl": pnl,
                "reason": "Stop Loss", "r_mult": round(r_mult, 2),
            },
        }

    # Profit Target
    if high >= target:
        exit_p = target * (1 - slippage)
        pnl = (exit_p - entry) * shares
        r_mult = (exit_p - entry) / risk if risk > 0 else 0
        return {
            "pnl": pnl,
            "proceeds": exit_p * shares * (1 - commission),
            "r_mult": round(r_mult, 2),
            "trade": {
                "date": bar_ts, "symbol": sym, "side": "SELL",
                "price": exit_p, "shares": shares, "pnl": pnl,
                "reason": f"Profit Target ({r_mult:.1f}R)",
                "r_mult": round(r_mult, 2),
            },
        }

    # Trailing Stop
    if price > pos["highest"]:
        pos["highest"] = price

    r_mult_now = (price - entry) / risk if risk > 0 else 0
    if r_mult_now >= trail_after_r:
        new_trail = pos["highest"] - trail_dist_r * risk
        if pos["trail_stop"] is None or new_trail > pos["trail_stop"]:
            pos["trail_stop"] = new_trail

    if pos["trail_stop"] is not None and low <= pos["trail_stop"]:
        exit_p = pos["trail_stop"] * (1 - slippage)
        pnl = (exit_p - entry) * shares
        r_mult = (exit_p - entry) / risk if risk > 0 else 0
        return {
            "pnl": pnl,
            "proceeds": exit_p * shares * (1 - commission),
            "r_mult": round(r_mult, 2),
            "trade": {
                "date": bar_ts, "symbol": sym, "side": "SELL",
                "price": exit_p, "shares": shares, "pnl": pnl,
                "reason": "Trailing Stop", "r_mult": round(r_mult, 2),
            },
        }

    return None


def _manage_short(
    sym, pos, price, high, low, bar_ts,
    profit_r, sl_r, trail_after_r, trail_dist_r,
    slippage, commission,
) -> Optional[dict]:
    """Prüfe Exit für Short-Position. Gibt dict bei Schließung, sonst None."""
    entry = pos["entry"]
    risk = pos["risk_per_share"]
    stop = pos["stop"]
    target = pos["target"]
    shares = pos["shares"]

    # Stop Loss (über Entry)
    if high >= stop:
        exit_p = stop * (1 + slippage)
        pnl = (entry - exit_p) * shares
        r_mult = (entry - exit_p) / risk if risk > 0 else 0
        return {
            "pnl": pnl,
            "proceeds": -exit_p * shares * (1 + commission),
            "r_mult": round(r_mult, 2),
            "trade": {
                "date": bar_ts, "symbol": sym, "side": "COVER",
                "price": exit_p, "shares": shares, "pnl": pnl,
                "reason": "Stop Loss", "r_mult": round(r_mult, 2),
            },
        }

    # Profit Target (unter Entry)
    if low <= target:
        exit_p = target * (1 + slippage)
        pnl = (entry - exit_p) * shares
        r_mult = (entry - exit_p) / risk if risk > 0 else 0
        return {
            "pnl": pnl,
            "proceeds": -exit_p * shares * (1 + commission),
            "r_mult": round(r_mult, 2),
            "trade": {
                "date": bar_ts, "symbol": sym, "side": "COVER",
                "price": exit_p, "shares": shares, "pnl": pnl,
                "reason": f"Profit Target ({r_mult:.1f}R)",
                "r_mult": round(r_mult, 2),
            },
        }

    # Trailing Stop
    if price < pos.get("lowest", entry):
        pos["lowest"] = price

    r_mult_now = (entry - price) / risk if risk > 0 else 0
    if r_mult_now >= trail_after_r:
        new_trail = pos["lowest"] + trail_dist_r * risk
        if pos["trail_stop"] is None or new_trail < pos["trail_stop"]:
            pos["trail_stop"] = new_trail

    if pos["trail_stop"] is not None and high >= pos["trail_stop"]:
        exit_p = pos["trail_stop"] * (1 + slippage)
        pnl = (entry - exit_p) * shares
        r_mult = (entry - exit_p) / risk if risk > 0 else 0
        return {
            "pnl": pnl,
            "proceeds": -exit_p * shares * (1 + commission),
            "r_mult": round(r_mult, 2),
            "trade": {
                "date": bar_ts, "symbol": sym, "side": "COVER",
                "price": exit_p, "shares": shares, "pnl": pnl,
                "reason": "Trailing Stop", "r_mult": round(r_mult, 2),
            },
        }

    return None


# ---------------------------------------------------------------------------
# 4. Hilfsfunktionen
# ---------------------------------------------------------------------------

def _extract_day(df: pd.DataFrame, day) -> Optional[pd.DataFrame]:
    """Extrahiere 5m-Bars für einen einzelnen Kalendertag."""
    idx = df.index
    if idx.tz is not None:
        idx_et = idx.tz_convert(ET)
    else:
        idx_et = idx.tz_localize("UTC").tz_convert(ET)
    mask = idx_et.date == day
    sub = df[mask]
    return sub if len(sub) >= 8 else None


def _last_price_on_day(df: Optional[pd.DataFrame], day) -> float:
    """Letzter Close-Kurs eines Tages."""
    if df is None or df.empty:
        return 0.0
    sub = _extract_day(df, day)
    if sub is not None and not sub.empty:
        return float(sub["Close"].iloc[-1])
    return 0.0


def _last_price_all(df: Optional[pd.DataFrame]) -> float:
    """Allerletzter Close-Kurs im DataFrame."""
    if df is None or df.empty:
        return 0.0
    return float(df["Close"].iloc[-1])


def _compute_metrics(eq: pd.Series, trades: pd.DataFrame, capital: float) -> dict:
    """Metriken berechnen (lokale Implementierung, kein v7-Import)."""
    if eq.empty or capital == 0:
        return {}
    total_ret = (eq.iloc[-1] - capital) / capital
    n_years = max((eq.index[-1] - eq.index[0]).days / 365.25, 1e-6)
    cagr = (1 + total_ret) ** (1 / n_years) - 1
    returns = eq.pct_change().dropna()
    sharpe = (returns.mean() / (returns.std() + 1e-9)) * np.sqrt(252)
    neg = returns[returns < 0]
    sortino = (returns.mean() / (neg.std() + 1e-9)) * np.sqrt(252) if len(neg) > 0 else np.nan
    rolling_max = eq.cummax()
    dd = (eq - rolling_max) / (rolling_max + 1e-9)
    max_dd = float(dd.min())
    calmar = cagr / (abs(max_dd) + 1e-9)

    if trades.empty or "pnl" not in trades.columns:
        win_rate = profit_factor = total_trades = 0.0
    else:
        sells = trades[trades["side"].isin(["SELL", "COVER"])]
        wins = sells[sells["pnl"] > 0]["pnl"]
        losses = sells[sells["pnl"] <= 0]["pnl"]
        total_trades = len(sells)
        win_rate = len(wins) / (total_trades + 1e-9)
        profit_factor = wins.sum() / (abs(losses.sum()) + 1e-9)

    return {
        "total_return_pct": round(total_ret * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 3),
        "sortino": round(sortino, 3) if not np.isnan(sortino) else None,
        "max_drawdown_pct": round(max_dd * 100, 2),
        "calmar": round(calmar, 3),
        "win_rate_pct": round(win_rate * 100, 2),
        "profit_factor": round(profit_factor, 3),
        "total_trades": int(total_trades),
    }


# ---------------------------------------------------------------------------
# 5. Report
# ---------------------------------------------------------------------------

def print_orb_report(
    report: dict,
    benchmark: Optional[pd.DataFrame] = None,
    output_dir: Optional[Path] = None,
) -> None:
    """Drucke ORB Backtest Report und speichere Plots + CSV."""
    eq = report.get("equity_curve", pd.Series())
    trades = report.get("trades", pd.DataFrame())

    sep = "─" * 55
    print(f"\n{sep}")
    print("  BACKTEST REPORT – ORB Bot v2")
    print(sep)
    print(f"  Gesamtrendite:      {report.get('total_return_pct', 0):>8.2f} %")
    print(f"  CAGR:               {report.get('cagr_pct', 0):>8.2f} %")
    print(f"  Sharpe Ratio:       {report.get('sharpe', 0):>8.3f}")
    sortino = report.get('sortino')
    print(f"  Sortino Ratio:      {sortino:>8.3f}" if sortino else "  Sortino Ratio:          n/a")
    print(f"  Max Drawdown:       {report.get('max_drawdown_pct', 0):>8.2f} %")
    print(f"  Calmar Ratio:       {report.get('calmar', 0):>8.3f}")
    print(f"  Win-Rate:           {report.get('win_rate_pct', 0):>8.2f} %")
    print(f"  Profit Factor:      {report.get('profit_factor', 0):>8.3f}")
    print(f"  Trades gesamt:      {report.get('total_trades', 0):>8d}")
    if "avg_r_multiple" in report:
        print(f"  Ø R-Multiple:       {report['avg_r_multiple']:>8.2f}")
    if "avg_holding_minutes" in report:
        print(f"  Ø Haltedauer:       {report['avg_holding_minutes']:>6.0f} min ({report.get('avg_holding_bars', 0):.0f} Bars)")
    print(sep)

    # Trade-Verteilung
    if not trades.empty and "reason" in trades.columns:
        if "date" in trades.columns:
            dates_utc = pd.to_datetime(trades["date"], errors="coerce", utc=True)
            years = dates_utc.dropna().dt.year.value_counts().sort_index()
            if not years.empty:
                print("\n  Jahresverteilung:")
                for year, cnt in years.items():
                    print(f"    {year}: {cnt}")

        entries = trades[trades["side"].isin(["BUY", "SHORT"])]
        if not entries.empty:
            print("\n  Entry-Verteilung:")
            for reason, cnt in entries["reason"].value_counts().items():
                print(f"    {reason:<40} {cnt:>4}")

        exits = trades[trades["side"].isin(["SELL", "COVER"])]
        if not exits.empty:
            print("\n  Exit-Verteilung:")
            for reason, cnt in exits["reason"].value_counts().items():
                print(f"    {reason:<40} {cnt:>4}")

    if output_dir is None:
        output_dir = Path(__file__).parent / "orb_trading_data"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Excel (bevorzugt) oder CSV Fallback
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Trade-Liste (mit Timestamps)
    if not trades.empty:
        if XLSX_AVAILABLE:
            xlsx_path = output_dir / f"orb_v2_trades_{ts}.xlsx"
            trades_xlsx = trades.copy()
            for col in trades_xlsx.select_dtypes(include=["datetimetz"]).columns:
                trades_xlsx[col] = trades_xlsx[col].dt.tz_localize(None)
            for col in trades_xlsx.columns:
                if trades_xlsx[col].dtype == object:
                    # Mixed object columns can still contain tz-aware Timestamp/datetime values.
                    # Excel writer rejects them unless timezone info is removed.
                    trades_xlsx[col] = trades_xlsx[col].apply(
                        lambda v: (
                            v.tz_localize(None) if isinstance(v, pd.Timestamp) and v.tzinfo is not None
                            else v.replace(tzinfo=None) if isinstance(v, datetime) and v.tzinfo is not None
                            else v
                        )
                    )
            try:
                trades_xlsx.to_excel(xlsx_path, index=False, engine="openpyxl")
                print(f"  Trade-Liste: {xlsx_path}")
            except PermissionError:
                alt_xlsx_path = output_dir / f"orb_v2_trades_{ts}_retry.xlsx"
                try:
                    trades_xlsx.to_excel(alt_xlsx_path, index=False, engine="openpyxl")
                    print(f"  [WARN] {xlsx_path.name} ist gesperrt (z. B. in Excel geöffnet).")
                    print(f"  Trade-Liste: {alt_xlsx_path}")
                except Exception as e:
                    csv_path = output_dir / f"orb_v2_trades_{ts}.csv"
                    trades.to_csv(csv_path, index=False)
                    print(f"  [WARN] Excel-Export fehlgeschlagen: {e}")
                    print(f"  Trade-Liste (CSV-Fallback): {csv_path}")
            except Exception as e:
                csv_path = output_dir / f"orb_v2_trades_{ts}.csv"
                trades.to_csv(csv_path, index=False)
                print(f"  [WARN] Excel-Export fehlgeschlagen: {e}")
                print(f"  Trade-Liste (CSV-Fallback): {csv_path}")
        else:
            csv_path = output_dir / f"orb_v2_trades_{ts}.csv"
            trades.to_csv(csv_path, index=False)
            print(f"  Trade-Liste: {csv_path}")
            print("  (Hinweis: pip install openpyxl für Excel-Export)")

    # Equity-Kurve Plot
    if MPL_AVAILABLE and not eq.empty:
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(eq.index, eq.values, label="ORB Bot v2", linewidth=1.5)
        ax.set_title("Equity Curve – ORB Bot v2 Backtest")
        ax.set_xlabel("Datum/Zeit (ET)")
        ax.set_ylabel("Equity ($)")
        
        # Zeitstempel-Formatierung auf x-Achse
        ax.xaxis.set_major_locator(AutoDateLocator())
        ax.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d %H:%M"))
        fig.autofmt_xdate(rotation=45, ha='right')
        
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plot_path = output_dir / "orb_v2_equity_curve.png"
        fig.savefig(plot_path, dpi=120)
        plt.close(fig)
        print(f"  Plot:            {plot_path}")

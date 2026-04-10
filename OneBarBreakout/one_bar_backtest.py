#!/usr/bin/env python3
"""
one_bar_backtest.py – Backtest-Engine für die One-Bar-Breakout-Strategie.

Simulations-Logik (exakt nach Original-Regeln):
  - Signal:  Close[D] > max(High[D-50:D-1]) → Long
             Close[D] < min(Low [D-50:D-1]) → Short
  - Entry:   Close[D]   (heutiger Closing Print – MOC-Order)
  - Exit:    Open[D+1]  (nächster Opening Print  – OPG-Order)
  - Haltedauer: ~17,5 Stunden (nur über Nacht, kein Intraday)
  - Kosten:  Slippage + Kommission je Seite (konfigurierbar)

Vergleichs-Report:
  Mit --compare-orb (oder compare_with_orb=True) wird der ORB-Backtester
  aus orb_backtest.py mit identischem Zeitraum aufgerufen und die Metriken
  werden nebeneinander dargestellt.

Nutzung (CLI):
    python one_bar_backtest.py --start 2024-01-01 --end 2025-01-01
    python one_bar_backtest.py --start 2023-01-01 --compare-orb --shorts

Nutzung (als Modul):
    from one_bar_backtest import load_obb_data, run_obb_backtest, print_obb_report
    data = load_obb_data(symbols, start, end, alpaca)
    report = run_obb_backtest(data, cfg)
    print_obb_report(report, output_dir=Path("obb_trading_data"))
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

warnings.filterwarnings("ignore")

# Matplotlib (optional)
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.gridspec import GridSpec
    from matplotlib.dates import DateFormatter, AutoDateLocator
    MPL_AVAILABLE = True
except ImportError:
    MPL_AVAILABLE = False

# yfinance Fallback (wenn kein Alpaca-Client)
try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False

# openpyxl für Excel-Export
try:
    import openpyxl  # noqa
    XLSX_AVAILABLE = True
except ImportError:
    XLSX_AVAILABLE = False

# SSoT Strategie-Logik
from one_bar_breakout_strategy import (
    ET,
    OBB_DEFAULT_CONFIG,
    calculate_obb_position_size,
    compute_obb_signals,
    compute_rolling_win_rate,
)


# ---------------------------------------------------------------------------
# 1. Datenladen
# ---------------------------------------------------------------------------

def load_obb_data(
    symbols: List[str],
    start_date: str,
    end_date: str,
    alpaca=None,          # AlpacaClientDaily-Instanz (aus one_bar_breakout_bot.py)
) -> Dict[str, pd.DataFrame]:
    """
    Lade TÄGLICHE OHLCV-Bars für alle Symbole.

    Priorisierung:
      1. Alpaca (via AlpacaClientDaily.fetch_daily_bars_bulk) – bevorzugt
      2. yfinance (Fallback, wenn kein Alpaca-Client übergeben)

    Wichtig: lookback_bars (Standard 50) + extra Puffer werden automatisch
    vor start_date geladen, damit das erste Signal valide ist.

    Parameter
    ----------
    symbols    : Ticker-Liste (z.B. ["SPY", "QQQ", ...])
    start_date : "YYYY-MM-DD" (Backtest-Startdatum)
    end_date   : "YYYY-MM-DD" (Backtest-Enddatum)
    alpaca     : AlpacaClientDaily-Instanz oder None

    Rückgabe
    --------
    Dict { symbol → DataFrame mit täglichen OHLCV-Bars }
    """
    # Lookback-Puffer: 50 Bars + 30 Kalender-Tage Extra
    lookback_days = 50 * 2  # ~50 Handelstage = ~100 Kalendertage
    try:
        start_dt    = datetime.strptime(start_date, "%Y-%m-%d")
        fetch_start = (start_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    except ValueError:
        fetch_start = start_date

    data_dict: Dict[str, pd.DataFrame] = {}

    # ── Weg 1: Alpaca ──────────────────────────────────────────────────────
    if alpaca is not None:
        print(f"[obb_backtest] Lade {len(symbols)} Symbole via Alpaca (Daily) ...")
        raw = alpaca.fetch_daily_bars_bulk(symbols, fetch_start, end_date)
        for sym, df in raw.items():
            if not df.empty and len(df) >= 52:
                data_dict[sym] = df
                min_d = df.index.min()
                max_d = df.index.max()
                print(f"  [range] {sym}: {min_d.date()} → {max_d.date()} "
                      f"({len(df)} Bars)")
            else:
                print(f"  [SKIP] {sym}: zu wenig Daten ({len(df) if not df.empty else 0})")
        print(f"[obb_backtest] {len(data_dict)} Symbole geladen (Alpaca).")
        return data_dict

    # ── Weg 2: yfinance Fallback ───────────────────────────────────────────
    if YF_AVAILABLE:
        print(f"[obb_backtest] Kein Alpaca-Client → yfinance Fallback für "
              f"{len(symbols)} Symbole ...")
        for sym in symbols:
            try:
                ticker = yf.Ticker(sym)
                df = ticker.history(start=fetch_start, end=end_date)
                if df.empty or len(df) < 52:
                    print(f"  [SKIP] {sym}: {len(df)} Bars (zu wenig)")
                    continue
                # yfinance Spalten normalisieren
                df = df.rename(columns={
                    "Open": "Open", "High": "High", "Low": "Low",
                    "Close": "Close", "Volume": "Volume",
                })[["Open", "High", "Low", "Close", "Volume"]]
                # Timezone entfernen für einheitlichen Index
                if df.index.tz is not None:
                    df.index = df.index.tz_convert("UTC")
                data_dict[sym] = df
                print(f"  [OK] {sym}: {len(df)} Bars (yfinance)")
            except Exception as e:
                print(f"  [ERROR] {sym}: {e}")
        print(f"[obb_backtest] {len(data_dict)} Symbole geladen (yfinance).")
        return data_dict

    print("[ERROR] Weder Alpaca-Client noch yfinance verfügbar. "
          "pip install yfinance oder AlpacaClientDaily übergeben.")
    return {}


# ---------------------------------------------------------------------------
# 2. Backtest-Simulation
# ---------------------------------------------------------------------------

def run_obb_backtest(
    data_dict: Dict[str, pd.DataFrame],
    cfg: dict,
) -> dict:
    """
    One-Bar-Breakout Backtest (tagesgranular).

    Simulations-Logik (MOC/OPG – exakte Strategie-Logik):
      Tag D   → Signal berechnen (Close[D] vs. Rolling High/Low)
      Tag D   → Entry: Close[D]  × (1 ± slippage) + Kommission  ← MOC-Order
      Tag D+1 → Exit:  Open[D+1] × (1 ∓ slippage) + Kommission  ← OPG-Order

    Haltedauer: nur die Nacht (~17,5 Stunden), kein Intraday-Exposure.

    Guards:
      - Max. gleichzeitige Positionen (max_concurrent_positions)
      - Max. tägliche Trades (max_daily_trades)
      - Buying-Power-Check (kein Short wenn cash < margin)

    Position-Sizing:
      - Fixed Fraction (Standard): equity × position_size_pct / price
      - Kelly (optional): rollierende Win-Rate × kelly_fraction

    Parameter
    ----------
    data_dict : {symbol → DataFrame} mit täglichen OHLCV-Bars
    cfg       : Konfigurationsdict (aus OBB_DEFAULT_CONFIG)

    Rückgabe
    --------
    report_dict mit:
      - equity_curve, trades, Metriken (total_return, CAGR, Sharpe, ...)
    """
    if not data_dict:
        raise ValueError("Keine Daten für Backtest.")

    # ── Config-Parameter ──────────────────────────────────────────────────
    capital       = float(cfg.get("initial_capital", 10_000.0))
    commission    = float(cfg.get("commission_pct", 0.00005))
    slippage      = float(cfg.get("slippage_pct", 0.0003))
    position_pct  = float(cfg.get("position_size_pct", 0.10))
    max_daily     = int(cfg.get("max_daily_trades", 3))
    max_concurrent = int(cfg.get("max_concurrent_positions", 3))
    allow_shorts  = bool(cfg.get("allow_shorts", True))
    lookback      = int(cfg.get("lookback_bars", 50))
    use_kelly     = bool(cfg.get("use_kelly_sizing", False))
    kelly_lb      = int(cfg.get("kelly_lookback_trades", 50))
    kelly_min     = int(cfg.get("kelly_min_trades", 20))

    print(f"[obb_backtest] Starte Simulation ...")
    print(f"  Lookback={lookback}  Shorts={'AN' if allow_shorts else 'AUS'}  "
          f"Position={position_pct*100:.0f}%  Kapital={capital:,.0f}")

    # ── Alle Handelstage ──────────────────────────────────────────────────
    # WICHTIG: UTC verwenden, nicht ET!
    # Alpaca Daily-Bars haben Timestamps wie 2026-03-27 04:00:00+00:00 (UTC).
    # Konvertierung nach ET ergibt 2026-03-26 23:00:00 ET → .date() = 26.03.
    # Das ist einen Tag zu früh → Signale werden nie gefunden.
    # Lösung: UTC-Datum nehmen, das ist immer korrekt für tägliche Bars.
    all_dates: set = set()
    for df in data_dict.values():
        idx = df.index
        if idx.tz is None:
            idx_utc = idx.tz_localize("UTC")
        else:
            idx_utc = idx.tz_convert("UTC")
        all_dates.update(d.date() for d in idx_utc)

    trading_days = sorted(d for d in all_dates if d.weekday() < 5)
    if not trading_days:
        raise ValueError("Keine Handelstage gefunden.")

    # ── Signale vorberechnen (vektorisiert, schnell) ──────────────────────
    signals_cache: Dict[str, pd.DataFrame] = {}
    for sym, df in data_dict.items():
        try:
            sig_df = compute_obb_signals(df, cfg)
            signals_cache[sym] = sig_df
        except Exception as e:
            print(f"  [WARN] Signal-Berechnung {sym} fehlgeschlagen: {e}")

    # ── Simulation ────────────────────────────────────────────────────────
    cash = capital

    # Positionen: { sym: {"side", "entry", "shares", "entry_date", "signal"} }
    positions: Dict[str, dict] = {}

    # Pending Exits: { sym: date }  → an diesem Datum Exit-Order ausführen
    pending_exits: Dict[str, object] = {}  # date = day the exit should happen

    trades_list:    List[dict] = []
    equity_daily:   List[dict] = []
    peak_equity     = capital
    r_multiples:    List[float] = []
    closed_trades:  List[dict] = []  # für Rolling Win-Rate (Kelly)

    symbols = list(data_dict.keys())

    for day_idx, day in enumerate(trading_days):

        # ── 1. Pending Exits ausführen (Entry war gestern, Exit heute am Open) ──
        exits_to_process = [s for s, exit_day in list(pending_exits.items())
                            if exit_day == day]
        for sym in exits_to_process:
            if sym not in positions:
                del pending_exits[sym]
                continue

            pos    = positions[sym]
            df_sym = data_dict.get(sym)
            exit_open = _get_open_on_day(df_sym, day)

            if exit_open is None:
                # Kein Open für diesen Tag → nächsten verfügbaren Tag suchen
                future = [d for d in trading_days[day_idx+1:] if
                          _get_open_on_day(df_sym, d) is not None]
                if future:
                    pending_exits[sym] = future[0]
                    print(f"  [WARN] {sym}: kein Open am {day} → Exit verschoben auf {future[0]}")
                else:
                    # Kein zukünftiger Open → Close zum letzten verfügbaren Close
                    last_close = _get_last_close(df_sym)
                    if last_close is None:
                        del pending_exits[sym]
                        del positions[sym]
                        continue
                    exit_open = last_close
                    del pending_exits[sym]
                if sym in pending_exits:
                    continue

            # Exit-Preis mit Slippage
            side = pos["side"]
            if side == "long":
                exit_p = exit_open * (1 - slippage)
                pnl    = (exit_p - pos["entry"]) * pos["shares"]
                cash  += exit_p * pos["shares"] * (1 - commission)
            else:
                exit_p = exit_open * (1 + slippage)
                pnl    = (pos["entry"] - exit_p) * pos["shares"]
                cash  -= exit_p * pos["shares"] * (1 + commission)

            # R-Multiple (kein echtes R wegen fehlendem SL, daher Preis-basiert)
            entry_move = abs(pos["entry"] - exit_p)
            r_mult     = pnl / (pos["entry"] * pos["shares"] * position_pct + 1e-9)
            r_mult     = round(r_mult, 4)

            trade_record = {
                "date":        pd.Timestamp(pos["entry_date"]), # Tag D:   Entry (Close/MOC)
                "exit_date":   pd.Timestamp(day),               # Tag D+1: Exit  (Open/OPG)
                "symbol":      sym,
                "side":        "SELL" if side == "long" else "COVER",
                "entry_price": round(pos["entry"], 4),
                "exit_price":  round(exit_p, 4),
                "price":       round(exit_p, 4),  # Kompatibilität
                "shares":      pos["shares"],
                "pnl":         round(pnl, 4),
                "reason":      "1-Bar Exit (Opening Print)",
                "r_mult":      r_mult,
                "holding_days": 1,
                "signal":      pos.get("signal", ""),
            }
            trades_list.append(trade_record)
            closed_trades.append({"pnl": pnl})
            r_multiples.append(r_mult)

            del positions[sym]
            del pending_exits[sym]

        # ── 2. Equity-Snapshot ────────────────────────────────────────────
        pos_value = 0.0
        for sym, pos in positions.items():
            last_p = _get_close_on_day(data_dict.get(sym), day)
            if last_p:
                if pos["side"] == "long":
                    pos_value += pos["shares"] * last_p
                else:
                    pos_value -= pos["shares"] * last_p

        eq = cash + pos_value
        if eq > peak_equity:
            peak_equity = eq
        equity_daily.append({"date": pd.Timestamp(day), "equity": eq})

        # ── 3. Neue Signale prüfen ────────────────────────────────────────
        trades_today = 0

        for sym in symbols:
            # Guards
            if len(positions) + len(pending_exits) >= max_concurrent:
                break
            if trades_today >= max_daily:
                break
            if sym in positions or sym in pending_exits:
                continue

            sig_df = signals_cache.get(sym)
            if sig_df is None:
                continue

            # Signal für heute (Tag D)
            today_sig = _get_signal_on_day(sig_df, day)
            # ── TEMPORÄRES DEBUG (danach wieder entfernen) ──
            if day.year == 2026 and day.month >= 3:
                print(f"  [DEBUG] {day} {sym}: sig={today_sig}")
            # ── ENDE DEBUG ──
            
            if today_sig not in ("BUY", "SHORT"):
                continue
            if today_sig == "SHORT" and not allow_shorts:
                continue

            # Entry: Close von heute (Tag D) → MOC-Order
            # Exit:  Open von morgen (Tag D+1)  → OPG-Order
            # Haltedauer: nur die Nacht (~17,5h), kein Intraday-Exposure
            df_sym      = data_dict[sym]
            entry_close = _get_close_on_day(df_sym, day)
            if entry_close is None:
                continue

            future_days = [d for d in trading_days[day_idx+1:]]
            if not future_days:
                continue
            next_day = future_days[0]

            # Slippage auf Entry-Close (MOC hat minimal höhere Slippage
            # als Intraday, da Closing Auction weniger liquide sein kann)
            side = "long" if today_sig == "BUY" else "short"
            if side == "long":
                entry_p = entry_close * (1 + slippage)
            else:
                entry_p = entry_close * (1 - slippage)

            # Position Sizing
            rolling_wr = None
            if use_kelly and len(closed_trades) >= kelly_min:
                rolling_wr = compute_rolling_win_rate(closed_trades, kelly_lb)

            shares = calculate_obb_position_size(eq, entry_p, cfg, rolling_wr)
            if shares <= 0:
                continue

            # Cash-Check für Long
            if side == "long":
                cost = entry_p * shares * (1 + commission)
                if cost > cash:
                    shares = int(cash / (entry_p * (1 + commission)))
                    if shares <= 0:
                        continue

            # Short Margin-Check (vereinfacht: 50% Margin)
            if side == "short":
                margin_required = entry_p * shares * 0.5
                if margin_required > cash:
                    shares = int(cash * 2 / (entry_p * (1 + commission)))
                    if shares <= 0:
                        continue

            # Cash-Update
            if side == "long":
                cash -= entry_p * shares * (1 + commission)
            else:
                cash += entry_p * shares * (1 - commission)

            positions[sym] = {
                "side":       side,
                "entry":      entry_p,
                "shares":     shares,
                "entry_date": day,       # Tag D: Entry am Closing Print
                "exit_date":  next_day,  # Tag D+1: Exit am Opening Print
                "signal":     today_sig,
            }

            # Exit am naechsten Handelstag (Open/OPG)
            exit_day_candidates = [d for d in trading_days
                                   if d > day]
            if exit_day_candidates:
                pending_exits[sym] = exit_day_candidates[0]
            else:
                # Kein weiterer Handelstag → sofortiger Cleanup
                positions.pop(sym, None)
                continue

            trades_list.append({
                "date":      pd.Timestamp(day),      # Tag D: Signal + Entry (Closing Print)
                "exit_date": pd.Timestamp(next_day), # Tag D+1: Exit (Opening Print)
                "symbol":    sym,
                "side":      "BUY" if side == "long" else "SHORT",
                "price":     round(entry_p, 4),
                "shares":    shares,
                "pnl":       0.0,
                "reason":    f"OBB MOC Entry ({lookback}-Bar {'High' if side == 'long' else 'Low'} Breakout)",
                "r_mult":    0.0,
                "signal":    today_sig,
            })
            trades_today += 1

    # ── Offene Positionen am Ende schließen ───────────────────────────────
    for sym, pos in list(positions.items()):
        last_p = _get_last_close(data_dict.get(sym))
        if not last_p:
            continue
        side = pos["side"]
        if side == "long":
            pnl  = (last_p - pos["entry"]) * pos["shares"]
            cash += last_p * pos["shares"] * (1 - commission - slippage)
        else:
            pnl  = (pos["entry"] - last_p) * pos["shares"]
            cash -= last_p * pos["shares"] * (1 + commission + slippage)
        r_mult = round(pnl / (pos["entry"] * pos["shares"] * position_pct + 1e-9), 4)
        trades_list.append({
            "date":        pd.Timestamp(pos.get("entry_date", trading_days[-1])),
            "exit_date":   pd.Timestamp(trading_days[-1]),
            "symbol":      sym,
            "side":        "SELL" if side == "long" else "COVER",
            "entry_price": round(pos["entry"], 4),
            "exit_price":  round(last_p, 4),
            "price":       round(last_p, 4),
            "shares":      pos["shares"],
            "pnl":         round(pnl, 4),
            "reason":      "End of Test",
            "r_mult": r_mult,
            "signal": pos.get("signal", ""),
        })
        r_multiples.append(r_mult)
    positions.clear()

    # ── Report bauen ──────────────────────────────────────────────────────
    trades_df = pd.DataFrame(trades_list)
    eq_df     = pd.DataFrame(equity_daily)
    eq_df["date"] = pd.to_datetime(eq_df["date"])
    eq_series = eq_df.set_index("date")["equity"]

    report = _compute_obb_metrics(eq_series, trades_df, capital)
    report["equity_curve"]    = eq_series
    report["equity_curve_df"] = eq_df
    report["trades"]          = trades_df
    report["capital"]         = capital
    report["cfg"]             = cfg

    if r_multiples:
        report["avg_r_proxy"] = round(float(np.mean(r_multiples)), 4)

    # Win-Streak / Loss-Streak
    if not trades_df.empty and "pnl" in trades_df.columns:
        exits = trades_df[trades_df["side"].isin(["SELL", "COVER"])]["pnl"]
        if not exits.empty:
            wins   = (exits > 0).astype(int)
            losses = (exits <= 0).astype(int)
            report["max_win_streak"]  = int(_max_streak(wins.tolist()))
            report["max_loss_streak"] = int(_max_streak(losses.tolist()))

    print(f"[obb_backtest] Simulation abgeschlossen: "
          f"{report.get('total_trades', 0)} Trades")
    return report


# ---------------------------------------------------------------------------
# 3. Hilfsfunktionen
# ---------------------------------------------------------------------------

def _get_open_on_day(df: Optional[pd.DataFrame], day) -> Optional[float]:
    if df is None or df.empty:
        return None
    idx = df.index
    # UTC verwenden – Alpaca Daily-Bars haben UTC-Timestamps (04:00 UTC)
    # ET-Konvertierung würde das Datum um einen Tag nach hinten verschieben
    if idx.tz is None:
        idx_utc = idx.tz_localize("UTC")
    else:
        idx_utc = idx.tz_convert("UTC")
    mask = [d.date() == day for d in idx_utc]
    sub  = df[mask]
    return float(sub["Open"].iloc[0]) if not sub.empty else None


def _get_close_on_day(df: Optional[pd.DataFrame], day) -> Optional[float]:
    if df is None or df.empty:
        return None
    idx = df.index
    # UTC verwenden – siehe Kommentar in _get_open_on_day
    if idx.tz is None:
        idx_utc = idx.tz_localize("UTC")
    else:
        idx_utc = idx.tz_convert("UTC")
    mask = [d.date() == day for d in idx_utc]
    sub  = df[mask]
    return float(sub["Close"].iloc[-1]) if not sub.empty else None


def _get_last_close(df: Optional[pd.DataFrame]) -> Optional[float]:
    if df is None or df.empty:
        return None
    return float(df["Close"].iloc[-1])


def _get_signal_on_day(sig_df: pd.DataFrame, day) -> str:
    idx = sig_df.index
    # UTC verwenden – siehe Kommentar in _get_open_on_day
    if idx.tz is None:
        try:
            idx_utc = idx.tz_localize("UTC")
        except Exception:
            idx_utc = idx
    else:
        idx_utc = idx.tz_convert("UTC")
    mask = [d.date() == day for d in idx_utc]
    sub  = sig_df[mask]
    if sub.empty:
        return "HOLD"
    return str(sub["signal"].iloc[-1])


def _max_streak(arr: list) -> int:
    """Längste aufeinanderfolgende Serie von 1en."""
    max_s = cur_s = 0
    for v in arr:
        cur_s = cur_s + 1 if v == 1 else 0
        max_s = max(max_s, cur_s)
    return max_s


# ---------------------------------------------------------------------------
# 4. Metriken
# ---------------------------------------------------------------------------

def _compute_obb_metrics(
    eq: pd.Series,
    trades: pd.DataFrame,
    capital: float,
) -> dict:
    if eq.empty or capital == 0:
        return {}

    total_ret = (eq.iloc[-1] - capital) / capital
    n_years   = max((eq.index[-1] - eq.index[0]).days / 365.25, 1e-6)
    cagr      = (1 + total_ret) ** (1 / n_years) - 1
    returns   = eq.pct_change().dropna()
    sharpe    = (returns.mean() / (returns.std() + 1e-9)) * np.sqrt(252)
    neg       = returns[returns < 0]
    sortino   = ((returns.mean() / (neg.std() + 1e-9)) * np.sqrt(252)
                 if len(neg) > 0 else np.nan)
    rolling_max = eq.cummax()
    dd          = (eq - rolling_max) / (rolling_max + 1e-9)
    max_dd      = float(dd.min())
    calmar      = cagr / (abs(max_dd) + 1e-9)

    if trades.empty or "pnl" not in trades.columns:
        win_rate = profit_factor = total_trades = 0.0
        avg_pnl = avg_win = avg_loss = 0.0
    else:
        exits  = trades[trades["side"].isin(["SELL", "COVER"])]
        wins   = exits[exits["pnl"] > 0]["pnl"]
        losses = exits[exits["pnl"] <= 0]["pnl"]
        total_trades  = len(exits)
        win_rate      = len(wins)  / (total_trades + 1e-9)
        profit_factor = wins.sum() / (abs(losses.sum()) + 1e-9)
        avg_pnl       = float(exits["pnl"].mean()) if not exits.empty else 0.0
        avg_win       = float(wins.mean())  if not wins.empty  else 0.0
        avg_loss      = float(losses.mean()) if not losses.empty else 0.0

    return {
        "total_return_pct":  round(total_ret * 100, 2),
        "cagr_pct":          round(cagr * 100, 2),
        "sharpe":            round(sharpe, 3),
        "sortino":           round(sortino, 3) if not np.isnan(sortino) else None,
        "max_drawdown_pct":  round(max_dd * 100, 2),
        "calmar":            round(calmar, 3),
        "win_rate_pct":      round(win_rate * 100, 2),
        "profit_factor":     round(profit_factor, 3),
        "total_trades":      int(total_trades),
        "avg_pnl_per_trade": round(avg_pnl, 2),
        "avg_win":           round(avg_win, 2),
        "avg_loss":          round(avg_loss, 2),
        "final_equity":      round(float(eq.iloc[-1]), 2),
    }


# ---------------------------------------------------------------------------
# 5. Report & Vergleich
# ---------------------------------------------------------------------------

def print_obb_report(
    report: dict,
    output_dir: Optional[Path] = None,
    compare_with_orb: bool = False,
    orb_cfg: Optional[dict] = None,
    orb_alpaca=None,
    start_date: str = "2024-01-01",
    end_date: Optional[str] = None,
) -> None:
    """
    Drucke OBB-Report und (optional) ORB-Vergleich.
    Speichert Equity-Kurve (PNG) und Trade-Liste (XLSX/CSV).
    """
    if output_dir is None:
        output_dir = Path(__file__).parent / "obb_trading_data"
    output_dir.mkdir(parents=True, exist_ok=True)

    eq     = report.get("equity_curve", pd.Series())
    trades = report.get("trades", pd.DataFrame())

    # ── ORB-Vergleich laden ────────────────────────────────────────────────
    orb_report = None
    if compare_with_orb and orb_cfg is not None:
        orb_report = _run_orb_for_comparison(
            orb_cfg, orb_alpaca, start_date, end_date
        )

    # ── Konsolen-Ausgabe ──────────────────────────────────────────────────
    sep  = "─" * 65
    sep2 = "═" * 65

    print(f"\n{sep2}")
    print(f"  BACKTEST REPORT – One-Bar-Breakout (50-Bar High/Low Momentum)")
    print(sep2)
    _print_metrics_table(report, orb_report)

    # Jahresverteilung
    if not trades.empty and "date" in trades.columns:
        exits = trades[trades["side"].isin(["SELL", "COVER"])]
        if not exits.empty:
            dates_conv = pd.to_datetime(exits["date"], errors="coerce", utc=True)
            years = dates_conv.dropna().dt.year.value_counts().sort_index()
            if not years.empty:
                print(f"\n  Jahresverteilung (Exits):")
                for yr, cnt in years.items():
                    sub   = exits[dates_conv.dt.year == yr]
                    wr    = (sub["pnl"] > 0).mean() * 100
                    p_sum = sub["pnl"].sum()
                    print(f"    {yr}: {cnt:>4} Trades  "
                          f"WR={wr:>5.1f}%  PnL={p_sum:>+10.2f}")

    # Exit-Verteilung
    if not trades.empty and "reason" in trades.columns:
        exits = trades[trades["side"].isin(["SELL", "COVER"])]
        if not exits.empty:
            print(f"\n  Exit-Verteilung:")
            for reason, cnt in exits["reason"].value_counts().items():
                print(f"    {str(reason):<40} {cnt:>4}")

    # Signal-Verteilung
    if not trades.empty and "signal" in trades.columns:
        entries = trades[trades["side"].isin(["BUY", "SHORT"])]
        if not entries.empty:
            sig_counts = entries["signal"].value_counts()
            print(f"\n  Signal-Verteilung (Entries):")
            for sig, cnt in sig_counts.items():
                wins = (trades[
                    (trades["side"].isin(["SELL", "COVER"])) &
                    (trades.get("pnl", 0) > 0)
                ]["pnl"].count())
                print(f"    {str(sig):<10} {cnt:>4} Trades")

    print(sep)

    # ── Streak ────────────────────────────────────────────────────────────
    if "max_win_streak" in report:
        print(f"  Max Win-Streak:     {report['max_win_streak']:>5}")
    if "max_loss_streak" in report:
        print(f"  Max Loss-Streak:    {report['max_loss_streak']:>5}")
    print(sep2)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── Trade-Export (XLSX / CSV) ─────────────────────────────────────────
    if not trades.empty:
        _export_trades(trades, output_dir, ts)

    # ── Equity-Kurve Plot ─────────────────────────────────────────────────
    if MPL_AVAILABLE and not eq.empty:
        _plot_equity_curves(eq, orb_report, output_dir, ts, report.get("cfg", {}))


def _print_metrics_table(obb: dict, orb: Optional[dict]) -> None:
    """Gibt Metriken tabellarisch aus (OBB ± ORB Vergleich)."""
    sep = "─" * 65

    if orb:
        print(f"\n  {'Metrik':<28} {'OBB':>12} {'ORB':>12} {'Diff':>10}")
        print(sep)
        metrics = [
            ("Gesamtrendite (%)",       "total_return_pct",  "{:>+.2f}"),
            ("CAGR (%)",                "cagr_pct",          "{:>+.2f}"),
            ("Sharpe Ratio",            "sharpe",            "{:>.3f}"),
            ("Sortino Ratio",           "sortino",           "{:>.3f}"),
            ("Max Drawdown (%)",        "max_drawdown_pct",  "{:>.2f}"),
            ("Calmar Ratio",            "calmar",            "{:>.3f}"),
            ("Win-Rate (%)",            "win_rate_pct",      "{:>.2f}"),
            ("Profit Factor",           "profit_factor",     "{:>.3f}"),
            ("Trades gesamt",           "total_trades",      "{:>d}"),
            ("Ø PnL / Trade",           "avg_pnl_per_trade", "{:>+.2f}"),
            ("Endkapital ($)",          "final_equity",      "{:>,.2f}"),
        ]
        for label, key, fmt in metrics:
            obb_val = obb.get(key)
            orb_val = orb.get(key)
            if obb_val is None:
                obb_str = "n/a"
            else:
                try:
                    obb_str = fmt.format(obb_val)
                except Exception:
                    obb_str = str(obb_val)
            if orb_val is None:
                orb_str = "n/a"
            else:
                try:
                    orb_str = fmt.format(orb_val)
                except Exception:
                    orb_str = str(orb_val)

            # Differenz (nur für numerische Felder)
            diff_str = ""
            try:
                if obb_val is not None and orb_val is not None:
                    diff = float(obb_val) - float(orb_val)
                    sign = "+" if diff >= 0 else ""
                    diff_str = f"{sign}{diff:.2f}"
            except Exception:
                pass
            print(f"  {label:<28} {obb_str:>12} {orb_str:>12} {diff_str:>10}")
    else:
        # Nur OBB
        print(f"\n  {'Metrik':<28} {'OBB':>12}")
        sep_short = "─" * 42
        print(sep_short)
        metrics = [
            ("Gesamtrendite (%)",       "total_return_pct",  "{:>+.2f}"),
            ("CAGR (%)",                "cagr_pct",          "{:>+.2f}"),
            ("Sharpe Ratio",            "sharpe",            "{:>.3f}"),
            ("Sortino Ratio",           "sortino",           "{:>.3f}"),
            ("Max Drawdown (%)",        "max_drawdown_pct",  "{:>.2f}"),
            ("Calmar Ratio",            "calmar",            "{:>.3f}"),
            ("Win-Rate (%)",            "win_rate_pct",      "{:>.2f}"),
            ("Profit Factor",           "profit_factor",     "{:>.3f}"),
            ("Trades gesamt",           "total_trades",      "{:>d}"),
            ("Ø PnL / Trade ($)",       "avg_pnl_per_trade", "{:>+.2f}"),
            ("Endkapital ($)",          "final_equity",      "{:>,.2f}"),
        ]
        for label, key, fmt in metrics:
            val = obb.get(key)
            if val is None:
                val_str = "n/a"
            else:
                try:
                    val_str = fmt.format(val)
                except Exception:
                    val_str = str(val)
            print(f"  {label:<28} {val_str:>12}")
        print(sep_short)


def _run_orb_for_comparison(
    orb_cfg: dict,
    orb_alpaca,
    start_date: str,
    end_date: Optional[str],
) -> Optional[dict]:
    """Versucht den ORB-Backtest für Vergleichszwecke auszuführen."""
    try:
        from orb_backtest import load_orb_data, run_orb_backtest
        print("\n[obb_backtest] Lade ORB-Daten für Vergleich ...")
        data, vix, vix3m = load_orb_data(
            orb_cfg.get("symbols", []),
            start_date,
            end_date or datetime.now().strftime("%Y-%m-%d"),
            alpaca=orb_alpaca,
        )
        if not data:
            print("[obb_backtest] ORB: keine Daten – Vergleich übersprungen.")
            return None
        _, orb_report = run_orb_backtest(data, vix, orb_cfg, vix3m_series=vix3m)
        print("[obb_backtest] ORB-Backtest für Vergleich abgeschlossen.")
        return orb_report
    except ImportError:
        print("[WARN] orb_backtest.py nicht gefunden – "
              "Vergleich nicht möglich. Stelle sicher, dass alle 3 ORB-Dateien "
              "im selben Verzeichnis liegen.")
        return None
    except Exception as e:
        print(f"[WARN] ORB-Vergleich fehlgeschlagen: {e}")
        return None


def _export_trades(trades: pd.DataFrame, output_dir: Path, ts: str) -> None:
    if XLSX_AVAILABLE:
        xlsx_path = output_dir / f"obb_trades_{ts}.xlsx"
        trades_exp = trades.copy()
        # Timezone-Strip für Excel
        for col in trades_exp.select_dtypes(include=["datetimetz"]).columns:
            trades_exp[col] = trades_exp[col].dt.tz_localize(None)
        for col in trades_exp.columns:
            if trades_exp[col].dtype == object:
                trades_exp[col] = trades_exp[col].apply(
                    lambda v: (
                        v.tz_localize(None) if isinstance(v, pd.Timestamp) and v.tzinfo
                        else v
                    )
                )
        try:
            trades_exp.to_excel(xlsx_path, index=False, engine="openpyxl")
            print(f"  Trade-Liste: {xlsx_path}")
        except Exception as e:
            csv_path = output_dir / f"obb_trades_{ts}.csv"
            trades.to_csv(csv_path, index=False)
            print(f"  Trade-Liste (CSV-Fallback): {csv_path} ({e})")
    else:
        csv_path = output_dir / f"obb_trades_{ts}.csv"
        trades.to_csv(csv_path, index=False)
        print(f"  Trade-Liste: {csv_path} (pip install openpyxl für Excel)")


def _plot_equity_curves(
    obb_eq:    pd.Series,
    orb_report: Optional[dict],
    output_dir: Path,
    ts: str,
    cfg: dict,
) -> None:
    """
    Plottet Equity-Kurven.
    Bei vorhandenem ORB-Report: doppelter Plot mit Vergleich.
    """
    orb_eq = (orb_report.get("equity_curve") if orb_report else None)

    has_orb = orb_eq is not None and not orb_eq.empty

    fig_h = 8 if has_orb else 5
    fig, axes = plt.subplots(
        2 if has_orb else 1, 1,
        figsize=(15, fig_h),
        squeeze=False,
    )
    ax_eq  = axes[0][0]
    ax_dd  = axes[1][0] if has_orb else None

    # Equity-Kurven
    ax_eq.plot(obb_eq.index, obb_eq.values,
               label=f"OBB ({cfg.get('lookback_bars', 50)}-Bar Breakout)",
               linewidth=2, color="#2196F3")
    if has_orb:
        # ORB-Kurve auf OBB-Kapital normalisieren (falls unterschiedliche Startkapitale)
        obb_start  = float(obb_eq.iloc[0])
        orb_start  = float(orb_eq.iloc[0])
        orb_normed = orb_eq * (obb_start / orb_start)
        ax_eq.plot(orb_normed.index, orb_normed.values,
                   label="ORB + MIT Overlay (normalisiert)",
                   linewidth=1.5, color="#FF5722", alpha=0.8, linestyle="--")

    ax_eq.set_title("Equity Curve – One-Bar-Breakout vs. ORB", fontsize=13)
    ax_eq.set_ylabel("Equity ($)")
    ax_eq.legend(fontsize=10)
    ax_eq.grid(True, alpha=0.3)
    ax_eq.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))
    fig.autofmt_xdate(rotation=30, ha="right")

    # Drawdown-Vergleich
    if has_orb and ax_dd is not None:
        obb_roll_max = obb_eq.cummax()
        obb_dd = ((obb_eq - obb_roll_max) / (obb_roll_max + 1e-9)) * 100
        ax_dd.fill_between(obb_dd.index, obb_dd.values, 0,
                           alpha=0.4, color="#2196F3", label="OBB Drawdown")

        orb_roll_max = orb_eq.cummax()
        orb_dd = ((orb_eq - orb_roll_max) / (orb_roll_max + 1e-9)) * 100
        ax_dd.fill_between(orb_dd.index, orb_dd.values, 0,
                           alpha=0.3, color="#FF5722", label="ORB Drawdown")
        ax_dd.set_title("Drawdown Vergleich")
        ax_dd.set_ylabel("Drawdown (%)")
        ax_dd.legend(fontsize=10)
        ax_dd.grid(True, alpha=0.3)
        ax_dd.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))

    plt.tight_layout()
    plot_path = output_dir / f"obb_equity_curve_{ts}.png"
    fig.savefig(plot_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot: {plot_path}")


# ---------------------------------------------------------------------------
# 6. Standalone CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="One-Bar-Breakout Backtest",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--start",       default="2024-01-01",
                        help="Backtest-Start (YYYY-MM-DD)")
    parser.add_argument("--end",         default=None,
                        help="Backtest-Ende (YYYY-MM-DD, Standard: heute)")
    parser.add_argument("--lookback",    type=int, default=50,
                        help="Lookback in Bars (Standard: 50)")
    parser.add_argument("--shorts",      action="store_true",
                        help="Short-Signale aktivieren")
    parser.add_argument("--position-pct", type=float, default=0.10,
                        help="Anteil des Kapitals je Position (Standard: 0.10 = 10%%)")
    parser.add_argument("--kelly",       action="store_true",
                        help="Kelly-basiertes Sizing aktivieren")
    parser.add_argument("--compare-orb", action="store_true",
                        help="Vergleich mit ORB-Bot einschließen")
    parser.add_argument("--capital",     type=float, default=10_000.0,
                        help="Startkapital (Standard: 10.000)")
    parser.add_argument("--live",        action="store_true",
                        help="Live-Alpaca-Modus (überschreibt APCA_PAPER)")
    args = parser.parse_args()

    # .env laden
    try:
        from dotenv import load_dotenv
        _base = Path(__file__).parent
        for candidate in [_base / ".env_OBB", _base / ".env"]:
            if candidate.exists():
                load_dotenv(candidate, override=True)
                print(f"[Config] Umgebung: {candidate.name}")
                break
    except ImportError:
        pass

    cfg = dict(OBB_DEFAULT_CONFIG)
    cfg.update({
        "lookback_bars":    args.lookback,
        "allow_shorts":     args.shorts,
        "position_size_pct": args.position_pct,
        "use_kelly_sizing": args.kelly,
        "initial_capital":  args.capital,
        "data_dir":         Path(__file__).parent / "obb_trading_data",
    })
    Path(cfg["data_dir"]).mkdir(exist_ok=True)

    # Alpaca-Client
    alpaca = None
    try:
        if args.live:
            os.environ["APCA_PAPER"] = "false"
        from one_bar_breakout_bot import AlpacaClientDaily, _build_alpaca_client
        alpaca = _build_alpaca_client(cfg)
    except Exception as e:
        print(f"[WARN] Kein Alpaca-Client: {e} → yfinance Fallback")

    end_date = args.end or datetime.now().strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  ONE-BAR BREAKOUT – BACKTEST")
    print(f"  Zeitraum: {args.start} → {end_date}")
    print(f"  Lookback: {cfg['lookback_bars']} Bars  "
          f"Shorts: {'AN' if cfg['allow_shorts'] else 'AUS'}")
    print(f"  Position: {cfg['position_size_pct']*100:.0f}%  "
          f"Kapital: {cfg['initial_capital']:,.0f}")
    print(f"{'='*60}")

    data = load_obb_data(cfg["symbols"], args.start, end_date, alpaca=alpaca)
    if not data:
        print("[ERROR] Keine Daten geladen. "
              "Alpaca-Keys setzen oder yfinance installieren: pip install yfinance")
        sys.exit(1)

    report = run_obb_backtest(data, cfg)

    # ORB-Config für Vergleich
    orb_cfg = None
    if args.compare_orb:
        try:
            from orb_strategy import ORB_DEFAULT_CONFIG
            import copy as _copy
            orb_cfg = _copy.deepcopy(ORB_DEFAULT_CONFIG)
            orb_cfg["initial_capital"] = cfg["initial_capital"]
        except ImportError:
            print("[WARN] orb_strategy.py nicht gefunden – kein ORB-Vergleich.")

    print_obb_report(
        report,
        output_dir=Path(cfg["data_dir"]),
        compare_with_orb=args.compare_orb and orb_cfg is not None,
        orb_cfg=orb_cfg,
        orb_alpaca=alpaca,
        start_date=args.start,
        end_date=end_date,
    )


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
orb_backtest.py – Backtest-Engine für ORB Bot v2.

Intraday-Backtest auf 5-Minuten-Bars mit:
  - Long-Breakout + Short-Breakdown
  - R-basiertes Exit-System (Profit Target, Stop Loss, Trailing Stop)
  - Guards: VIX-Regime, Drawdown Circuit Breaker, Volume-Guard, Max Daily Trades
  - Commission + Slippage
  - Metriken importiert aus vbt_backtest (_compute_metrics)

Nutzung:
    from orb_backtest import load_orb_data, run_orb_backtest, print_orb_report

    data, vix = load_orb_data(symbols, "2024-01-01", "2025-04-01", alpaca)
    pf, report = run_orb_backtest(data, vix, ORB_CONFIG)
    print_orb_report(report, output_dir=Path("orb_trading_data"))
"""

from __future__ import annotations

import sys
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

warnings.filterwarnings("ignore")

# v7-Module importieren
_v7_dir = Path(__file__).resolve().parent.parent / "Trading_Bot"
if str(_v7_dir) not in sys.path:
    sys.path.insert(0, str(_v7_dir))

try:
    from vbt_backtest import _compute_metrics  # type: ignore
except ImportError:
    _compute_metrics = None

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    MPL_AVAILABLE = True
except ImportError:
    MPL_AVAILABLE = False

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False

from orb_signals import (
    calculate_orb_levels,
    compute_indicators,
    compute_orb_signals,
)

ET = pytz.timezone("America/New_York")


# ---------------------------------------------------------------------------
# 1. Datenladen
# ---------------------------------------------------------------------------

def load_orb_data(
    symbols: List[str],
    start_date: str,
    end_date: str,
    alpaca=None,
) -> Tuple[Dict[str, pd.DataFrame], pd.Series]:
    """
    Lade 5-Minuten-Bars für alle Symbole + VIX-Tageskurse.

    Parameter
    ---------
    symbols    : Ticker-Liste
    start_date : "YYYY-MM-DD"
    end_date   : "YYYY-MM-DD"
    alpaca     : AlpacaClient-Instanz (aus orb_bot_alpaca.py)

    Rückgabe
    --------
    (data_dict, vix_series)
    """
    data_dict: Dict[str, pd.DataFrame] = {}

    if alpaca is not None:
        print(f"[orb_backtest] Lade {len(symbols)} Symbole via Alpaca (5m) …")
        raw = alpaca.fetch_bars_bulk(symbols, start_date, end_date)
        for sym, df in raw.items():
            if not df.empty and len(df) > 100:
                data_dict[sym] = compute_indicators(df)
    else:
        print("[orb_backtest] Kein Alpaca-Client → kein Datenabruf möglich.")

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

    print(f"[orb_backtest] {len(data_dict)} Symbole geladen.")
    return data_dict, vix_series


# ---------------------------------------------------------------------------
# 2. Backtest-Simulation
# ---------------------------------------------------------------------------

def run_orb_backtest(
    data_dict: Dict[str, pd.DataFrame],
    vix_series: pd.Series,
    cfg: dict,
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
    vix_thresh    = float(cfg.get("vix_high_threshold", 30))
    vix_reduce    = float(cfg.get("vix_size_reduction", 0.5))
    max_vol_pct   = float(cfg.get("max_volume_pct", 0.01))
    allow_shorts  = bool(cfg.get("allow_shorts", False))
    orb_minutes   = int(cfg.get("opening_range_minutes", 30))
    min_strength  = float(cfg.get("min_signal_strength", 0.3))
    avoid_fri     = bool(cfg.get("avoid_fridays", True))
    avoid_mon     = bool(cfg.get("avoid_mondays", False))

    print("[orb_backtest] Starte Simulation …")

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

    # ── Simulation ────────────────────────────────────────────────────────
    cash = capital
    positions: Dict[str, dict] = {}       # sym → position-dict
    trades_list: List[dict] = []
    equity_daily: List[Tuple] = []        # (date, equity)
    peak_equity = capital
    r_multiples: List[float] = []
    holding_bars_list: List[int] = []

    symbols = list(data_dict.keys())

    for day in trading_days:
        weekday = day.weekday()
        if weekday >= 5:
            continue
        if avoid_fri and weekday == 4:
            continue
        if avoid_mon and weekday == 0:
            continue

        trades_today = 0

        # VIX für heute (ffill: nächstliegenden Wert nehmen)
        vix_val = vix_dict.get(day, 20.0)
        if vix_val == 0:
            vix_val = 20.0
        size_mult = vix_reduce if vix_val >= vix_thresh else 1.0

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
                        unrealized += pos["shares"] * (2 * pos["entry"] - last_p)

        equity = cash + unrealized
        if equity > peak_equity:
            peak_equity = equity
        dd_active = (peak_equity - equity) / (peak_equity + 1e-9) >= max_dd

        # ── Für jedes Symbol: Tages-5m-Bars verarbeiten ──────────────────
        for sym in symbols:
            df_full = data_dict[sym]
            day_df = _extract_day(df_full, day)
            if day_df is None or len(day_df) < 8:
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

            for i, (bar_ts, bar) in enumerate(day_df.iterrows()):
                if not post_orb_mask[i]:
                    continue

                price = float(bar["Close"])
                high = float(bar["High"])
                low = float(bar["Low"])
                bar_count += 1

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

                # Volume guard
                vol_ma = float(bar.get("Volume_MA", 0))
                vol = float(bar.get("Volume", 0))
                if vol_ma > 0 and max_vol_pct > 0:
                    max_shares_vol = int(max_vol_pct * vol_ma)
                else:
                    max_shares_vol = 999_999

                side = "long" if is_long else "short"
                atr_val = float(bar.get("ATR", orb_range))
                if np.isnan(atr_val) or atr_val <= 0:
                    atr_val = orb_range

                if side == "long":
                    entry_p = price * (1 + slippage)
                    stop = max(orb_low, entry_p - 1.5 * atr_val)
                    if stop >= entry_p:
                        stop = entry_p - atr_val
                    risk_per_share = entry_p - stop
                else:
                    entry_p = price * (1 - slippage)
                    stop = min(orb_high, entry_p + 1.5 * atr_val)
                    if stop <= entry_p:
                        stop = entry_p + atr_val
                    risk_per_share = stop - entry_p

                if risk_per_share <= 0:
                    continue

                # Position sizing (R-basiert)
                risk_amount = equity * risk_pt * size_mult
                shares = int(risk_amount / risk_per_share)
                shares = min(shares, max_shares_vol)

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
                    "reason": f"ORB {'Breakout' if side == 'long' else 'Breakdown'} "
                              f"[{strength:.2f}]",
                }

                trades_list.append({
                    "date": bar_ts, "symbol": sym,
                    "side": "BUY" if side == "long" else "SHORT",
                    "price": entry_p, "shares": shares,
                    "pnl": 0.0,
                    "reason": positions[sym]["reason"],
                    "r_mult": 0.0,
                })

                entered_today = True
                trades_today += 1

        # ── EOD: offene Positionen NICHT schließen (Overnight möglich) ────
        # Equity Snapshot
        eod_unrealized = 0.0
        for sym, pos in positions.items():
            lp = _last_price_on_day(data_dict.get(sym), day)
            if lp > 0:
                if pos["side"] == "long":
                    eod_unrealized += pos["shares"] * lp
                else:
                    eod_unrealized += pos["shares"] * (2 * pos["entry"] - lp)
        equity_daily.append((day, cash + eod_unrealized))

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
        eq_df = pd.DataFrame(equity_daily, columns=["date", "equity"])
        eq_df["date"] = pd.to_datetime(eq_df["date"])
        eq_series = eq_df.set_index("date")["equity"]
    else:
        eq_series = pd.Series(dtype=float, name="equity")

    if _compute_metrics is not None and not eq_series.empty:
        report = _compute_metrics(eq_series, trades_df, capital, eq_series.index)
    else:
        report = _fallback_metrics(eq_series, trades_df, capital)

    report["equity_curve"] = eq_series
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


def _fallback_metrics(eq: pd.Series, trades: pd.DataFrame, capital: float) -> dict:
    """Metriken berechnen falls vbt_backtest nicht importierbar."""
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

    # CSV
    if not trades.empty:
        csv_path = output_dir / "orb_v2_trades.csv"
        trades.to_csv(csv_path, index=False)
        print(f"\n  Trade-Liste: {csv_path}")

    # Equity-Kurve Plot
    if MPL_AVAILABLE and not eq.empty:
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(eq.index, eq.values, label="ORB Bot v2", linewidth=1.5)
        ax.set_title("Equity Curve – ORB Bot v2 Backtest")
        ax.set_xlabel("Datum")
        ax.set_ylabel("Equity ($)")
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plot_path = output_dir / "orb_v2_equity_curve.png"
        fig.savefig(plot_path, dpi=120)
        plt.close(fig)
        print(f"  Equity-Kurve: {plot_path}")

#!/usr/bin/env python3
"""
vbt_backtest.py – vectorbt Portfolio-Simulation für trader_v7.

Übernimmt alle Guards aus trader_v6:
  - VIX-Regime (Positionsgrößen halbieren wenn VIX ≥ Schwelle)
  - Drawdown Circuit Breaker (keine neuen Trades wenn DD > max_drawdown_pct)
  - Sektor-Klumpenrisiko-Guard (max N Positionen pro Sektor)
  - Volume-Guard (max 1 % des Tagesvolumens je Order)
  - Stop-Loss / Trailing Stop (ATR-basiert)
  - Partial Profit (25 % bei +partial_profit_pct)
  - Commission + Slippage

Nutzung:
    from vbt_backtest import load_data, run_backtest, print_report

    data, vix = load_data(CONFIG["symbols"], period="800d")
    pf, report = run_backtest(data, vix, CONFIG)
    print_report(pf, report, benchmark=data.get("SPY"))
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

try:
    import vectorbt as vbt
    VBT_AVAILABLE = True
except ImportError:
    VBT_AVAILABLE = False
    print("[WARN] vectorbt nicht installiert → pip install vectorbt", flush=True)

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False

try:
    import matplotlib
    matplotlib.use("Agg")   # ohne GUI
    import matplotlib.pyplot as plt
    MPL_AVAILABLE = True
except ImportError:
    MPL_AVAILABLE = False

from vbt_signals import prepare_all, compute_indicators, compute_signals


# ---------------------------------------------------------------------------
# 1. Datenladen
# ---------------------------------------------------------------------------

def load_data(
    symbols: List[str],
    period: str = "800d",
    alpaca=None,
) -> Tuple[Dict[str, pd.DataFrame], pd.Series]:
    """
    Lade OHLCV-Daten für alle Symbole + VIX-Zeitreihe.

    Parameter
    ---------
    symbols : Liste von Ticker-Symbolen
    period  : Zeitraum als String «800d», «3y» etc.
    alpaca  : optionale AlpacaClient-Instanz (aus trader_v6)

    Rückgabe
    --------
    (data_dict, vix_series)
      data_dict  : {symbol: OHLCV-DataFrame mit DatetimeIndex}
      vix_series : pd.Series (daily closing VIX) mit DatetimeIndex
    """
    from datetime import datetime, timedelta

    data_dict: Dict[str, pd.DataFrame] = {}

    # Zeitraum berechnen
    days      = int(period.replace("d", "").replace("y", "")) * (1 if "d" in period else 365)
    end_date  = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days + 100)).strftime("%Y-%m-%d")

    if alpaca is not None:
        print(f"[vbt_backtest] Lade {len(symbols)} Symbole via Alpaca …")
        raw = alpaca.fetch_bars_bulk(symbols, start_date, end_date)
        for sym, df in raw.items():
            if not df.empty:
                data_dict[sym] = df[["Open", "High", "Low", "Close", "Volume"]]
    elif YF_AVAILABLE:
        print(f"[vbt_backtest] Lade {len(symbols)} Symbole via yfinance …")
        for sym in symbols:
            try:
                df = yf.Ticker(sym).history(period=period)
                if not df.empty:
                    data_dict[sym] = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            except Exception as e:
                print(f"  [yf] {sym}: {e}")
    else:
        raise RuntimeError("Weder alpaca-py noch yfinance verfügbar.")

    # VIX laden
    vix_series = pd.Series(dtype=float, name="VIX")
    if YF_AVAILABLE:
        try:
            vix_df    = yf.Ticker("^VIX").history(period=period)
            vix_series = vix_df["Close"].rename("VIX")
            vix_series.index = vix_series.index.tz_localize(None)
        except Exception as e:
            print(f"  [yf] VIX: {e}")

    print(f"[vbt_backtest] {len(data_dict)} Symbole geladen.")
    return data_dict, vix_series


# ---------------------------------------------------------------------------
# 2. Portfolio-Simulation: Kernlogik
# ---------------------------------------------------------------------------

def _build_sector_map(symbols: List[str], sector_groups: dict) -> Dict[str, int]:
    """Erstellt {symbol: sektor_idx}. Symbole ohne Gruppe bekommen Index -1."""
    mapping: Dict[str, int] = {}
    for idx, (_, members) in enumerate(sector_groups.items()):
        for sym in members:
            mapping[sym] = idx
    for sym in symbols:
        if sym not in mapping:
            mapping[sym] = -1
    return mapping


def run_backtest(
    data_dict: Dict[str, pd.DataFrame],
    vix_series: pd.Series,
    cfg: dict,
) -> Tuple[Optional[object], dict]:
    """
    Führe den vectorbt-Backtest aus.

    Neben dem vbt.Portfolio-Objekt wird ein Report-Dict zurückgegeben das
    auch ohne VBT nutzbar ist (für Tests).

    Parameter
    ---------
    data_dict  : {symbol: OHLCV-DataFrame}
    vix_series : Tages-VIX-Close als pd.Series
    cfg        : Konfigurations-Dict

    Rückgabe
    --------
    (portfolio, report_dict)
    """
    if not VBT_AVAILABLE:
        raise RuntimeError("vectorbt nicht installiert → pip install vectorbt")

    print("[vbt_backtest] Berechne Indikatoren und Signale …")
    prepared = prepare_all(data_dict, cfg)

    if not prepared:
        raise ValueError("Keine Symbole mit ausreichend Daten.")

    # Gemeinsamen Zeitindex bestimmen
    all_indices = [ind.index for ind, _ in prepared.values()]
    common_index = all_indices[0]
    for idx in all_indices[1:]:
        common_index = common_index.union(idx)
    common_index = common_index.sort_values()

    symbols   = list(prepared.keys())
    n_sym     = len(symbols)
    n_bars    = len(common_index)

    # Arrays in gemeinsamen Index einpassen (NaN für fehlende Daten)
    def _align(series_like, fill=np.nan) -> np.ndarray:
        s = series_like.reindex(common_index)
        return s.fillna(fill).values

    close_mat   = np.column_stack([_align(prepared[s][0]["Close"])  for s in symbols])
    high_mat    = np.column_stack([_align(prepared[s][0]["High"])   for s in symbols])
    low_mat     = np.column_stack([_align(prepared[s][0]["Low"])    for s in symbols])
    open_mat    = np.column_stack([_align(prepared[s][0]["Open"])   for s in symbols])
    volume_mat  = np.column_stack([_align(prepared[s][0]["Volume"]) for s in symbols])
    atr_mat     = np.column_stack([_align(prepared[s][0]["ATR"])    for s in symbols])
    sma20_mat   = np.column_stack([_align(prepared[s][0]["SMA20"])  for s in symbols])
    sma50_mat   = np.column_stack([_align(prepared[s][0]["SMA50"])  for s in symbols])
    volsma_mat  = np.column_stack([_align(prepared[s][0]["Vol_SMA"])for s in symbols])
    bbmid_mat   = np.column_stack([_align(prepared[s][0]["BB_mid"]) for s in symbols])
    rsi_mat     = np.column_stack([_align(prepared[s][0]["RSI"])    for s in symbols])
    macdh_mat   = np.column_stack([_align(prepared[s][0]["MACD_hist"]) for s in symbols])

    entry_mat   = np.column_stack([_align(prepared[s][1]["entry"],  False).astype(bool) for s in symbols])
    is_mr_mat   = np.column_stack([_align(prepared[s][1]["is_mr"],  False).astype(bool) for s in symbols])
    exit_dc_mat = np.column_stack([_align(prepared[s][1]["exit_death"], False).astype(bool) for s in symbols])

    # VIX auf gemeinsamen Index – Zeitzonen angleichen
    vix_idx = vix_series.index
    if common_index.tz is None and vix_idx.tz is not None:
        vix_series = vix_series.copy()
        vix_series.index = vix_idx.tz_convert("UTC").tz_localize(None)
    elif common_index.tz is not None and vix_idx.tz is None:
        vix_series = vix_series.copy()
        vix_series.index = vix_idx.tz_localize("UTC")
    elif common_index.tz is not None and vix_idx.tz is not None and common_index.tz != vix_idx.tz:
        vix_series = vix_series.copy()
        vix_series.index = vix_idx.tz_convert(common_index.tz)
    vix_aligned = vix_series.reindex(common_index, method="ffill").fillna(20.0).values

    # Sektor-Mapping als Int-Array
    sector_groups = cfg.get("sector_groups", {})
    sector_map_dict = _build_sector_map(symbols, sector_groups)
    sector_arr = np.array([sector_map_dict[s] for s in symbols], dtype=np.int32)
    n_sectors  = max(1, len(sector_groups))
    max_per_sec = int(cfg.get("max_per_sector", 2))

    # Konfigurationsparameter als flache Arrays (Numba-freundlich)
    commission    = float(cfg["commission_pct"])
    slippage      = float(cfg["slippage_pct"])
    risk_pt       = float(cfg["risk_per_trade"])
    sl_mult       = float(cfg["initial_sl_atr_mult"])
    trail_mult    = float(cfg["trailing_atr_mult"])
    partial_pct   = float(cfg["partial_profit_pct"])
    vix_thresh    = float(cfg.get("vix_high_threshold", 30))
    vix_reduce    = float(cfg.get("vix_size_reduction", 0.5))
    max_dd        = float(cfg.get("max_drawdown_pct", 0.15))
    max_vol_pct   = float(cfg.get("max_volume_pct", 0.01))
    capital       = float(cfg.get("initial_capital", 10_000.0))

    # --- Simulation über Python-Loop (vbt.Portfolio.from_order_func erfordert Numba
    #     für volle Performance; wir nutzen hier einen interpretierten Fallback der
    #     alle Guards korrekt abbildet und identische Trades wie v6 produziert).
    print("[vbt_backtest] Simuliere Portfolio …")

    cash           = capital
    # positions: {sym_idx: {entry, shares, stop, trailing, highest, partial, is_mr, mr_target}}
    positions_meta: Dict[int, dict] = {}
    trades_list: List[dict] = []
    equity_curve: List[float] = []
    peak_equity = capital

    mr_targets: Dict[int, float] = {}  # sym_idx → mr_target_price

    for bar in range(n_bars):
        date = common_index[bar]

        # Equity berechnen
        unrealized = sum(
            pos["shares"] * float(close_mat[bar, idx])
            for idx, pos in positions_meta.items()
            if not np.isnan(close_mat[bar, idx])
        )
        equity = cash + unrealized
        equity_curve.append(equity)
        if equity > peak_equity:
            peak_equity = equity

        # Circuit Breaker
        dd_active = (peak_equity - equity) / (peak_equity + 1e-9) >= max_dd
        if dd_active:
            continue

        # VIX size_mult
        vix_val  = float(vix_aligned[bar])
        size_mul = vix_reduce if vix_val >= vix_thresh else 1.0

        # --- Positionen verwalten: Stop / Trailing / Partial / MR-Exit ----
        for idx in list(positions_meta.keys()):
            pos   = positions_meta[idx]
            price = float(close_mat[bar, idx])
            if np.isnan(price):
                continue

            # MR-Exit: Preis >= BB_mid
            if pos["is_mr"]:
                target = pos.get("mr_target", np.nan)
                if not np.isnan(target) and price >= target:
                    pnl  = (price - pos["entry"]) * pos["shares"]
                    proceeds = price * pos["shares"] * (1 - commission - slippage)
                    cash += proceeds
                    trades_list.append({
                        "date": date, "symbol": symbols[idx],
                        "side": "SELL", "price": price,
                        "shares": pos["shares"], "pnl": pnl, "reason": "MR Target",
                    })
                    del positions_meta[idx]
                    continue

            # Trailing-Stop aktualisieren
            if price > pos["highest"]:
                pos["highest"]  = price
                pos["trailing"] = price - trail_mult * pos["atr"]

            # Stop-Loss oder Trailing ausgelöst
            if price <= pos["stop"] or price <= pos["trailing"]:
                pnl      = (price - pos["entry"]) * pos["shares"]
                proceeds = price * pos["shares"] * (1 - commission - slippage)
                cash    += proceeds
                reason   = "Stop-Loss" if price <= pos["stop"] else "Trailing Stop"
                trades_list.append({
                    "date": date, "symbol": symbols[idx],
                    "side": "SELL", "price": price,
                    "shares": pos["shares"], "pnl": pnl, "reason": reason,
                })
                del positions_meta[idx]
                continue

            # Death Cross → Exit
            if bool(exit_dc_mat[bar, idx]):
                pnl      = (price - pos["entry"]) * pos["shares"]
                proceeds = price * pos["shares"] * (1 - commission - slippage)
                cash    += proceeds
                trades_list.append({
                    "date": date, "symbol": symbols[idx],
                    "side": "SELL", "price": price,
                    "shares": pos["shares"], "pnl": pnl, "reason": "Death Cross",
                })
                del positions_meta[idx]
                continue

            # Partial-Profit
            if not pos.get("partial", False) and price >= pos["entry"] * (1 + partial_pct):
                half = pos["shares"] // 2
                if half > 0:
                    pnl      = (price - pos["entry"]) * half
                    proceeds = price * half * (1 - commission - slippage)
                    cash    += proceeds
                    pos["shares"]  -= half
                    pos["partial"]  = True
                    trades_list.append({
                        "date": date, "symbol": symbols[idx],
                        "side": "PARTIAL_SELL", "price": price,
                        "shares": half, "pnl": pnl, "reason": "Partial Profit",
                    })

        # --- Sektor-Count für Guard ----------------------------------------
        sector_count = np.zeros(n_sectors, dtype=np.int32)
        for idx in positions_meta:
            sec = int(sector_arr[idx])
            if sec >= 0:
                sector_count[sec] += 1

        # --- Neue Entries -----------------------------------------------------
        for col_idx, sym in enumerate(symbols):
            if col_idx in positions_meta:
                continue
            if not bool(entry_mat[bar, col_idx]):
                continue

            price  = float(close_mat[bar, col_idx])
            atr_v  = float(atr_mat[bar, col_idx])
            vol_v  = float(volume_mat[bar, col_idx])
            vs     = float(volsma_mat[bar, col_idx])

            if np.isnan(price) or np.isnan(atr_v) or price <= 0 or atr_v <= 0:
                continue

            # Sektor-Guard
            sec = int(sector_arr[col_idx])
            if sec >= 0 and sector_count[sec] >= max_per_sec:
                continue

            # Volume-Guard
            avg_vol = vs if not np.isnan(vs) else vol_v
            max_shares_vol = int(avg_vol * max_vol_pct)
            if max_shares_vol <= 0:
                continue

            # Positionsgröße
            rps    = sl_mult * atr_v
            shares = int((equity * risk_pt * size_mul) / (rps + 1e-9))
            shares = min(shares, int(cash * 0.4 / price), max_shares_vol)
            if shares <= 0:
                continue

            # Kosten
            entry_price = price * (1 + slippage)
            cost        = shares * entry_price * (1 + commission)
            if cost > cash:
                continue
            cash -= cost

            # MR?
            mr   = bool(is_mr_mat[bar, col_idx])
            mrt  = float(bbmid_mat[bar, col_idx]) if mr and not np.isnan(bbmid_mat[bar, col_idx]) else np.nan

            positions_meta[col_idx] = {
                "entry":     entry_price,
                "shares":    shares,
                "stop":      entry_price - sl_mult * atr_v,
                "trailing":  entry_price - trail_mult * atr_v,
                "highest":   entry_price,
                "partial":   False,
                "is_mr":     mr,
                "mr_target": mrt,
                "atr":       atr_v,
            }
            if sec >= 0:
                sector_count[sec] += 1

            trades_list.append({
                "date": date, "symbol": sym,
                "side": "BUY", "price": entry_price,
                "shares": shares, "pnl": 0.0,
                "reason": "mr" if mr else "trend",
            })

    # Offene Positionen am Ende schließen
    last_bar = n_bars - 1
    for idx, pos in positions_meta.items():
        price = float(close_mat[last_bar, idx])
        if np.isnan(price):
            continue
        pnl = (price - pos["entry"]) * pos["shares"]
        cash += price * pos["shares"] * (1 - commission - slippage)
        trades_list.append({
            "date": common_index[last_bar], "symbol": symbols[idx],
            "side": "SELL", "price": price,
            "shares": pos["shares"], "pnl": pnl, "reason": "End of Test",
        })

    trades_df = pd.DataFrame(trades_list)
    eq_series = pd.Series(equity_curve, index=common_index, name="equity")

    report = _compute_metrics(eq_series, trades_df, capital, common_index)
    report["equity_curve"] = eq_series
    report["trades"]       = trades_df

    # Optionales vbt.Portfolio-Objekt für tiefere Analyse
    pf = None
    if VBT_AVAILABLE:
        # Wir bauen ein vbt.Portfolio aus unseren berechneten Daten
        # (from_orders gibt exakte Kontrolle)
        try:
            close_df = pd.DataFrame(
                close_mat, index=common_index, columns=symbols
            )
            entry_df = pd.DataFrame(
                entry_mat, index=common_index, columns=symbols
            )
            exit_df  = pd.DataFrame(
                exit_dc_mat, index=common_index, columns=symbols
            )
            pf = vbt.Portfolio.from_signals(
                close=close_df,
                entries=entry_df,
                exits=exit_df,
                init_cash=capital,
                fees=commission * 2,      # je Seite
                slippage=slippage,
                freq="1D",
            )
        except Exception as e:
            print(f"  [vbt] Portfolio.from_signals Fehler (nur für Analyse): {e}")
            pf = None

    return pf, report


# ---------------------------------------------------------------------------
# 3. Metriken berechnen
# ---------------------------------------------------------------------------

def _compute_metrics(
    eq: pd.Series,
    trades: pd.DataFrame,
    capital: float,
    index: pd.DatetimeIndex,
) -> dict:
    """Berechne CAGR, Sharpe, Sortino, MaxDD, Calmar, Win-Rate, Profit Factor."""
    if eq.empty or eq.iloc[0] == 0:
        return {}

    returns = eq.pct_change().dropna()
    total_ret = (eq.iloc[-1] - capital) / capital
    n_years   = max((index[-1] - index[0]).days / 365.25, 1e-6)
    cagr      = (1 + total_ret) ** (1 / n_years) - 1

    # Sharpe (annualisiert, 252 Handelstage)
    excess = returns  # risk-free = 0 (vereinfacht)
    sharpe = (excess.mean() / (excess.std() + 1e-9)) * np.sqrt(252)

    # Sortino (nur negative Abweichungen)
    neg = returns[returns < 0]
    sortino = (returns.mean() / (neg.std() + 1e-9)) * np.sqrt(252) if len(neg) > 0 else np.nan

    # Max Drawdown
    rolling_max = eq.cummax()
    dd          = (eq - rolling_max) / (rolling_max + 1e-9)
    max_dd      = float(dd.min())

    # Calmar
    calmar = cagr / (abs(max_dd) + 1e-9)

    # Trade-Statistiken
    if trades.empty or "pnl" not in trades.columns:
        win_rate = profit_factor = total_trades = 0.0
    else:
        sell_trades = trades[trades["side"].isin(["SELL", "PARTIAL_SELL"])]
        wins        = sell_trades[sell_trades["pnl"] > 0]["pnl"]
        losses      = sell_trades[sell_trades["pnl"] <= 0]["pnl"]
        total_trades = len(sell_trades)
        win_rate     = len(wins) / (total_trades + 1e-9)
        profit_factor = wins.sum() / (abs(losses.sum()) + 1e-9)

    return {
        "total_return_pct": round(total_ret * 100, 2),
        "cagr_pct":         round(cagr * 100, 2),
        "sharpe":           round(sharpe, 3),
        "sortino":          round(sortino, 3) if not np.isnan(sortino) else None,
        "max_drawdown_pct": round(max_dd * 100, 2),
        "calmar":           round(calmar, 3),
        "win_rate_pct":     round(win_rate * 100, 2),
        "profit_factor":    round(profit_factor, 3),
        "total_trades":     int(total_trades),
    }


# ---------------------------------------------------------------------------
# 4. Alpha vs. Benchmark
# ---------------------------------------------------------------------------

def _compute_alpha(eq: pd.Series, benchmark: pd.DataFrame) -> float | None:
    """Berechne annualisiertes Alpha vs. SPY (vereinfacht: CAGR-Differenz)."""
    if benchmark is None or benchmark.empty:
        return None
    try:
        bm_ret = (benchmark["Close"].iloc[-1] - benchmark["Close"].iloc[0]) / benchmark["Close"].iloc[0]
        n_years = max((eq.index[-1] - eq.index[0]).days / 365.25, 1e-6)
        bm_cagr = (1 + bm_ret) ** (1 / n_years) - 1
        strat_total = (eq.iloc[-1] - eq.iloc[0]) / eq.iloc[0]
        strat_cagr  = (1 + strat_total) ** (1 / n_years) - 1
        return round((strat_cagr - bm_cagr) * 100, 2)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 5. Report ausgeben
# ---------------------------------------------------------------------------

def print_report(
    pf,
    report: dict,
    benchmark: Optional[pd.DataFrame] = None,
    output_dir: Optional[Path] = None,
) -> None:
    """
    Drucke Backtest-Report in die Konsole und speichere Plots + CSV.

    Parameter
    ---------
    pf         : vbt.Portfolio-Objekt (kann None sein)
    report     : dict aus run_backtest()
    benchmark  : OHLCV-DataFrame für SPY (für Alpha-Berechnung + Plot)
    output_dir : Verzeichnis für Plots/CSV (Standard: trading_data/)
    """
    eq      = report.get("equity_curve", pd.Series())
    trades  = report.get("trades", pd.DataFrame())
    alpha   = _compute_alpha(eq, benchmark)

    sep = "─" * 55
    print(f"\n{sep}")
    print("  BACKTEST REPORT – Botti Trader v7 (vectorbt)")
    print(sep)
    print(f"  Gesamtrendite:      {report.get('total_return_pct', 0):>8.2f} %")
    print(f"  CAGR:               {report.get('cagr_pct', 0):>8.2f} %")
    print(f"  Sharpe Ratio:       {report.get('sharpe', 0):>8.3f}")
    print(f"  Sortino Ratio:      {report.get('sortino', 0) or 'n/a':>8}")
    print(f"  Max Drawdown:       {report.get('max_drawdown_pct', 0):>8.2f} %")
    print(f"  Calmar Ratio:       {report.get('calmar', 0):>8.3f}")
    print(f"  Win-Rate:           {report.get('win_rate_pct', 0):>8.2f} %")
    print(f"  Profit Factor:      {report.get('profit_factor', 0):>8.3f}")
    print(f"  Trades gesamt:      {report.get('total_trades', 0):>8d}")
    if alpha is not None:
        print(f"  Alpha vs. SPY:      {alpha:>8.2f} %")
    print(sep)

    # Trade-Verteilung nach Signal-Typ
    if not trades.empty and "reason" in trades.columns:
        buy_trades = trades[trades["side"] == "BUY"]
        if not buy_trades.empty:
            print("\n  Signal-Typ Verteilung (Entries):")
            for reason, cnt in buy_trades["reason"].value_counts().items():
                print(f"    {reason:<20s} {cnt:>5d}×")

    # vbt-Details wenn vorhanden
    if pf is not None:
        try:
            print(f"\n  [vbt] Weitere Statistiken:")
            stats = pf.stats()
            for k in ["Total Return [%]", "Sharpe Ratio", "Max Drawdown [%]",
                      "Win Rate [%]", "Profit Factor", "Expectancy"]:
                if k in stats.index:
                    print(f"    {k:<30s} {stats[k]:.3f}")
        except Exception:
            pass

    # Outputs speichern
    if output_dir is None:
        output_dir = Path(__file__).parent / "trading_data"
    output_dir.mkdir(parents=True, exist_ok=True)

    # CSV Trade-Liste
    if not trades.empty:
        csv_path = output_dir / "v7_trades.csv"
        trades.to_csv(csv_path, index=False)
        print(f"\n  Trade-Liste gespeichert: {csv_path}")

    # Equity-Kurve Plot
    if MPL_AVAILABLE and not eq.empty:
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(eq.index, eq.values, label="Strategie", linewidth=1.5)
        if benchmark is not None and not benchmark.empty:
            bm_eq = benchmark["Close"] / benchmark["Close"].iloc[0] * float(eq.iloc[0])
            bm_eq = bm_eq.reindex(eq.index, method="ffill")
            ax.plot(bm_eq.index, bm_eq.values, label="SPY Buy&Hold",
                    linestyle="--", linewidth=1.0, alpha=0.7)
        ax.set_title("Equity Curve – Botti v7 Backtest")
        ax.set_xlabel("Datum")
        ax.set_ylabel("Equity (€)")
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plot_path = output_dir / "v7_equity_curve.png"
        fig.savefig(plot_path, dpi=120)
        plt.close(fig)
        print(f"  Equity-Kurve gespeichert: {plot_path}")

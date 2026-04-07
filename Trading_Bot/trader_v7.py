#!/usr/bin/env python3
"""
trader_v7.py – Botti Trader v7 – vectorbt Backtesting Suite.

Erweitert trader_v6 um:
  - Vollständig vektorisiertes Backtesting via vectorbt
  - Walk-Forward-Optimierung (Rolling, konfigurierbare Fenster)
  - Monte-Carlo-Simulation  (Reshuffling, Bootstrap, Noise-Injection)
  - Parameter-Sensitivitätsanalyse + Stabilitäts-Report

CONFIG wird aus trader_v6 importiert – keine Duplizierung.
trader_v6.py muss im gleichen Verzeichnis liegen.

Aufruf:
    python trader_v7.py --mode backtest  [--period 800d] [--symbols AAPL NVDA]
    python trader_v7.py --mode wfo       [--period 2000d]
    python trader_v7.py --mode montecarlo
    python trader_v7.py --mode all       # backtest → wfo → montecarlo hintereinander

    # Nur einzelne Symbole testen:
    python trader_v7.py --mode backtest --symbols NVDA AAPL CRWD

    # Paper-Daten über Alpaca (wenn trader_v6 Alpaca-Setup vorhanden):
    python trader_v7.py --mode backtest --alpaca

Installation:
    pip install vectorbt yfinance matplotlib numpy pandas
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path
from typing import Dict, List, Optional

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# CONFIG aus trader_v6 importieren (kein Duplikat)
# ---------------------------------------------------------------------------
try:
    # Verzeichnis des aktuellen Skripts in sys.path eintragen
    _here = Path(__file__).parent
    if str(_here) not in sys.path:
        sys.path.insert(0, str(_here))

    from trader_v6 import CONFIG as _BASE_CONFIG, AlpacaClient, ALPACA_AVAILABLE  # type: ignore
    import copy
    CONFIG = copy.deepcopy(_BASE_CONFIG)
    print("[v7] CONFIG aus trader_v6.py geladen.")
except ImportError as e:
    print(f"[WARN] trader_v6.py nicht gefunden ({e}). Verwende Minimal-CONFIG.")
    CONFIG = {
        "symbols": ["SPY", "QQQ", "AAPL", "NVDA", "TSLA", "META",
                    "CRWD", "GOOGL", "AMD", "PLTR"],
        "sector_groups": {
            "tech_semi":  ["NVDA", "AMD"],
            "tech_large": ["AAPL", "META", "GOOGL"],
            "tech_growth":["CRWD", "PLTR"],
            "etf_broad":  ["SPY", "QQQ"],
            "ev_space":   ["TSLA"],
        },
        "use_multi_timeframe":     False,
        "lower_timeframe":        "15Min",
        "lower_tf_lookback_days":  10,
        "pullback_entry":          True,
        "lower_ema_period":        20,
        "lower_rsi_min":           50,
        "max_per_sector":          2,
        "sma_short":               20,
        "sma_long":                50,
        "use_fast_cross":          True,
        "fast_cross_type":        "EMA",
        "fast_cross_short":        9,
        "fast_cross_long":         21,
        "use_early_golden_cross":  True,
        "early_gc_proximity_pct":  0.02,
        "early_gc_rsi_min":        55,
        "use_pullback_entry_daily":True,
        "pullback_daily_lookback": 15,
        "pullback_daily_ema":      20,
        "pullback_daily_proximity":0.015,
        "pullback_daily_rsi_min":  50,
        "use_rsi_filter":          True,
        "rsi_buy_min":             40,
        "rsi_buy_max":             70,
        "use_volume_filter":       True,
        "volume_sma_period":       20,
        "use_macd_filter":         True,
        "macd_fast":               12,
        "macd_slow":               26,
        "macd_signal_period":      9,
        "use_mean_reversion":      True,
        "bb_period":               20,
        "bb_std":                  2.0,
        "mr_rsi_max":              35,
        "mr_profit_target_pct":    0.05,
        "atr_period":              14,
        "risk_per_trade":          0.02,
        "max_equity_at_risk":      0.80,
        "partial_profit_pct":      0.25,
        "initial_sl_atr_mult":     2.5,
        "trailing_atr_mult":       3.0,
        "allow_reentry":           True,
        "reentry_atr_mult":        1.5,
        "adx_period":              14,
        "adx_threshold":           20,
        "use_vix_filter":          True,
        "vix_high_threshold":      30,
        "vix_size_reduction":      0.5,
        "max_drawdown_pct":        0.15,
        "commission_pct":          0.001,
        "slippage_pct":            0.001,
        "max_volume_pct":          0.01,
        "use_orb":                 True,
        "orb_minutes":             30,
        "orb_lookback_bars":       3,
        "orb_min_body_pct":        0.003,
        "orb_surge_volume":        1.3,
        "use_momentum_surge":      False,
        "surge_volume_mult":       1.5,
        "surge_rsi_min":           58,
        "surge_price_chg_pct":     0.003,
        "use_ml":                  False,
        "ml_prob_threshold":       0.6,
        "benchmark":              "SPY",
        "initial_capital":         10_000.0,
        "data_dir":               Path(__file__).parent / "trading_data",
    }
    ALPACA_AVAILABLE = False

from vbt_backtest  import load_data, run_backtest, print_report
from vbt_wfo       import WalkForwardOptimizer
from vbt_montecarlo import MonteCarlo


# ---------------------------------------------------------------------------
# Walk-Forward Parameter-Grid (Standard)
# ---------------------------------------------------------------------------

WFO_PARAM_GRID = {
    "sma_short":     [10, 20, 30],
    "sma_long":      [30, 50],
    "rsi_buy_min":   [35, 40, 50],
    "adx_threshold": [15, 20, 25],
}


# ---------------------------------------------------------------------------
# Modus-Funktionen
# ---------------------------------------------------------------------------

def mode_backtest(
    cfg: dict,
    symbols: List[str],
    period: str,
    alpaca_client=None,
) -> dict:
    """Einfacher Backtest über alle Symbole."""
    print(f"\n{'='*60}")
    print(f"  BOTTI TRADER v7 – BACKTEST ({period})")
    print(f"  Symbole: {len(symbols)} | Kapital: {cfg['initial_capital']:,.0f}")
    print(f"{'='*60}")

    data, vix = load_data(symbols, period=period, alpaca=alpaca_client)

    if not data:
        print("[ERROR] Keine Daten geladen.")
        return {}

    benchmark = data.get(cfg.get("benchmark", "SPY"))

    pf, report = run_backtest(data, vix, cfg)
    print_report(pf, report, benchmark=benchmark,
                 output_dir=cfg.get("data_dir", Path("trading_data")))

    return report


def mode_wfo(
    cfg: dict,
    symbols: List[str],
    period: str,
    is_days:   int = 500,
    oos_days:  int = 125,
    step_days: int = 63,
    alpaca_client=None,
) -> WalkForwardOptimizer:
    """Walk-Forward-Optimierung."""
    print(f"\n{'='*60}")
    print(f"  BOTTI TRADER v7 – WALK-FORWARD-OPTIMIERUNG")
    print(f"  IS={is_days}d  OOS={oos_days}d  Step={step_days}d")
    print(f"{'='*60}")

    data, vix = load_data(symbols, period=period, alpaca=alpaca_client)
    if not data:
        print("[ERROR] Keine Daten geladen.")
        return None

    wfo = WalkForwardOptimizer(
        data_dict  = data,
        vix_series = vix,
        base_cfg   = cfg,
        param_grid = WFO_PARAM_GRID,
        is_days    = is_days,
        oos_days   = oos_days,
        step_days  = step_days,
        metric     = "sharpe",
        verbose    = True,
    )
    wfo.run()
    wfo.print_summary()
    wfo.stability_report()

    # Kombinierte OOS-Equity-Kurve ausgeben
    oos_eq = wfo.combined_oos_equity()
    if not oos_eq.empty:
        total_oos = (oos_eq.iloc[-1] - oos_eq.iloc[0]) / oos_eq.iloc[0] * 100
        print(f"\n  Kombinierte OOS-Gesamtrendite: {total_oos:.2f}%")

    return wfo


def mode_montecarlo(
    report: dict,
    cfg: dict,
    n: int = 2000,
    noise_pct: float = 0.005,
) -> MonteCarlo:
    """Monte-Carlo-Simulation auf Basis eines bestehenden Backtest-Reports."""
    print(f"\n{'='*60}")
    print(f"  BOTTI TRADER v7 – MONTE-CARLO-SIMULATION ({n} Läufe)")
    print(f"{'='*60}")

    trades = report.get("trades")
    if trades is None or trades.empty:
        print("[ERROR] Keine Trades im Report – zuerst Backtest ausführen.")
        return None
    if "pnl" not in trades.columns:
        print("[ERROR] Trade-DataFrame hat keine 'pnl'-Spalte.")
        return None

    mc = MonteCarlo(
        trades_df       = trades,
        initial_capital = cfg.get("initial_capital", 10_000.0),
    )
    mc.run_reshuffling(n=n)
    mc.run_bootstrap(n=n)
    mc.run_noise(n=n, noise_pct=noise_pct)
    mc.summary()
    mc.plot_distributions(output_dir=cfg.get("data_dir", Path("trading_data")))
    mc.export_csv(output_dir=cfg.get("data_dir", Path("trading_data")))

    return mc


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="trader_v7.py",
        description="Botti Trader v7 – vectorbt Backtesting Suite",
    )
    p.add_argument(
        "--mode", "-m",
        choices=["backtest", "wfo", "montecarlo", "all"],
        default="backtest",
        help="Ausführungsmodus",
    )
    p.add_argument(
        "--period", "-p",
        default="800d",
        help="Datenzeitraum z.B. 800d oder 2000d (Standard: 800d)",
    )
    p.add_argument(
        "--symbols", "-s",
        nargs="+",
        default=None,
        help="Symbole überschreiben (Standard: CONFIG[symbols])",
    )
    p.add_argument(
        "--alpaca",
        action="store_true",
        help="Daten über Alpaca laden (erfordert trader_v6 + API-Keys)",
    )
    p.add_argument(
        "--is-days",
        type=int,
        default=500,
        help="In-Sample-Länge für WFO in Tagen (Standard: 500)",
    )
    p.add_argument(
        "--oos-days",
        type=int,
        default=125,
        help="Out-of-Sample-Länge für WFO in Tagen (Standard: 125)",
    )
    p.add_argument(
        "--step-days",
        type=int,
        default=63,
        help="Schrittweite für WFO in Tagen (Standard: 63)",
    )
    p.add_argument(
        "--mc-runs",
        type=int,
        default=2000,
        help="Anzahl Monte-Carlo-Läufe (Standard: 2000)",
    )
    return p


def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    cfg     = CONFIG.copy()
    symbols = args.symbols or cfg["symbols"]

    # data_dir sicherstellen
    data_dir = cfg.get("data_dir", Path(__file__).parent / "trading_data")
    if isinstance(data_dir, str):
        data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    cfg["data_dir"] = data_dir

    # Alpaca-Client
    alpaca_client = None
    if args.alpaca and ALPACA_AVAILABLE:
        import os
        key    = os.environ.get("APCA_API_KEY_ID", "")
        secret = os.environ.get("APCA_API_SECRET_KEY", "")
        paper  = os.environ.get("APCA_PAPER", "true").lower() != "false"
        feed   = os.environ.get("APCA_DATA_FEED", cfg.get("alpaca_data_feed", "iex"))
        if key and secret:
            alpaca_client = AlpacaClient(key, secret, paper=paper, data_feed=feed)
            print(f"[v7] Alpaca-Client initialisiert (paper={paper}, feed={feed})")
        else:
            print("[WARN] APCA_API_KEY_ID / APCA_API_SECRET_KEY nicht gesetzt.")

    # --- Ausführung ---------------------------------------------------------
    report: dict = {}

    if args.mode in ("backtest", "all"):
        report = mode_backtest(cfg, symbols, period=args.period,
                               alpaca_client=alpaca_client)

    if args.mode in ("wfo", "all"):
        wfo_period = args.period if args.mode == "wfo" else "2000d"
        mode_wfo(cfg, symbols,
                 period    = wfo_period,
                 is_days   = args.is_days,
                 oos_days  = args.oos_days,
                 step_days = args.step_days,
                 alpaca_client = alpaca_client)

    if args.mode in ("montecarlo", "all"):
        if not report:
            # Backtest zuerst ausführen um Trades zu bekommen
            print("[v7] Monte-Carlo benötigt Backtest-Ergebnisse → führe Backtest aus …")
            report = mode_backtest(cfg, symbols, period=args.period,
                                   alpaca_client=alpaca_client)
        mode_montecarlo(report, cfg, n=args.mc_runs)


# ---------------------------------------------------------------------------
# Direkte Nutzung als Modul (z.B. in Jupyter)
# ---------------------------------------------------------------------------

def quick_backtest(
    symbols: Optional[List[str]] = None,
    period: str = "800d",
    cfg_overrides: Optional[dict] = None,
    alpaca_client=None,
) -> dict:
    """
    Schnell-Backtest ohne CLI.

    Beispiel (Jupyter):
        from trader_v7 import quick_backtest
        report = quick_backtest(symbols=["NVDA", "AAPL"], period="400d")
    """
    cfg = CONFIG.copy()
    if cfg_overrides:
        cfg.update(cfg_overrides)
    syms = symbols or cfg["symbols"]
    return mode_backtest(cfg, syms, period=period, alpaca_client=alpaca_client)


def quick_wfo(
    symbols: Optional[List[str]] = None,
    period: str = "2000d",
    cfg_overrides: Optional[dict] = None,
    is_days: int = 500,
    oos_days: int = 125,
    step_days: int = 63,
) -> WalkForwardOptimizer:
    """
    Schnell-WFO ohne CLI.

    Beispiel (Jupyter):
        from trader_v7 import quick_wfo
        wfo = quick_wfo(symbols=["NVDA", "AAPL"], period="2000d")
        oos = wfo.combined_oos_equity()
    """
    cfg = CONFIG.copy()
    if cfg_overrides:
        cfg.update(cfg_overrides)
    syms = symbols or cfg["symbols"]
    return mode_wfo(cfg, syms, period=period,
                    is_days=is_days, oos_days=oos_days, step_days=step_days)


def quick_montecarlo(
    report: dict,
    n: int = 2000,
    cfg_overrides: Optional[dict] = None,
) -> MonteCarlo:
    """
    Schnell-Monte-Carlo ohne CLI.

    Beispiel (Jupyter):
        from trader_v7 import quick_backtest, quick_montecarlo
        report = quick_backtest()
        mc = quick_montecarlo(report, n=3000)
    """
    cfg = CONFIG.copy()
    if cfg_overrides:
        cfg.update(cfg_overrides)
    return mode_montecarlo(report, cfg, n=n)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()

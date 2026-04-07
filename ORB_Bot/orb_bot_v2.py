#!/usr/bin/env python3
"""
orb_bot_v2.py – ORB Bot v2 – Backtest-Suite mit WFO + Monte Carlo.

Erweitert den ORB Bot um:
  - Vollständigen Intraday-Backtest (5m-Bars, Bar-by-Bar)
  - Walk-Forward-Optimierung (importiert WalkForwardOptimizer aus orb_wfo)
  - Monte-Carlo-Simulation   (importiert MonteCarlo aus orb_montecarlo)
  - Long + Short Trades
  - Guards: VIX, DD-Breaker, Volume, Max-Daily-Trades
  - Trend-Filter (SPY EMA-20), Gap-Filter, ORB-Range Stops

Datenquelle: Alpaca API (5-Minuten-Bars).

Aufruf:
    python orb_bot_v2.py --mode backtest  --start 2024-06-01 --end 2025-04-01
    python orb_bot_v2.py --mode wfo       --start 2023-06-01 --end 2025-04-01
    python orb_bot_v2.py --mode montecarlo
    python orb_bot_v2.py --mode all       --start 2024-01-01 --end 2025-04-01

    # Short-Trades aktivieren:
    python orb_bot_v2.py --mode backtest --shorts --start 2024-06-01

    # Eigene Symbole:
    python orb_bot_v2.py --mode backtest --symbols SPY QQQ AAPL NVDA

Installation:
    pip install alpaca-py yfinance matplotlib numpy pandas pytz
"""

from __future__ import annotations

import argparse
import copy
import os
import sys
import warnings
from datetime import time
from pathlib import Path
from typing import Dict, List, Optional

warnings.filterwarnings("ignore")

## für eine lokale Ausführung ohne OpenClaw-Umgebung können die
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Lokale Module (v7-unabhängig)
# ---------------------------------------------------------------------------
_here = Path(__file__).resolve().parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

try:
    from orb_wfo import WalkForwardOptimizer  # type: ignore
    WFO_AVAILABLE = True
except ImportError:
    WFO_AVAILABLE = False
    print("[WARN] orb_wfo.py nicht gefunden – WFO nicht verfügbar")

try:
    from orb_montecarlo import MonteCarlo  # type: ignore
    MC_AVAILABLE = True
except ImportError:
    MC_AVAILABLE = False
    print("[WARN] orb_montecarlo.py nicht gefunden – Monte Carlo nicht verfügbar")

from orb_backtest import load_orb_data, run_orb_backtest, print_orb_report
from orb_strategy import ORB_DEFAULT_CONFIG

# AlpacaClient aus orb_bot_alpaca importieren
try:
    from orb_bot_alpaca import AlpacaClient, ALPACA_AVAILABLE  # type: ignore
except ImportError:
    ALPACA_AVAILABLE = False


# ============================= Konfiguration ================================
# Erbt Defaults aus orb_strategy (Single Source of Truth)

ORB_CONFIG = copy.deepcopy(ORB_DEFAULT_CONFIG)
ORB_CONFIG.update({
    "data_dir": Path(__file__).parent / "orb_trading_data",
})


# ============================= WFO Parameter-Grid ===========================

WFO_PARAM_GRID = {
    "profit_target_r":   [1.5, 2.0, 3.0],
    "trail_after_r":     [0.5, 1.0, 1.5],
    "trail_distance_r":  [0.3, 0.5, 0.75],
    "volume_multiplier": [1.0, 1.3, 1.5],
}
# 3 × 3 × 3 × 3 = 81 Kombinationen – überschaubar


# ============================= Modus-Funktionen =============================

def mode_backtest(
    cfg: dict,
    symbols: List[str],
    start: str,
    end: str,
    alpaca_client=None,
) -> dict:
    """ORB Backtest."""
    print(f"\n{'=' * 60}")
    print(f"  ORB BOT v2 – BACKTEST")
    print(f"  Zeitraum: {start} → {end}")
    print(f"  Symbole: {len(symbols)} | Kapital: {cfg['initial_capital']:,.0f}")
    print(f"  Shorts: {'AN' if cfg.get('allow_shorts') else 'AUS'}")
    print(f"{'=' * 60}")

    data, vix = load_orb_data(symbols, start, end, alpaca=alpaca_client)
    if not data:
        print("[ERROR] Keine Daten geladen.")
        return {}

    _, report = run_orb_backtest(data, vix, cfg)
    print_orb_report(report, output_dir=cfg.get("data_dir", Path("orb_trading_data")))
    return report


def mode_wfo(
    cfg: dict,
    symbols: List[str],
    start: str,
    end: str,
    is_days: int = 120,
    oos_days: int = 30,
    step_days: int = 20,
    alpaca_client=None,
) -> Optional[WalkForwardOptimizer]:
    """Walk-Forward-Optimierung für ORB-Parameter."""
    if not WFO_AVAILABLE:
        print("[ERROR] orb_wfo.py nicht verfügbar.")
        return None

    print(f"\n{'=' * 60}")
    print(f"  ORB BOT v2 – WALK-FORWARD-OPTIMIERUNG")
    print(f"  IS={is_days}d  OOS={oos_days}d  Step={step_days}d")
    print(f"  Zeitraum: {start} → {end}")
    print(f"{'=' * 60}")

    data, vix = load_orb_data(symbols, start, end, alpaca=alpaca_client)
    if not data:
        print("[ERROR] Keine Daten geladen.")
        return None

    # VIX Index normalisieren (WFO nutzt date-based Slicing)
    # WFO erwartet Tages-Index → wir müssen einen Tages-Index aus 5m-Daten bauen
    # Die WFO-Daten-Slicerung funktioniert über _slice_data, die auf Datums-Level schneidet

    wfo = WalkForwardOptimizer(
        data_dict=data,
        vix_series=vix,
        base_cfg=cfg,
        param_grid=WFO_PARAM_GRID,
        is_days=is_days,
        oos_days=oos_days,
        step_days=step_days,
        metric="sharpe",
        verbose=True,
        backtest_func=run_orb_backtest,
        validation_func=_orb_validate_params,
    )
    wfo.run()
    wfo.print_summary()
    wfo.stability_report()

    oos_eq = wfo.combined_oos_equity()
    if not oos_eq.empty:
        total = (oos_eq.iloc[-1] - oos_eq.iloc[0]) / oos_eq.iloc[0] * 100
        print(f"\n  Kombinierte OOS-Gesamtrendite: {total:.2f}%")

    return wfo


def mode_montecarlo(
    report: dict,
    cfg: dict,
    n: int = 2000,
    noise_pct: float = 0.005,
) -> Optional[object]:
    """Monte-Carlo-Simulation auf Basis des ORB-Backtest-Reports."""
    if not MC_AVAILABLE:
        print("[ERROR] orb_montecarlo.py nicht verfügbar.")
        return None

    print(f"\n{'=' * 60}")
    print(f"  ORB BOT v2 – MONTE-CARLO-SIMULATION ({n} Läufe)")
    print(f"{'=' * 60}")

    trades = report.get("trades")
    if trades is None or trades.empty:
        print("[ERROR] Keine Trades – zuerst Backtest ausführen.")
        return None
    if "pnl" not in trades.columns:
        print("[ERROR] Trade-DataFrame hat keine 'pnl'-Spalte.")
        return None

    mc = MonteCarlo(
        trades_df=trades,
        initial_capital=cfg.get("initial_capital", 10_000.0),
    )
    mc.run_reshuffling(n=n)
    mc.run_bootstrap(n=n)
    mc.run_noise(n=n, noise_pct=noise_pct)
    mc.summary()
    mc.plot_distributions(output_dir=cfg.get("data_dir", Path("orb_trading_data")))
    mc.export_csv(output_dir=cfg.get("data_dir", Path("orb_trading_data")))

    return mc


# ============================= Validierung ==================================

def _orb_validate_params(cfg: dict, overrides: dict) -> bool:
    """Validierung für ORB-spezifische Parameterkombinationen."""
    pt = cfg.get("profit_target_r", 2.0)
    sl = cfg.get("stop_loss_r", 1.0)
    trail = cfg.get("trail_after_r", 1.0)
    # Profit Target muss größer als Trail-Aktivierung sein
    if pt <= trail:
        return False
    # Trail-Aktivierung muss positiv sein
    if trail <= 0:
        return False
    return True


# ============================= Alpaca-Client ================================

def _build_alpaca_client(cfg: dict):
    """Alpaca-Client aus Umgebungsvariablen bauen."""
    if not ALPACA_AVAILABLE:
        print("[ERROR] alpaca-py fehlt – pip install alpaca-py")
        return None

    key = os.getenv("APCA_API_KEY_ID", "")
    secret = os.getenv("APCA_API_SECRET_KEY", "")

    if not key or not secret:
        print("[ERROR] APCA_API_KEY_ID / APCA_API_SECRET_KEY nicht gesetzt.")
        return None

    paper_env = os.getenv("APCA_PAPER", "true").lower()
    paper = paper_env != "false"
    feed = os.getenv("APCA_DATA_FEED", cfg.get("alpaca_data_feed", "iex"))

    return AlpacaClient(api_key=key, secret_key=secret, paper=paper, data_feed=feed)


# ============================= CLI ==========================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="orb_bot_v2.py",
        description="ORB Bot v2 – Backtest-Suite mit WFO + Monte Carlo",
    )
    p.add_argument(
        "--mode", "-m",
        choices=["backtest", "wfo", "montecarlo", "all"],
        default="backtest",
        help="Ausführungsmodus",
    )
    p.add_argument("--start", default="2024-06-01",
                   help="Start-Datum (YYYY-MM-DD, Standard: 2024-06-01)")
    p.add_argument("--end", default=None,
                   help="End-Datum (YYYY-MM-DD, Standard: heute)")
    p.add_argument("--symbols", "-s", nargs="+", default=None,
                   help="Symbole überschreiben")
    p.add_argument("--shorts", action="store_true",
                   help="Short-Trades aktivieren")
    p.add_argument("--is-days", type=int, default=120,
                   help="IS-Tage für WFO (Standard: 120)")
    p.add_argument("--oos-days", type=int, default=30,
                   help="OOS-Tage für WFO (Standard: 30)")
    p.add_argument("--step-days", type=int, default=20,
                   help="Schrittweite für WFO (Standard: 20)")
    p.add_argument("--mc-runs", type=int, default=2000,
                   help="Monte-Carlo-Läufe (Standard: 2000)")
    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    from datetime import datetime as dt
    if args.end is None:
        args.end = dt.now().strftime("%Y-%m-%d")

    cfg = copy.deepcopy(ORB_CONFIG)
    if args.shorts:
        cfg["allow_shorts"] = True
    symbols = args.symbols or cfg["symbols"]

    # data_dir sicherstellen
    data_dir = cfg.get("data_dir", Path(__file__).parent / "orb_trading_data")
    if isinstance(data_dir, str):
        data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    cfg["data_dir"] = data_dir

    # Alpaca-Client
    alpaca = _build_alpaca_client(cfg)

    # ── Ausführung ─────────────────────────────────────────────────────────
    report: dict = {}

    if args.mode in ("backtest", "all"):
        report = mode_backtest(cfg, symbols, start=args.start, end=args.end,
                               alpaca_client=alpaca)

    if args.mode in ("wfo", "all"):
        wfo_start = args.start if args.mode == "wfo" else args.start
        mode_wfo(cfg, symbols, start=wfo_start, end=args.end,
                 is_days=args.is_days, oos_days=args.oos_days,
                 step_days=args.step_days, alpaca_client=alpaca)

    if args.mode in ("montecarlo", "all"):
        if not report:
            print("[v2] Monte-Carlo benötigt Backtest → führe Backtest aus …")
            report = mode_backtest(cfg, symbols, start=args.start, end=args.end,
                                   alpaca_client=alpaca)
        mode_montecarlo(report, cfg, n=args.mc_runs)


# ============================= Jupyter-API ==================================

def quick_backtest(
    symbols: Optional[List[str]] = None,
    start: str = "2024-06-01",
    end: Optional[str] = None,
    cfg_overrides: Optional[dict] = None,
    alpaca_client=None,
) -> dict:
    """
    Schnell-Backtest ohne CLI.

    Beispiel (Jupyter):
        from orb_bot_v2 import quick_backtest
        report = quick_backtest(symbols=["SPY", "QQQ"], start="2025-01-01")
    """
    from datetime import datetime as dt
    cfg = copy.deepcopy(ORB_CONFIG)
    if cfg_overrides:
        cfg.update(cfg_overrides)
    syms = symbols or cfg["symbols"]
    end = end or dt.now().strftime("%Y-%m-%d")
    return mode_backtest(cfg, syms, start=start, end=end, alpaca_client=alpaca_client)


def quick_wfo(
    symbols: Optional[List[str]] = None,
    start: str = "2023-06-01",
    end: Optional[str] = None,
    is_days: int = 120,
    oos_days: int = 30,
    step_days: int = 20,
    cfg_overrides: Optional[dict] = None,
    alpaca_client=None,
):
    """
    Schnell-WFO ohne CLI.

    Beispiel (Jupyter):
        from orb_bot_v2 import quick_wfo
        wfo = quick_wfo(symbols=["SPY", "QQQ"])
    """
    from datetime import datetime as dt
    cfg = copy.deepcopy(ORB_CONFIG)
    if cfg_overrides:
        cfg.update(cfg_overrides)
    syms = symbols or cfg["symbols"]
    end = end or dt.now().strftime("%Y-%m-%d")
    return mode_wfo(cfg, syms, start=start, end=end,
                    is_days=is_days, oos_days=oos_days, step_days=step_days,
                    alpaca_client=alpaca_client)


def quick_montecarlo(
    report: dict,
    n: int = 2000,
    cfg_overrides: Optional[dict] = None,
):
    """
    Schnell-Monte-Carlo ohne CLI.

    Beispiel (Jupyter):
        from orb_bot_v2 import quick_backtest, quick_montecarlo
        report = quick_backtest()
        mc = quick_montecarlo(report)
    """
    cfg = copy.deepcopy(ORB_CONFIG)
    if cfg_overrides:
        cfg.update(cfg_overrides)
    return mode_montecarlo(report, cfg, n=n)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()

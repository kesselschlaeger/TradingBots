#!/usr/bin/env python3
"""
vbt_montecarlo.py – Monte-Carlo-Simulation für Botti Trader v7.

Drei Methoden:
  1. Trade-Reshuffling    – zufällige Reihenfolge der realisierten Trades
  2. Bootstrap-Sampling  – Trades mit Zurücklegen samplen
  3. Noise-Injection     – Trade-Returns ± zufälliges Gaussian-Noise

Jede Methode erzeugt N Equity-Kurven und berechnet:
  - Maximaler Drawdown (MaxDD)
  - Sharpe Ratio
  - Profit Factor
  - CAGR

Outputs:
  - Kennzahlen-Verteilungen (Median + 5./95. Perzentil)
  - 4er-Histogramm-Plot
  - CSV-Export der Simulations-Ergebnisse

Nutzung:
    from vbt_montecarlo import MonteCarlo

    mc = MonteCarlo(trades_df=report["trades"], initial_capital=10_000)
    mc.run_reshuffling(n=2000)
    mc.run_bootstrap(n=2000)
    mc.run_noise(n=2000, noise_pct=0.005)
    mc.summary()
    mc.plot_distributions(output_dir=Path("trading_data"))
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    MPL_AVAILABLE = True
except ImportError:
    MPL_AVAILABLE = False


# ---------------------------------------------------------------------------
# Hilfs-Funktionen
# ---------------------------------------------------------------------------

def _equity_from_returns(returns: np.ndarray, capital: float) -> np.ndarray:
    """Erstelle eine Equity-Kurve aus einem Array von Trade-Returns (PnL-Beträge)."""
    eq = np.empty(len(returns) + 1)
    eq[0] = capital
    for i, r in enumerate(returns):
        eq[i + 1] = eq[i] + r
    return eq


def _metrics_from_equity(eq: np.ndarray) -> Dict[str, float]:
    """Berechne Kennzahlen aus einer Equity-Kurve (numpy Array)."""
    if len(eq) < 2 or eq[0] <= 0:
        return {"cagr": 0.0, "sharpe": 0.0, "max_dd": 0.0, "profit_factor": 0.0}

    # CAGR (keine echten Daten → Anzahl Trades als Proxy für Zeit)
    n_trades = len(eq) - 1
    years    = max(n_trades / 250, 1e-6)       # ~250 Trades/Jahr Annahme
    total    = (eq[-1] - eq[0]) / eq[0]
    cagr     = (1 + total) ** (1 / years) - 1

    # Tages-Returns des Equity-Vektors
    rets = np.diff(eq) / (eq[:-1] + 1e-9)

    # Sharpe
    std = rets.std()
    sharpe = (rets.mean() / (std + 1e-9)) * np.sqrt(250)

    # Max Drawdown
    roll_max = np.maximum.accumulate(eq)
    dd       = (eq - roll_max) / (roll_max + 1e-9)
    max_dd   = float(dd.min())

    # Profit Factor (positive PnL / |negative PnL|)
    pnl      = np.diff(eq)
    wins     = pnl[pnl > 0].sum()
    losses   = abs(pnl[pnl < 0].sum())
    pf       = wins / (losses + 1e-9)

    return {
        "cagr":          float(cagr),
        "sharpe":        float(sharpe),
        "max_dd":        float(max_dd),
        "profit_factor": float(pf),
    }


# ---------------------------------------------------------------------------
# Haupt-Klasse
# ---------------------------------------------------------------------------

class MonteCarlo:
    """
    Monte-Carlo-Simulation auf Basis realisierter Trades.

    Parameter
    ---------
    trades_df       : Trade-DataFrame aus vbt_backtest.run_backtest()
                      muss Spalte "pnl" (realisierter Gewinn/Verlust) enthalten.
    initial_capital : Startkapital (Standard: 10 000)
    """

    def __init__(self, trades_df: pd.DataFrame, initial_capital: float = 10_000.0):
        if "pnl" not in trades_df.columns:
            raise ValueError("trades_df muss eine 'pnl'-Spalte enthalten.")

        # Nur SELL/PARTIAL_SELL-Trades mit realisiertem PnL
        mask = trades_df["side"].isin(["SELL", "PARTIAL_SELL"])
        self._pnl = trades_df.loc[mask, "pnl"].dropna().values.astype(float)

        self.capital = float(initial_capital)
        self._results: Dict[str, List[Dict[str, float]]] = {
            "reshuffling": [],
            "bootstrap":   [],
            "noise":       [],
        }

    # ------------------------------------------------------------------

    def run_reshuffling(self, n: int = 2000, seed: Optional[int] = None) -> None:
        """
        Methode 1: Trade-Reshuffling.

        Zufällige Permutation der realisierten Trade-PnLs → N Equity-Kurven.
        Beantwortet: «Wäre das Ergebnis auch anders ausgefallen, wenn die
        gleichen Trades in anderer Reihenfolge aufgetreten wären?»
        """
        rng     = np.random.default_rng(seed)
        results = []
        for _ in range(n):
            shuffled = rng.permutation(self._pnl)
            eq       = _equity_from_returns(shuffled, self.capital)
            results.append(_metrics_from_equity(eq))
        self._results["reshuffling"] = results
        print(f"[MC] Reshuffling: {n} Läufe abgeschlossen.")

    # ------------------------------------------------------------------

    def run_bootstrap(self, n: int = 2000, seed: Optional[int] = None) -> None:
        """
        Methode 2: Bootstrap-Sampling mit Zurücklegen.

        Samplet aus den realisierten Trade-PnLs (gleiche Länge, mit Zurücklegen).
        Beantwortet: «Wie robust ist das Ergebnis gegen Zufall bei der
        Auswahl der Trades?»
        """
        rng     = np.random.default_rng(seed)
        n_obs   = len(self._pnl)
        results = []
        for _ in range(n):
            sample = rng.choice(self._pnl, size=n_obs, replace=True)
            eq     = _equity_from_returns(sample, self.capital)
            results.append(_metrics_from_equity(eq))
        self._results["bootstrap"] = results
        print(f"[MC] Bootstrap: {n} Läufe abgeschlossen.")

    # ------------------------------------------------------------------

    def run_noise(
        self,
        n: int = 2000,
        noise_pct: float = 0.005,
        seed: Optional[int] = None,
    ) -> None:
        """
        Methode 3: Noise-Injection.

        Addiert zufälliges Gaussian-Noise (Sigma = noise_pct × |PnL|) zu jedem
        Trade-PnL. Simuliert leicht veränderte Entry/Exit-Preise.
        Beantwortet: «Wie empfindlich ist das Ergebnis gegen leichte Timing-
        Unterschiede?»
        """
        rng     = np.random.default_rng(seed)
        results = []
        abs_pnl = np.abs(self._pnl)
        for _ in range(n):
            noise  = rng.normal(0, noise_pct * abs_pnl)
            noisy  = self._pnl + noise
            eq     = _equity_from_returns(noisy, self.capital)
            results.append(_metrics_from_equity(eq))
        self._results["noise"] = results
        print(f"[MC] Noise (σ={noise_pct:.1%}): {n} Läufe abgeschlossen.")

    # ------------------------------------------------------------------

    def summary(self) -> pd.DataFrame:
        """
        Drucke und gibt DataFrame zurück mit Median + 5./95. Perzentil
        für jede Methode und Metrik.
        """
        metrics = ["cagr", "sharpe", "max_dd", "profit_factor"]
        rows: List[dict] = []

        sep = "─" * 70
        print(f"\n{sep}")
        print("  MONTE-CARLO ZUSAMMENFASSUNG")
        print(sep)

        for method, results in self._results.items():
            if not results:
                continue
            df = pd.DataFrame(results)
            print(f"\n  [{method.upper()}] ({len(results)} Läufe)")
            print(f"  {'Metrik':<20} {'Median':>10} {'5. Pzl.':>10} {'95. Pzl.':>10}")
            print(f"  {'─'*20} {'─'*10} {'─'*10} {'─'*10}")
            for m in metrics:
                if m not in df.columns:
                    continue
                med = df[m].median()
                p05 = df[m].quantile(0.05)
                p95 = df[m].quantile(0.95)

                unit = " %" if m in ("cagr", "max_dd") else ""
                mult = 100 if m in ("cagr", "max_dd") else 1
                print(
                    f"  {m:<20} "
                    f"{med*mult:>10.2f}{unit}  "
                    f"{p05*mult:>9.2f}{unit}  "
                    f"{p95*mult:>9.2f}{unit}"
                )
                rows.append({
                    "method": method, "metric": m,
                    "median": med, "p05": p05, "p95": p95,
                })

        print(sep)
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------

    def plot_distributions(
        self,
        output_dir: Optional[Path] = None,
        methods: Optional[List[str]] = None,
    ) -> None:
        """
        Erstelle Histogramme für alle vier Kennzahlen.

        Für jede Methode und Metrik ein Subplot.
        Speichert als PNG in output_dir.
        """
        if not MPL_AVAILABLE:
            print("[MC] matplotlib nicht installiert – keine Plots.")
            return

        if output_dir is None:
            output_dir = Path(__file__).parent / "trading_data"
        output_dir.mkdir(parents=True, exist_ok=True)

        active_methods = methods or [m for m in ("reshuffling", "bootstrap", "noise")
                                     if self._results[m]]
        if not active_methods:
            print("[MC] Keine Simulations-Ergebnisse für Plots vorhanden.")
            return

        metrics = [
            ("max_dd",        "Max Drawdown",  "×100 → %",  100),
            ("sharpe",        "Sharpe Ratio",  "",           1),
            ("profit_factor", "Profit Factor", "",           1),
            ("cagr",          "CAGR",          "×100 → %",  100),
        ]

        for method in active_methods:
            results = self._results.get(method, [])
            if not results:
                continue

            df  = pd.DataFrame(results)
            fig, axes = plt.subplots(2, 2, figsize=(14, 8))
            fig.suptitle(
                f"Monte-Carlo Verteilungen – {method.capitalize()} "
                f"({len(results)} Läufe)",
                fontsize=13,
            )
            axes = axes.flatten()

            for ax, (col, label, unit_note, mult) in zip(axes, metrics):
                if col not in df.columns:
                    ax.set_visible(False)
                    continue

                vals = df[col] * mult
                try:
                    ax.hist(vals, bins=60, alpha=0.75, color="steelblue", edgecolor="None")
                except ValueError:
                    ax.hist(vals, bins="auto", alpha=0.75, color="steelblue", edgecolor="None")
                ax.axvline(vals.median(), color="red",    linewidth=1.5, label=f"Median {vals.median():.2f}")
                ax.axvline(vals.quantile(0.05), color="orange", linewidth=1.0, linestyle="--", label="5. Pzl.")
                ax.axvline(vals.quantile(0.95), color="green",  linewidth=1.0, linestyle="--", label="95. Pzl.")
                ax.set_title(f"{label}{' (' + unit_note + ')' if unit_note else ''}")
                ax.legend(fontsize=8)
                ax.grid(True, alpha=0.3)

            plt.tight_layout()
            fname = output_dir / f"v7_mc_{method}.png"
            fig.savefig(fname, dpi=120)
            plt.close(fig)
            print(f"[MC] Plot gespeichert: {fname}")

    # ------------------------------------------------------------------

    def export_csv(self, output_dir: Optional[Path] = None) -> None:
        """Exportiere alle Simulations-Ergebnisse als CSV."""
        if output_dir is None:
            output_dir = Path(__file__).parent / "trading_data"
        output_dir.mkdir(parents=True, exist_ok=True)

        for method, results in self._results.items():
            if not results:
                continue
            df   = pd.DataFrame(results)
            path = output_dir / f"v7_mc_{method}.csv"
            df.to_csv(path, index=False)
            print(f"[MC] CSV gespeichert: {path}")

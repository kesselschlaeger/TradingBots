#!/usr/bin/env python3
"""
vbt_wfo.py – Walk-Forward-Optimierung für Botti Trader v7.

Ansatz: Rolling Walk-Forward
  - In-Sample (IS):  500 Tage  (~2 Jahre)       konfig.: is_days
  - Out-of-Sample:   125 Tage  (~6 Monate)      konfig.: oos_days
  - Step:             63 Tage  (~3 Monate)       konfig.: step_days

Optimierungsmetrik: Sharpe Ratio auf IS-Daten (konfigurierbar).

Nutzung:
    from vbt_wfo import WalkForwardOptimizer

    wfo = WalkForwardOptimizer(
        data_dict=data,
        vix_series=vix,
        base_cfg=CONFIG,
        param_grid={
            "sma_short":     [10, 20, 30],
            "sma_long":      [30, 50],
            "rsi_buy_min":   [35, 40, 50],
            "adx_threshold": [15, 20, 25],
        },
    )
    windows = wfo.run()
    oos_eq  = wfo.combined_oos_equity()
    wfo.stability_report()
"""

from __future__ import annotations

import copy
import itertools
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from vbt_backtest import run_backtest as _default_backtest


# ---------------------------------------------------------------------------
# Datenklasse für ein WFO-Fenster
# ---------------------------------------------------------------------------

@dataclass
class WFOWindow:
    """Ergebnis eines einzelnen Walk-Forward-Fensters."""
    window_idx:   int
    is_start:     pd.Timestamp
    is_end:       pd.Timestamp
    oos_start:    pd.Timestamp
    oos_end:      pd.Timestamp
    best_params:  Dict[str, Any]
    is_sharpe:    float
    oos_metrics:  Dict[str, Any]
    oos_equity:   pd.Series = field(default_factory=pd.Series)


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def _slice_data(
    data_dict: Dict[str, pd.DataFrame],
    vix_series: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> Tuple[Dict[str, pd.DataFrame], pd.Series]:
    """Schneide data_dict und vix_series auf [start, end] zu."""
    sliced: Dict[str, pd.DataFrame] = {}
    start_day = pd.Timestamp(start).normalize()
    end_day = pd.Timestamp(end).normalize()
    for sym, df in data_dict.items():
        # Index normalisieren
        idx = df.index
        if idx.tz is not None:
            idx = idx.tz_localize(None)
        idx_days = pd.DatetimeIndex(idx).normalize()
        mask = (idx_days >= start_day) & (idx_days <= end_day)
        sub  = df.iloc[np.where(mask)[0]]
        if len(sub) >= 60:          # Mindestlänge für sinnvolle Berechnungen
            sliced[sym] = sub.copy()
    vix_index = pd.DatetimeIndex(vix_series.index)
    if vix_index.tz is not None:
        vix_index = vix_index.tz_localize(None)
    vix_days = vix_index.normalize()
    vix_sliced = vix_series.loc[(vix_days >= start_day) & (vix_days <= end_day)]
    return sliced, vix_sliced


def _param_combinations(param_grid: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    """Kartesisches Produkt aller Parameter-Kombinationen."""
    keys   = list(param_grid.keys())
    values = list(param_grid.values())
    combos = list(itertools.product(*values))
    return [dict(zip(keys, combo)) for combo in combos]


def _override_cfg(
    base_cfg: dict,
    overrides: Dict[str, Any],
    validation_func=None,
) -> dict:
    """Erstelle eine Kopie von base_cfg mit den überschriebenen Werten."""
    cfg = copy.deepcopy(base_cfg)
    cfg.update(overrides)
    # Eigene Validierung falls übergeben
    if validation_func is not None:
        return cfg if validation_func(cfg, overrides) else {}
    # Standard-Validierung: sma_short muss < sma_long sein
    if "sma_short" in overrides or "sma_long" in overrides:
        if cfg.get("sma_short", 20) >= cfg.get("sma_long", 50):
            return {}       # ungültige Kombination → leer
    return cfg


# ---------------------------------------------------------------------------
# Haupt-Klasse
# ---------------------------------------------------------------------------

class WalkForwardOptimizer:
    """
    Rolling Walk-Forward-Optimierung.

    Parameter
    ---------
    data_dict     : {symbol: OHLCV-DataFrame}  – kompletter Datensatz
    vix_series    : Tages-VIX als pd.Series
    base_cfg      : Basis-Konfiguration aus trader_v6.CONFIG
    param_grid    : Dict mit Parameternamen → Liste möglicher Werte
    is_days       : In-Sample-Länge in Handelstagen (Standard: 500)
    oos_days      : Out-of-Sample-Länge in Handelstagen (Standard: 125)
    step_days     : Schritt zwischen zwei Fenstern in Handelstagen (Standard: 63)
    metric        : Optimierungsmetrik ("sharpe", "calmar", "profit_factor")
    verbose       : Fortschrittsausgabe
    """

    DEFAULT_GRID: Dict[str, List[Any]] = {
        "sma_short":     [10, 20, 30],
        "sma_long":      [30, 50],
        "rsi_buy_min":   [35, 40, 50],
        "adx_threshold": [15, 20, 25],
    }

    def __init__(
        self,
        data_dict: Dict[str, pd.DataFrame],
        vix_series: pd.Series,
        base_cfg: dict,
        param_grid: Optional[Dict[str, List[Any]]] = None,
        is_days:   int = 500,
        oos_days:  int = 125,
        step_days: int = 63,
        metric:    str = "sharpe",
        verbose:   bool = True,
        backtest_func=None,
        validation_func=None,
    ):
        self.data_dict  = data_dict
        self.vix_series = vix_series
        self.base_cfg   = base_cfg
        self.param_grid = param_grid or self.DEFAULT_GRID
        self.is_days    = is_days
        self.oos_days   = oos_days
        self.step_days  = step_days
        self.metric     = metric
        self.verbose    = verbose
        self.backtest_func   = backtest_func or _default_backtest
        self.validation_func = validation_func
        self.windows:   List[WFOWindow] = []

        # Gemeinsamen Handels-Tagesindex ermitteln.
        # Bei Intraday-Daten darf WFO nicht über einzelne 5m-Bars laufen.
        all_idx = pd.DatetimeIndex([])
        for df in data_dict.values():
            idx = df.index
            if idx.tz is not None:
                idx = idx.tz_localize(None)
            all_idx = all_idx.union(pd.DatetimeIndex(idx).normalize().unique())
        self._dates = all_idx.sort_values().unique()

    # ------------------------------------------------------------------

    def estimated_window_count(self) -> int:
        """Schätze die Anzahl der WFO-Fenster auf Basis des Tagesindex."""
        n = len(self._dates)
        total_len = self.is_days + self.oos_days
        if n < total_len:
            return 0
        return ((n - total_len) // self.step_days) + 1

    # ------------------------------------------------------------------

    def run(self) -> List[WFOWindow]:
        """
        Führe die Walk-Forward-Optimierung aus.

        Rückgabe
        --------
        Liste von WFOWindow-Objekten, eines je Fenster.
        """
        dates    = self._dates
        n        = len(dates)
        window_n = 0

        combos    = _param_combinations(self.param_grid)
        n_combos  = len(combos)
        total_len = self.is_days + self.oos_days
        est_windows = self.estimated_window_count()

        if n < total_len:
            raise ValueError(
                f"Zu wenige Daten ({n} Bars) für IS({self.is_days})+OOS({self.oos_days})."
            )

        if self.verbose:
            est_runs = est_windows * (n_combos + 1)
            print(
                f"[WFO] Planung: {n} Handelstage | {est_windows} Fenster | "
                f"{n_combos} Kombinationen/Fenster | ca. {est_runs} Backtest-Läufe"
            )

        cursor = 0
        while cursor + total_len <= n:
            is_start  = dates[cursor]
            is_end    = dates[cursor + self.is_days - 1]
            oos_start = dates[cursor + self.is_days]
            oos_end   = dates[min(cursor + total_len - 1, n - 1)]

            if self.verbose:
                print(
                    f"\n[WFO] Fenster {window_n + 1}/{est_windows} — "
                    f"IS: {is_start.date()} – {is_end.date()}  |  "
                    f"OOS: {oos_start.date()} – {oos_end.date()}  "
                    f"({n_combos} Kombinationen)"
                )

            # ---------- In-Sample Optimierung ----------------------------------
            is_data, is_vix = _slice_data(self.data_dict, self.vix_series, is_start, is_end)

            best_metric = -np.inf
            best_params: Dict[str, Any] = {}
            best_is_sharpe = -np.inf

            for i, overrides in enumerate(combos):
                cfg = _override_cfg(self.base_cfg, overrides, self.validation_func)
                if not cfg:
                    continue
                try:
                    _, rep = self.backtest_func(is_data, is_vix, cfg)
                except Exception as e:
                    if self.verbose:
                        print(f"    Combo {i+1}/{n_combos} FEHLER: {e}")
                    continue

                val = rep.get(self.metric, rep.get("sharpe", -np.inf))
                if val is None:
                    val = -np.inf
                if self.verbose and (i + 1) % max(1, n_combos // 5) == 0:
                    print(f"    {i+1}/{n_combos} combo, bestes IS {self.metric}={best_metric:.3f}")
                if float(val) > best_metric:
                    best_metric   = float(val)
                    best_params   = overrides
                    best_is_sharpe = rep.get("sharpe", -np.inf) or -np.inf

            if not best_params:
                if self.verbose:
                    print("    ⚠ Keine gültige Parameterkombination gefunden.")
                cursor += self.step_days
                window_n += 1
                continue

            if self.verbose:
                print(f"    Beste Params (IS {self.metric}={best_metric:.3f}): {best_params}")

            # ---------- Out-of-Sample Validierung ------------------------------
            oos_data, oos_vix = _slice_data(self.data_dict, self.vix_series, oos_start, oos_end)
            oos_cfg = _override_cfg(self.base_cfg, best_params, self.validation_func)

            try:
                _, oos_rep = self.backtest_func(oos_data, oos_vix, oos_cfg)
            except Exception as e:
                if self.verbose:
                    print(f"    OOS-Backtest FEHLER: {e}")
                cursor += self.step_days
                window_n += 1
                continue

            oos_eq = oos_rep.pop("equity_curve", pd.Series())
            oos_rep.pop("trades", None)

            win = WFOWindow(
                window_idx  = window_n,
                is_start    = is_start,
                is_end      = is_end,
                oos_start   = oos_start,
                oos_end     = oos_end,
                best_params = best_params,
                is_sharpe   = best_is_sharpe,
                oos_metrics = oos_rep,
                oos_equity  = oos_eq,
            )
            self.windows.append(win)

            if self.verbose:
                print(
                    f"    OOS: CAGR={oos_rep.get('cagr_pct', 'n/a')}%  "
                    f"Sharpe={oos_rep.get('sharpe', 'n/a')}  "
                    f"MaxDD={oos_rep.get('max_drawdown_pct', 'n/a')}%"
                )

            cursor += self.step_days
            window_n += 1

        print(f"\n[WFO] Abgeschlossen: {len(self.windows)} Fenster.")
        return self.windows

    # ------------------------------------------------------------------

    def combined_oos_equity(self) -> pd.Series:
        """
        Verkettet die OOS-Equity-Kurven aller Fenster zu einer gemeinsamen Kurve.
        Jede OOS-Periode wird am letzten Wert der vorherigen normiert.
        """
        if not self.windows:
            return pd.Series(dtype=float)

        pieces: List[pd.Series] = []
        running_end = 1.0     # normierter Wert

        for win in self.windows:
            eq = win.oos_equity
            if eq.empty:
                continue
            # normieren
            start_val = float(eq.iloc[0]) if float(eq.iloc[0]) != 0 else 1.0
            normed    = eq / start_val * running_end
            pieces.append(normed)
            running_end = float(normed.iloc[-1])

        if not pieces:
            return pd.Series(dtype=float)

        combined = pd.concat(pieces)
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        combined.name = "OOS_Equity"
        return combined

    # ------------------------------------------------------------------

    def stability_report(self) -> pd.DataFrame:
        """
        Ausgabe + DataFrame: wie oft wurde jeder Parameterwert in IS gewählt.

        Zeigt Parameterstabilität: stabile Parameter werden über viele Fenster
        mit demselben Wert gewählt.
        """
        if not self.windows:
            print("[WFO] Keine Fenster vorhanden.")
            return pd.DataFrame()

        records: List[dict] = []
        for win in self.windows:
            row = {"window": win.window_idx,
                   "oos_sharpe": win.oos_metrics.get("sharpe"),
                   "oos_cagr":   win.oos_metrics.get("cagr_pct")}
            row.update(win.best_params)
            records.append(row)

        df = pd.DataFrame(records)

        print("\n[WFO] Parameter-Stabilität:")
        sep = "─" * 60
        print(sep)
        for col in [c for c in df.columns if c not in ("window", "oos_sharpe", "oos_cagr")]:
            counts = df[col].value_counts()
            print(f"  {col}:")
            for val, cnt in counts.items():
                bar = "█" * cnt
                print(f"    {str(val):>6}  {bar} ({cnt}×)")
        print(sep)
        print(f"\n  OOS Sharpe  – Median: {df['oos_sharpe'].median():.3f}  "
              f"Min: {df['oos_sharpe'].min():.3f}  Max: {df['oos_sharpe'].max():.3f}")
        print(f"  OOS CAGR %  – Median: {df['oos_cagr'].median():.2f}%  "
              f"Min: {df['oos_cagr'].min():.2f}%  Max: {df['oos_cagr'].max():.2f}%")

        return df

    # ------------------------------------------------------------------

    def print_summary(self) -> None:
        """Drucke eine kompakte Tabelle aller Fenster."""
        if not self.windows:
            print("[WFO] Keine Ergebnisse vorhanden.")
            return

        hdr = (f"{'#':>3}  {'IS Start':>12}  {'IS End':>12}  "
               f"{'OOS Start':>12}  {'OOS End':>12}  "
               f"{'IS Sharpe':>10}  {'OOS Sharpe':>10}  "
               f"{'OOS CAGR%':>10}  {'OOS MaxDD%':>10}")
        print("\n[WFO] Fensterzusammenfassung:")
        print("─" * len(hdr))
        print(hdr)
        print("─" * len(hdr))
        for w in self.windows:
            print(
                f"{w.window_idx:>3}  "
                f"{str(w.is_start.date()):>12}  {str(w.is_end.date()):>12}  "
                f"{str(w.oos_start.date()):>12}  {str(w.oos_end.date()):>12}  "
                f"{w.is_sharpe:>10.3f}  "
                f"{w.oos_metrics.get('sharpe', 0) or 0:>10.3f}  "
                f"{w.oos_metrics.get('cagr_pct', 0) or 0:>10.2f}  "
                f"{w.oos_metrics.get('max_drawdown_pct', 0) or 0:>10.2f}  "
            )
        print("─" * len(hdr))

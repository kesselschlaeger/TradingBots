#!/usr/bin/env python3
"""
orb_wfo.py – Walk-Forward-Optimierung für ORB Bot.

Lokale Kopie von Trading_Bot/vbt_wfo.py, angepasst für ORB-Unabhängigkeit:
  - Kein Import aus vbt_backtest (backtest_func ist Pflichtparameter)
  - Output-Prefix "orb_" statt "v7_"

Ansatz: Rolling Walk-Forward
  - In-Sample (IS):  konfigurierbar (Standard: 120 Tage)
  - Out-of-Sample:   konfigurierbar (Standard: 30 Tage)
  - Step:            konfigurierbar (Standard: 20 Tage)

Nutzung:
    from orb_wfo import WalkForwardOptimizer
    from orb_backtest import run_orb_backtest

    wfo = WalkForwardOptimizer(
        data_dict=data,
        vix_series=vix,
        base_cfg=ORB_CONFIG,
        param_grid={...},
        backtest_func=run_orb_backtest,
    )
    windows = wfo.run()
    wfo.print_summary()
"""

from __future__ import annotations

import copy
import itertools
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from orb_strategy import compute_indicators


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
    warmup_calendar_days: int = 40,
    vix3m_series: Optional[pd.Series] = None,
) -> Tuple[Dict[str, pd.DataFrame], pd.Series, Optional[pd.Series]]:
    """Schneide data_dict, vix_series und vix3m_series auf [start, end] zu.

    Fix #19: Volume_MA / ATR werden für den Slice neu berechnet, um
    Pre-Sample-Kontamination durch die einmalige Voll-Historien-Berechnung zu
    vermeiden.  Ein Warmup-Puffer von `warmup_calendar_days` Kalendertagen
    (Standard 40 ≈ 28 Handelstage) vor `start` stellt sicher, dass der
    Rolling(20)-Indikator am IS-Start vollständig befüllt ist.
    """
    sliced: Dict[str, pd.DataFrame] = {}
    start_day   = pd.Timestamp(start).normalize()
    end_day     = pd.Timestamp(end).normalize()
    warmup_start = start_day - pd.Timedelta(days=warmup_calendar_days)

    for sym, df in data_dict.items():
        idx = df.index
        if idx.tz is not None:
            idx_plain = idx.tz_localize(None)
        else:
            idx_plain = idx
        idx_days = pd.DatetimeIndex(idx_plain).normalize()

        # Warmup-Puffer + eigentliches Slice laden, dann Indikatoren neu berechnen
        warmup_mask = (idx_days >= warmup_start) & (idx_days <= end_day)
        df_warmup   = df.iloc[np.where(warmup_mask)[0]].copy()
        if len(df_warmup) < 30:
            continue

        # Indikatoren frisch auf dem Warmup-Segment berechnen
        df_warmup = compute_indicators(df_warmup)

        # Warmup-Puffer wegschneiden → nur [start, end] behalten
        w_idx = df_warmup.index
        if w_idx.tz is not None:
            w_plain = w_idx.tz_localize(None)
        else:
            w_plain = w_idx
        w_days = pd.DatetimeIndex(w_plain).normalize()
        slice_mask = (w_days >= start_day) & (w_days <= end_day)
        sub = df_warmup.iloc[np.where(slice_mask)[0]]
        if len(sub) >= 60:
            sliced[sym] = sub
    vix_index = pd.DatetimeIndex(vix_series.index)
    if vix_index.tz is not None:
        vix_index = vix_index.tz_localize(None)
    vix_days   = vix_index.normalize()
    vix_sliced = vix_series.loc[(vix_days >= start_day) & (vix_days <= end_day)]

    # VIX3M slicen (Fix: WFO-Parität mit Full-Backtest)
    vix3m_sliced = None
    if vix3m_series is not None and not vix3m_series.empty:
        v3_index = pd.DatetimeIndex(vix3m_series.index)
        if v3_index.tz is not None:
            v3_index = v3_index.tz_localize(None)
        v3_days = v3_index.normalize()
        vix3m_sliced = vix3m_series.loc[(v3_days >= start_day) & (v3_days <= end_day)]

    return sliced, vix_sliced, vix3m_sliced


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
    if validation_func is not None:
        return cfg if validation_func(cfg, overrides) else {}
    return cfg


# ---------------------------------------------------------------------------
# Haupt-Klasse
# ---------------------------------------------------------------------------

class WalkForwardOptimizer:
    """
    Rolling Walk-Forward-Optimierung.

    Parameter
    ---------
    data_dict       : {symbol: OHLCV-DataFrame}  – kompletter Datensatz
    vix_series      : Tages-VIX als pd.Series
    base_cfg        : Basis-Konfiguration
    param_grid      : Dict mit Parameternamen → Liste möglicher Werte
    backtest_func   : Callable(data_dict, vix_series, cfg) → (_, report_dict)
                      **Pflichtparameter** – kein Default.
    is_days         : In-Sample-Länge in Handelstagen
    oos_days        : Out-of-Sample-Länge in Handelstagen
    step_days       : Schrittweite zwischen Fenstern in Handelstagen
    metric          : Optimierungsmetrik ("sharpe", "calmar", "profit_factor")
    verbose         : Fortschrittsausgabe
    validation_func : Optional – prüft Parameterkombinationen
    """

    def __init__(
        self,
        data_dict: Dict[str, pd.DataFrame],
        vix_series: pd.Series,
        base_cfg: dict,
        param_grid: Dict[str, List[Any]],
        backtest_func: Callable,
        is_days:       int = 120,
        oos_days:      int = 30,
        step_days:     int = 20,
        metric:        str = "sharpe",
        verbose:       bool = True,
        validation_func: Optional[Callable] = None,
        min_trades_is: int = 20,
        vix3m_series: Optional[pd.Series] = None,
    ):
        if backtest_func is None:
            raise ValueError("backtest_func ist Pflichtparameter.")
        self.data_dict       = data_dict
        self.vix_series      = vix_series
        self.vix3m_series    = vix3m_series
        self.base_cfg        = base_cfg
        self.param_grid      = param_grid
        self.is_days         = is_days
        self.oos_days        = oos_days
        self.step_days       = step_days
        self.metric          = metric
        self.verbose         = verbose
        self.backtest_func   = backtest_func
        self.validation_func = validation_func
        self.min_trades_is   = min_trades_is
        self.windows: List[WFOWindow] = []

        # Gemeinsamen Handels-Tagesindex ermitteln.
        all_idx = pd.DatetimeIndex([])
        for df in data_dict.values():
            idx = df.index
            if idx.tz is not None:
                idx = idx.tz_localize(None)
            all_idx = all_idx.union(pd.DatetimeIndex(idx).normalize().unique())
        self._dates = all_idx.sort_values().unique()

    # ------------------------------------------------------------------

    def estimated_window_count(self) -> int:
        """Schätze die Anzahl der WFO-Fenster."""
        n = len(self._dates)
        total_len = self.is_days + self.oos_days
        if n < total_len:
            return 0
        return ((n - total_len) // self.step_days) + 1

    # ------------------------------------------------------------------

    def run(self) -> List[WFOWindow]:
        """Führe die Walk-Forward-Optimierung aus."""
        dates    = self._dates
        n        = len(dates)
        window_n = 0

        combos    = _param_combinations(self.param_grid)
        n_combos  = len(combos)
        total_len = self.is_days + self.oos_days
        est_windows = self.estimated_window_count()

        if n < total_len:
            raise ValueError(
                f"Zu wenige Daten ({n} Tage) für IS({self.is_days})+OOS({self.oos_days})."
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

            # ---- In-Sample Optimierung ------------------------------------
            is_data, is_vix, is_vix3m = _slice_data(
                self.data_dict, self.vix_series, is_start, is_end,
                vix3m_series=self.vix3m_series,
            )

            best_metric    = -np.inf
            best_params: Dict[str, Any] = {}
            best_is_sharpe = -np.inf

            for i, overrides in enumerate(combos):
                cfg = _override_cfg(self.base_cfg, overrides, self.validation_func)
                if not cfg:
                    continue
                try:
                    _, rep = self.backtest_func(is_data, is_vix, cfg, vix3m_series=is_vix3m)
                except Exception as e:
                    if self.verbose:
                        print(f"    Combo {i+1}/{n_combos} FEHLER: {e}")
                    continue

                # Minimum-Trades-Gate: Kombos mit zu wenig IS-Trades erzeugen
                # Sharpe-Artefakte und liefern im OOS reine Zufallsergebnisse.
                n_trades_is = int(rep.get("total_trades", rep.get("n_trades", 0)))
                if n_trades_is < self.min_trades_is:
                    if self.verbose and (i + 1) % max(1, n_combos // 5) == 0:
                        print(f"    {i+1}/{n_combos} combo SKIP (n_trades={n_trades_is} < {self.min_trades_is})")
                    continue

                val = rep.get(self.metric, rep.get("sharpe", -np.inf))
                if val is None:
                    val = -np.inf
                if self.verbose and (i + 1) % max(1, n_combos // 5) == 0:
                    print(f"    {i+1}/{n_combos} combo, bestes IS {self.metric}={best_metric:.3f}")
                if float(val) > best_metric:
                    best_metric    = float(val)
                    best_params    = overrides
                    best_is_sharpe = rep.get("sharpe", -np.inf) or -np.inf

            if not best_params:
                if self.verbose:
                    print("    ⚠ Keine gültige Parameterkombination gefunden.")
                cursor += self.step_days
                window_n += 1
                continue

            if self.verbose:
                print(f"    Beste Params (IS {self.metric}={best_metric:.3f}): {best_params}")

            # ---- Out-of-Sample Validierung --------------------------------
            oos_data, oos_vix, oos_vix3m = _slice_data(
                self.data_dict, self.vix_series, oos_start, oos_end,
                vix3m_series=self.vix3m_series,
            )
            oos_cfg = _override_cfg(self.base_cfg, best_params, self.validation_func)

            try:
                _, oos_rep = self.backtest_func(oos_data, oos_vix, oos_cfg, vix3m_series=oos_vix3m)
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
        """Verkettet die OOS-Equity-Kurven aller Fenster."""
        if not self.windows:
            return pd.Series(dtype=float)

        pieces: List[pd.Series] = []
        running_end = 1.0

        for win in self.windows:
            eq = win.oos_equity
            if eq.empty:
                continue
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
        """Parameterstabilität über alle Fenster."""
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
        """Kompakte Tabelle aller Fenster."""
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

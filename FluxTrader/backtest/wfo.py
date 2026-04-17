"""Walk-Forward-Optimierung – framework-weit, strategie-agnostisch.

Rolling Walk-Forward:
  - In-Sample (IS):  konfigurierbar (Standard: 120 Tage)
  - Out-of-Sample:   konfigurierbar (Standard: 30 Tage)
  - Step:            konfigurierbar (Standard: 20 Tage)

Die Engine bleibt austauschbar: ``backtest_func`` ist Pflichtparameter. Für
den FluxTrader-Standardpfad gibt es ``run_flux_backtest`` als Top-Level
Adapter auf :class:`BarByBarEngine` + :func:`build_tearsheet` – picklebar,
damit auch Prozess-Pool-Worker ihn importieren können.

Kein Import aus ``vbt_*``. Kein vectorbt. Kein yfinance. Keine Broker-SDKs.
"""
from __future__ import annotations

import asyncio
import itertools
import math
import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd

from core.config import AppConfig
from core.logging import get_logger

log = get_logger(__name__)


# ────────────────────────────── Datenklassen ─────────────────────────────

@dataclass
class WFOWindow:
    """Ergebnis eines einzelnen Walk-Forward-Fensters."""
    window_idx: int
    is_start: pd.Timestamp
    is_end: pd.Timestamp
    oos_start: pd.Timestamp
    oos_end: pd.Timestamp
    best_params: dict[str, Any]
    is_metric: float
    oos_metrics: dict[str, Any]
    oos_equity: pd.Series = field(default_factory=pd.Series)


# ────────────────────────────── Hilfsfunktionen ──────────────────────────

def _normalize_days(idx: pd.Index) -> pd.DatetimeIndex:
    """Normalisiere Index auf tz-naive Tagesgranularität."""
    dti = pd.DatetimeIndex(idx)
    if dti.tz is not None:
        dti = dti.tz_localize(None)
    return dti.normalize()


def _slice_data(
    data_dict: dict[str, pd.DataFrame],
    vix_series: Optional[pd.Series],
    start: pd.Timestamp,
    end: pd.Timestamp,
    vix3m_series: Optional[pd.Series] = None,
    spy_df: Optional[pd.DataFrame] = None,
    min_bars_per_symbol: int = 2,
) -> tuple[dict[str, pd.DataFrame], Optional[pd.Series],
           Optional[pd.Series], Optional[pd.DataFrame]]:
    """Schneide data_dict, vix_series, vix3m_series und spy_df auf [start, end].

    Indikatoren werden nicht vorab berechnet – :class:`BarByBarEngine` bzw.
    die injizierte ``backtest_func`` berechnet sie frisch pro Slice.
    """
    start_day = pd.Timestamp(start).normalize()
    end_day = pd.Timestamp(end).normalize()

    sliced: dict[str, pd.DataFrame] = {}
    for sym, df in data_dict.items():
        days = _normalize_days(df.index)
        mask = (days >= start_day) & (days <= end_day)
        sub = df.iloc[np.where(mask)[0]]
        if len(sub) >= min_bars_per_symbol:
            sliced[sym] = sub

    vix_sliced: Optional[pd.Series] = None
    if vix_series is not None and len(vix_series):
        vix_days = _normalize_days(vix_series.index)
        vix_sliced = vix_series.iloc[
            np.where((vix_days >= start_day) & (vix_days <= end_day))[0]
        ]

    vix3m_sliced: Optional[pd.Series] = None
    if vix3m_series is not None and len(vix3m_series):
        v3_days = _normalize_days(vix3m_series.index)
        vix3m_sliced = vix3m_series.iloc[
            np.where((v3_days >= start_day) & (v3_days <= end_day))[0]
        ]

    spy_sliced: Optional[pd.DataFrame] = None
    if spy_df is not None and not spy_df.empty:
        spy_days = _normalize_days(spy_df.index)
        spy_sliced = spy_df.iloc[
            np.where((spy_days >= start_day) & (spy_days <= end_day))[0]
        ]

    return sliced, vix_sliced, vix3m_sliced, spy_sliced


def _param_combinations(param_grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    keys = list(param_grid.keys())
    values = list(param_grid.values())
    return [dict(zip(keys, combo)) for combo in itertools.product(*values)]


def _override_cfg(
    base_cfg: AppConfig,
    overrides: dict[str, Any],
    validation_func: Optional[Callable[[dict, dict], bool]] = None,
) -> Optional[AppConfig]:
    """Erstelle eine neue :class:`AppConfig` mit überschriebenen
    ``strategy.params``-Werten (Pydantic v2 ``model_copy``).

    Gibt ``None`` zurück, wenn ``validation_func`` die Kombination ablehnt.
    """
    new_params = {**base_cfg.strategy.params, **overrides}
    new_strategy = base_cfg.strategy.model_copy(update={"params": new_params})
    new_cfg = base_cfg.model_copy(update={"strategy": new_strategy})
    if validation_func is not None and not validation_func(new_params, overrides):
        return None
    return new_cfg


def _format_duration(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m:02d}m {s:02d}s"
    if m > 0:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _build_progress_milestones(
    total_runs: int,
    first_report_after: int = 10,
    max_reports: int = 10,
) -> list[int]:
    if total_runs < first_report_after:
        return []
    slots = total_runs - first_report_after + 1
    count = min(max_reports, slots)
    if count == 1:
        return [first_report_after]
    milestones: list[int] = []
    span = total_runs - first_report_after
    for i in range(count):
        v = first_report_after + math.floor(i * span / (count - 1))
        if milestones and v <= milestones[-1]:
            v = milestones[-1] + 1
        milestones.append(min(v, total_runs))
    return milestones


# ────────────────────────── Prozess-Pool-Worker ──────────────────────────

_worker_state: dict[str, Any] = {}


def _init_worker(
    backtest_func: Callable,
    data: dict[str, pd.DataFrame],
    vix: Optional[pd.Series],
    vix3m: Optional[pd.Series],
    spy: Optional[pd.DataFrame],
    base_cfg: AppConfig,
    metric: str,
    min_trades_is: int,
) -> None:
    """Shared state pro Worker-Prozess einmalig setzen."""
    _worker_state.update({
        "func": backtest_func,
        "data": data,
        "vix": vix,
        "vix3m": vix3m,
        "spy": spy,
        "base_cfg": base_cfg,
        "metric": metric,
        "min_trades": min_trades_is,
    })


def _eval_combo(overrides: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Evaluiere eine Parameterkombination (Worker-Einstiegspunkt)."""
    s = _worker_state
    cfg = _override_cfg(s["base_cfg"], overrides)
    if cfg is None:
        return None
    try:
        rep = s["func"](
            s["data"], s["vix"], cfg,
            vix3m_series=s["vix3m"], spy_df=s["spy"], silent=True,
        )
    except Exception:
        return None
    n_trades = int(rep.get("total_trades", rep.get("num_trades", 0)))
    if n_trades < s["min_trades"]:
        return None
    val = rep.get(s["metric"], rep.get("sharpe", -np.inf))
    if val is None:
        val = -np.inf
    sharpe = rep.get("sharpe", -np.inf) or -np.inf
    return {"overrides": overrides, "metric_val": float(val), "sharpe": float(sharpe)}


# ────────────────────────────── Haupt-Klasse ─────────────────────────────

class WalkForwardOptimizer:
    """Rolling Walk-Forward-Optimierung.

    Parameter
    ---------
    data_dict       : ``{symbol: OHLCV-DataFrame}``
    vix_series      : Tages-VIX als :class:`pd.Series` (optional)
    base_cfg        : :class:`AppConfig` – Basis-Konfiguration
    param_grid      : Dict mit Parametername → Liste möglicher Werte
                      (überschreibt ``strategy.params`` je Kombination)
    backtest_func   : Callable ``(data, vix, cfg, vix3m_series, spy_df, silent)
                      -> dict`` – **Pflichtparameter**. Muss picklebar sein,
                      wenn ``n_workers > 1``. Der Rückgabe-Dict enthält
                      mindestens ``sharpe`` und ``num_trades``/``total_trades``,
                      optional ``cagr_pct``, ``max_drawdown_pct``,
                      ``equity_curve``, ``trades``.
    is_days         : In-Sample-Länge in Handelstagen
    oos_days        : Out-of-Sample-Länge in Handelstagen
    step_days       : Schrittweite zwischen Fenstern in Handelstagen
    metric          : Optimierungsmetrik (z. B. ``sharpe``, ``cagr_pct``,
                      ``profit_factor``)
    validation_func : Optionaler Validator ``(merged_params, overrides) -> bool``
    min_trades_is   : Minimale Trade-Anzahl, damit eine Kombination in der
                      IS-Bestauswahl überhaupt zählt
    vix3m_series    : Optional – VIX3M für Term-Structure-Regime
    spy_df          : Optional – SPY-DataFrame für Trendfilter
    n_workers       : 0 = auto, 1 = sequentiell, >1 = ProcessPoolExecutor
    """

    def __init__(
        self,
        data_dict: dict[str, pd.DataFrame],
        vix_series: Optional[pd.Series],
        base_cfg: AppConfig,
        param_grid: dict[str, list[Any]],
        backtest_func: Callable,
        is_days: int = 120,
        oos_days: int = 30,
        step_days: int = 20,
        metric: str = "sharpe",
        validation_func: Optional[Callable] = None,
        min_trades_is: int = 20,
        vix3m_series: Optional[pd.Series] = None,
        spy_df: Optional[pd.DataFrame] = None,
        n_workers: int = 0,
        min_bars_per_symbol: int = 2,
    ):
        if backtest_func is None:
            raise ValueError("backtest_func ist Pflichtparameter.")
        self.data_dict = data_dict
        self.vix_series = vix_series
        self.vix3m_series = vix3m_series
        self.spy_df = spy_df
        self.base_cfg = base_cfg
        self.param_grid = param_grid
        self.backtest_func = backtest_func
        self.is_days = is_days
        self.oos_days = oos_days
        self.step_days = step_days
        self.metric = metric
        self.validation_func = validation_func
        self.min_trades_is = min_trades_is
        self.min_bars_per_symbol = min_bars_per_symbol
        self.windows: list[WFOWindow] = []

        if n_workers <= 0:
            self._n_workers = max(1, (os.cpu_count() or 4) - 1)
        else:
            self._n_workers = n_workers

        all_days = pd.DatetimeIndex([])
        for df in data_dict.values():
            days = _normalize_days(df.index).unique()
            all_days = all_days.union(days)
        self._dates = all_days.sort_values().unique()

    def estimated_window_count(self) -> int:
        n = len(self._dates)
        total = self.is_days + self.oos_days
        if n < total:
            return 0
        return ((n - total) // self.step_days) + 1

    # ──────────────────────────────────────────────────────────────────

    def run(self) -> list[WFOWindow]:
        dates = self._dates
        n = len(dates)

        combos = _param_combinations(self.param_grid)
        valid_combos = [
            c for c in combos
            if _override_cfg(self.base_cfg, c, self.validation_func) is not None
        ]
        n_combos = len(valid_combos)
        total_len = self.is_days + self.oos_days
        est_windows = self.estimated_window_count()

        if n < total_len:
            raise ValueError(
                f"Zu wenige Daten ({n} Tage) für IS({self.is_days})"
                f"+OOS({self.oos_days})."
            )
        if n_combos == 0:
            raise ValueError("Kein Parameter-Grid bzw. keine valide Kombination.")

        est_runs = est_windows * (n_combos + 1)
        milestones = _build_progress_milestones(est_runs)
        reports_done = 0
        completed = 0
        started = perf_counter()

        def tick(delta: int = 1) -> None:
            nonlocal completed, reports_done
            completed += delta
            while (reports_done < len(milestones)
                   and completed >= milestones[reports_done]):
                elapsed = perf_counter() - started
                avg = elapsed / max(completed, 1)
                remaining = max(est_runs - completed, 0)
                reports_done += 1
                log.info(
                    "wfo.progress",
                    step=reports_done,
                    total_steps=len(milestones),
                    completed=completed,
                    planned=est_runs,
                    avg_run_s=round(avg, 2),
                    elapsed=_format_duration(elapsed),
                    eta=_format_duration(remaining * avg),
                )

        mode = (f"parallel({self._n_workers})"
                if self._n_workers > 1 else "sequential")
        log.info(
            "wfo.plan",
            trading_days=n,
            windows=est_windows,
            combos_per_window=n_combos,
            total_runs=est_runs,
            mode=mode,
        )

        cursor = 0
        window_n = 0
        while cursor + total_len <= n:
            is_start = dates[cursor]
            is_end = dates[cursor + self.is_days - 1]
            oos_start = dates[cursor + self.is_days]
            oos_end = dates[min(cursor + total_len - 1, n - 1)]

            log.info(
                "wfo.window.start",
                window=window_n + 1,
                total=est_windows,
                is_start=str(is_start.date()),
                is_end=str(is_end.date()),
                oos_start=str(oos_start.date()),
                oos_end=str(oos_end.date()),
                combos=n_combos,
            )

            is_data, is_vix, is_vix3m, is_spy = _slice_data(
                self.data_dict, self.vix_series, is_start, is_end,
                self.vix3m_series, self.spy_df,
                min_bars_per_symbol=self.min_bars_per_symbol,
            )

            best_metric = -np.inf
            best_params: dict[str, Any] = {}
            best_is_sharpe = -np.inf

            use_parallel = self._n_workers > 1 and n_combos >= 4
            is_results: Optional[list] = None

            if use_parallel:
                try:
                    with ProcessPoolExecutor(
                        max_workers=min(self._n_workers, n_combos),
                        initializer=_init_worker,
                        initargs=(self.backtest_func, is_data, is_vix,
                                  is_vix3m, is_spy, self.base_cfg,
                                  self.metric, self.min_trades_is),
                    ) as pool:
                        is_results = list(pool.map(_eval_combo, valid_combos))
                except Exception as e:
                    log.warning("wfo.parallel_fallback", error=str(e))
                    is_results = None

            if is_results is not None:
                tick(n_combos)
                for r in is_results:
                    if r is None:
                        continue
                    if r["metric_val"] > best_metric:
                        best_metric = r["metric_val"]
                        best_params = r["overrides"]
                        best_is_sharpe = r["sharpe"]
            else:
                for i, overrides in enumerate(valid_combos):
                    cfg_i = _override_cfg(self.base_cfg, overrides)
                    if cfg_i is None:
                        tick()
                        continue
                    try:
                        rep = self.backtest_func(
                            is_data, is_vix, cfg_i,
                            vix3m_series=is_vix3m, spy_df=is_spy, silent=True,
                        )
                    except Exception as e:
                        tick()
                        log.debug("wfo.combo_error",
                                  window=window_n + 1, combo=i + 1,
                                  error=str(e))
                        continue
                    tick()
                    n_trades = int(rep.get("total_trades",
                                            rep.get("num_trades", 0)))
                    if n_trades < self.min_trades_is:
                        continue
                    val = rep.get(self.metric, rep.get("sharpe", -np.inf))
                    if val is None:
                        val = -np.inf
                    if float(val) > best_metric:
                        best_metric = float(val)
                        best_params = overrides
                        best_is_sharpe = rep.get("sharpe", -np.inf) or -np.inf

            if not best_params:
                log.warning("wfo.window.no_best", window=window_n + 1)
                cursor += self.step_days
                window_n += 1
                continue

            log.info("wfo.window.best",
                     window=window_n + 1,
                     metric=self.metric,
                     value=round(float(best_metric), 4),
                     params=best_params)

            # ── OOS ────────────────────────────────────────────────
            oos_data, oos_vix, oos_vix3m, oos_spy = _slice_data(
                self.data_dict, self.vix_series, oos_start, oos_end,
                self.vix3m_series, self.spy_df,
                min_bars_per_symbol=self.min_bars_per_symbol,
            )
            oos_cfg = _override_cfg(self.base_cfg, best_params,
                                    self.validation_func)
            if oos_cfg is None:
                tick()
                log.warning("wfo.window.invalid_oos_cfg",
                            window=window_n + 1)
                cursor += self.step_days
                window_n += 1
                continue

            try:
                oos_rep = self.backtest_func(
                    oos_data, oos_vix, oos_cfg,
                    vix3m_series=oos_vix3m, spy_df=oos_spy, silent=True,
                )
                tick()
            except Exception as e:
                tick()
                log.warning("wfo.oos_error",
                            window=window_n + 1, error=str(e))
                cursor += self.step_days
                window_n += 1
                continue

            oos_eq = oos_rep.pop("equity_curve", pd.Series(dtype=float))
            oos_rep.pop("trades", None)

            self.windows.append(WFOWindow(
                window_idx=window_n,
                is_start=is_start,
                is_end=is_end,
                oos_start=oos_start,
                oos_end=oos_end,
                best_params=best_params,
                is_metric=float(best_metric),
                oos_metrics=dict(oos_rep),
                oos_equity=oos_eq if isinstance(oos_eq, pd.Series)
                           else pd.Series(dtype=float),
            ))

            log.info(
                "wfo.window.oos",
                window=window_n + 1,
                sharpe=oos_rep.get("sharpe"),
                cagr_pct=oos_rep.get("cagr_pct"),
                max_drawdown_pct=oos_rep.get("max_drawdown_pct"),
            )

            cursor += self.step_days
            window_n += 1

        log.info("wfo.done", windows_completed=len(self.windows))
        return self.windows

    # ──────────────────────────────────────────────────────────────────

    def combined_oos_equity(self) -> pd.Series:
        """Verkettet die OOS-Equity-Kurven aller Fenster (normalisiert)."""
        if not self.windows:
            return pd.Series(dtype=float)
        pieces: list[pd.Series] = []
        running = 1.0
        for w in self.windows:
            eq = w.oos_equity
            if eq is None or eq.empty:
                continue
            first = float(eq.iloc[0])
            if first == 0.0:
                first = 1.0
            normed = eq / first * running
            pieces.append(normed)
            running = float(normed.iloc[-1])
        if not pieces:
            return pd.Series(dtype=float)
        combined = pd.concat(pieces)
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        combined.name = "OOS_Equity"
        return combined

    # ──────────────────────────────────────────────────────────────────

    def stability_report(self) -> pd.DataFrame:
        """Parameter-Stabilität über alle Fenster als DataFrame + Log."""
        if not self.windows:
            log.info("wfo.stability.empty")
            return pd.DataFrame()

        rows: list[dict] = []
        for w in self.windows:
            row = {
                "window": w.window_idx,
                "oos_sharpe": w.oos_metrics.get("sharpe"),
                "oos_cagr_pct": w.oos_metrics.get("cagr_pct"),
            }
            row.update(w.best_params)
            rows.append(row)
        df = pd.DataFrame(rows)

        param_cols = [c for c in df.columns
                      if c not in ("window", "oos_sharpe", "oos_cagr_pct")]
        stability = {
            col: {str(k): int(v) for k, v in df[col].value_counts().items()}
            for col in param_cols
        }
        oos_sharpe = df["oos_sharpe"].dropna()
        oos_cagr = df["oos_cagr_pct"].dropna()
        log.info(
            "wfo.stability",
            parameters=stability,
            oos_sharpe_median=float(oos_sharpe.median()) if len(oos_sharpe) else None,
            oos_sharpe_min=float(oos_sharpe.min()) if len(oos_sharpe) else None,
            oos_sharpe_max=float(oos_sharpe.max()) if len(oos_sharpe) else None,
            oos_cagr_median=float(oos_cagr.median()) if len(oos_cagr) else None,
        )
        return df

    # ──────────────────────────────────────────────────────────────────

    def summary_frame(self) -> pd.DataFrame:
        """Kompakte Tabelle aller Fenster als DataFrame."""
        if not self.windows:
            return pd.DataFrame()
        rows = []
        for w in self.windows:
            rows.append({
                "window": w.window_idx,
                "is_start": str(w.is_start.date()),
                "is_end": str(w.is_end.date()),
                "oos_start": str(w.oos_start.date()),
                "oos_end": str(w.oos_end.date()),
                "is_metric": w.is_metric,
                "oos_sharpe": w.oos_metrics.get("sharpe"),
                "oos_cagr_pct": w.oos_metrics.get("cagr_pct"),
                "oos_max_dd_pct": w.oos_metrics.get("max_drawdown_pct"),
            })
        return pd.DataFrame(rows)


# ──────────────────────── FluxTrader-Standardadapter ─────────────────────

def run_flux_backtest(
    data: dict[str, pd.DataFrame],
    vix: Optional[pd.Series],
    cfg: AppConfig,
    vix3m_series: Optional[pd.Series] = None,
    spy_df: Optional[pd.DataFrame] = None,
    silent: bool = True,
) -> dict[str, Any]:
    """Top-Level-Adapter auf :class:`BarByBarEngine` + :func:`build_tearsheet`.

    Muss auf Modul-Ebene liegen, damit ``ProcessPoolExecutor`` ihn picklen
    kann. Richtet für jeden Aufruf frischen ``MarketContextService``,
    ``PaperAdapter`` und Strategie-Instanz ein – kein shared State zwischen
    Combo-Evaluationen.

    Rückgabe: ``sharpe``, ``sortino``, ``cagr_pct``, ``max_drawdown_pct``,
    ``total_return_pct``, ``num_trades``/``total_trades``, ``win_rate``,
    ``profit_factor``, ``expectancy``, ``equity_curve`` (pd.Series),
    ``trades`` (list[Trade]).
    """
    # Strategien erst beim Aufruf importieren (Worker-Prozess-kompatibel).
    import strategy as _strategy_pkg  # noqa: F401 – triggert @register
    from backtest.engine import BacktestConfig, BarByBarEngine
    from backtest.report import build_tearsheet
    from core.context import MarketContextService, set_context_service
    from execution.paper_adapter import PaperAdapter
    from strategy.registry import StrategyRegistry

    if not data:
        return {
            "sharpe": 0.0, "sortino": 0.0, "cagr_pct": 0.0,
            "max_drawdown_pct": 0.0, "total_return_pct": 0.0,
            "num_trades": 0, "total_trades": 0, "win_rate": 0.0,
            "profit_factor": 0.0, "expectancy": 0.0,
            "equity_curve": pd.Series(dtype=float), "trades": [],
        }

    initial_capital = float(cfg.initial_capital)
    ctx = MarketContextService(initial_capital=initial_capital)
    ctx.update_account(equity=initial_capital, cash=initial_capital,
                       buying_power=initial_capital * 4)
    set_context_service(ctx)

    strat = StrategyRegistry.get(
        cfg.strategy.name, cfg.strategy.params, context=ctx,
    )
    paper = PaperAdapter(initial_cash=initial_capital,
                         slippage_pct=0.0002, commission_pct=0.00005)

    if spy_df is None and cfg.benchmark in data:
        spy_df = data[cfg.benchmark]

    eod_time = cfg.strategy.params.get("eod_close_time")
    if isinstance(eod_time, str):
        eod_time = None

    bt_cfg = BacktestConfig(
        initial_capital=initial_capital,
        risk_pct=cfg.strategy.risk_pct,
        eod_close_time=eod_time,
    )
    engine = BarByBarEngine(strat, paper, ctx, bt_cfg)

    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(engine.run(
            data=data, spy_df=spy_df,
            vix_series=vix, vix3m_series=vix3m_series,
        ))
    finally:
        loop.close()

    ts = build_tearsheet(result.equity_curve, result.trades,
                         result.initial_capital)
    return {
        "sharpe": ts.sharpe,
        "sortino": ts.sortino,
        "cagr_pct": ts.cagr_pct,
        "max_drawdown_pct": ts.max_drawdown_pct,
        "total_return_pct": ts.total_return_pct,
        "num_trades": ts.num_trades,
        "total_trades": ts.num_trades,
        "win_rate": ts.win_rate,
        "profit_factor": ts.profit_factor,
        "expectancy": ts.expectancy,
        "equity_curve": result.equity_curve,
        "trades": result.trades,
    }


__all__ = [
    "WFOWindow",
    "WalkForwardOptimizer",
    "run_flux_backtest",
]

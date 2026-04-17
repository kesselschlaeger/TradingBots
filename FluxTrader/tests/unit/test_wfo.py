"""Tests für backtest/wfo.py – Minimal-Daten + Mock-backtest_func."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backtest.wfo import (
    WalkForwardOptimizer,
    _override_cfg,
    _param_combinations,
    _slice_data,
)
from core.config import AppConfig, StrategyConfig


# ──────────────────────────── Fixtures / Helfer ──────────────────────────

def _make_daily_df(n: int = 30, start: str = "2024-01-02") -> pd.DataFrame:
    """Deterministische Daily-Bars mit UTC-Index."""
    rng = np.random.default_rng(7)
    idx = pd.date_range(start, periods=n, freq="1D", tz="UTC")
    close = 100.0 + np.cumsum(rng.normal(0.0, 0.5, n))
    return pd.DataFrame({
        "Open": close - 0.2,
        "High": close + 0.6,
        "Low": close - 0.6,
        "Close": close,
        "Volume": rng.integers(10_000, 100_000, n),
    }, index=idx)


def _base_cfg() -> AppConfig:
    return AppConfig(
        strategy=StrategyConfig(
            name="mock",
            symbols=["SPY"],
            params={"foo": 1.0, "bar": 0.5},
        ),
        initial_capital=10_000.0,
    )


# ──────────────────────────── Hilfsfunktionen ────────────────────────────

def test_param_combinations_cartesian_product():
    grid = {"a": [1, 2], "b": [10, 20, 30]}
    combos = _param_combinations(grid)
    assert len(combos) == 6
    assert {"a": 1, "b": 10} in combos
    assert {"a": 2, "b": 30} in combos


def test_override_cfg_uses_model_copy():
    base = _base_cfg()
    new = _override_cfg(base, {"foo": 9.9})
    # Original unverändert (model_copy → kein Shared State)
    assert base.strategy.params["foo"] == 1.0
    assert new is not None
    assert new.strategy.params["foo"] == 9.9
    assert new.strategy.params["bar"] == 0.5  # erhalten


def test_override_cfg_validation_rejects():
    base = _base_cfg()
    # Validator verwirft foo > 5
    out = _override_cfg(base, {"foo": 10.0},
                        validation_func=lambda p, o: p["foo"] <= 5.0)
    assert out is None


def test_slice_data_day_boundaries():
    df = _make_daily_df(n=20, start="2024-01-02")
    data = {"SPY": df}
    start = pd.Timestamp("2024-01-05")
    end = pd.Timestamp("2024-01-10")
    sliced, vix, vix3m, spy = _slice_data(
        data, vix_series=None, start=start, end=end,
        min_bars_per_symbol=3,
    )
    assert "SPY" in sliced
    sub = sliced["SPY"]
    first = sub.index.min().tz_convert(None).normalize()
    last = sub.index.max().tz_convert(None).normalize()
    assert first == start.normalize()
    assert last == end.normalize()
    assert vix is None and vix3m is None and spy is None


# ────────────────────────────── End-to-End ───────────────────────────────

def _mock_backtest(
    data, vix, cfg, *, vix3m_series=None, spy_df=None, silent=True,
):
    """Deterministischer Mock-Backtest.

    metric = foo - 0.1 * bar  (eindeutiges Maximum bei foo=3.0, bar=0.0)
    """
    foo = float(cfg.strategy.params.get("foo", 0.0))
    bar = float(cfg.strategy.params.get("bar", 0.0))
    score = foo - 0.1 * bar
    # Equity-Kurve aus den tatsächlichen Daten-Tagen konstruieren
    first_sym = next(iter(data.values()))
    eq = pd.Series(
        10_000.0 + np.linspace(0, score * 100, len(first_sym)),
        index=first_sym.index,
        name="equity",
    )
    return {
        "sharpe": score,
        "cagr_pct": score * 5.0,
        "max_drawdown_pct": 1.0,
        "num_trades": 30,
        "total_trades": 30,
        "equity_curve": eq,
        "trades": [],
    }


def test_wfo_two_window_run_picks_best_params():
    # 20 Tage daily → is=8 + oos=4 + step=8 = 2 Fenster (8+4=12, 12+8=20)
    data = {"SPY": _make_daily_df(n=20)}
    cfg = _base_cfg()
    grid = {"foo": [1.0, 2.0, 3.0], "bar": [0.0, 1.0]}

    wfo = WalkForwardOptimizer(
        data_dict=data,
        vix_series=None,
        base_cfg=cfg,
        param_grid=grid,
        backtest_func=_mock_backtest,
        is_days=8,
        oos_days=4,
        step_days=8,
        metric="sharpe",
        min_trades_is=1,
        n_workers=1,  # sequential – Mock ist nicht picklebar
    )

    assert wfo.estimated_window_count() == 2
    windows = wfo.run()

    assert len(windows) == 2
    for w in windows:
        # Mock-Maximum: foo=3.0, bar=0.0 -> sharpe=3.0
        assert w.best_params == {"foo": 3.0, "bar": 0.0}
        assert pytest.approx(w.is_metric, rel=1e-6) == 3.0
        assert w.oos_metrics["sharpe"] == pytest.approx(3.0, rel=1e-6)
        assert not w.oos_equity.empty


def test_wfo_raises_when_not_enough_data():
    data = {"SPY": _make_daily_df(n=5)}
    cfg = _base_cfg()
    wfo = WalkForwardOptimizer(
        data_dict=data,
        vix_series=None,
        base_cfg=cfg,
        param_grid={"foo": [1.0]},
        backtest_func=_mock_backtest,
        is_days=10,
        oos_days=5,
        step_days=2,
        n_workers=1,
    )
    with pytest.raises(ValueError, match="Zu wenige Daten"):
        wfo.run()


def test_wfo_combined_oos_equity_and_summary():
    data = {"SPY": _make_daily_df(n=20)}
    cfg = _base_cfg()
    wfo = WalkForwardOptimizer(
        data_dict=data,
        vix_series=None,
        base_cfg=cfg,
        param_grid={"foo": [2.0, 3.0], "bar": [0.0]},
        backtest_func=_mock_backtest,
        is_days=8,
        oos_days=4,
        step_days=8,
        min_trades_is=1,
        n_workers=1,
    )
    wfo.run()

    summary = wfo.summary_frame()
    assert len(summary) == 2
    assert {"is_start", "is_end", "oos_start", "oos_end",
            "is_metric", "oos_sharpe"}.issubset(summary.columns)

    stability = wfo.stability_report()
    assert not stability.empty
    assert "foo" in stability.columns and "bar" in stability.columns

    combined = wfo.combined_oos_equity()
    assert not combined.empty
    assert combined.name == "OOS_Equity"

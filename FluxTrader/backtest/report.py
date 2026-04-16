"""Tearsheet-Reporter: Sharpe, MaxDD, CAGR, Win-Rate, Expectancy."""
from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

from core.models import Trade


@dataclass
class Tearsheet:
    initial_capital: float
    final_equity: float
    total_return_pct: float
    cagr_pct: float
    max_drawdown_pct: float
    sharpe: float
    sortino: float
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    expectancy: float
    num_trades: int

    def as_dict(self) -> dict:
        return asdict(self)


def _annualization_factor(equity: pd.Series) -> float:
    if len(equity) < 2:
        return 252.0
    span_days = (equity.index[-1] - equity.index[0]).days
    if span_days <= 0:
        return 252.0
    bars_per_day = len(equity) / max(span_days, 1)
    return bars_per_day * 252.0


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    rolling_max = equity.cummax()
    dd = (equity - rolling_max) / rolling_max.replace(0, np.nan)
    return float(abs(dd.min())) if not dd.isna().all() else 0.0


def _sharpe(returns: pd.Series, ann: float, rf: float = 0.0) -> float:
    if returns.std(ddof=0) == 0 or len(returns) < 2:
        return 0.0
    excess = returns - rf / ann
    return float(np.sqrt(ann) * excess.mean() / returns.std(ddof=0))


def _sortino(returns: pd.Series, ann: float, rf: float = 0.0) -> float:
    downside = returns[returns < 0]
    if len(downside) < 2 or downside.std(ddof=0) == 0:
        return 0.0
    excess = returns - rf / ann
    return float(np.sqrt(ann) * excess.mean() / downside.std(ddof=0))


def _trade_pnls(trades: list[Trade]) -> list[float]:
    return [float(t.pnl) for t in trades if t.pnl != 0.0]


def build_tearsheet(equity: pd.Series, trades: list[Trade],
                    initial_capital: float) -> Tearsheet:
    if equity.empty:
        return Tearsheet(initial_capital, initial_capital, 0, 0, 0, 0, 0,
                         0, 0, 0, 0, 0, 0)

    final = float(equity.iloc[-1])
    total_ret = (final / initial_capital - 1.0) * 100.0

    span_days = max((equity.index[-1] - equity.index[0]).days, 1)
    cagr = ((final / initial_capital) ** (365.0 / span_days) - 1.0) * 100.0 \
        if final > 0 else -100.0

    returns = equity.pct_change().dropna()
    ann = _annualization_factor(equity)

    pnls = _trade_pnls(trades)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    win_rate = (len(wins) / len(pnls)) if pnls else 0.0
    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = float(np.mean(losses)) if losses else 0.0
    pf = (sum(wins) / abs(sum(losses))) if losses and sum(losses) < 0 else 0.0
    expectancy = (win_rate * avg_win + (1 - win_rate) * avg_loss) if pnls else 0.0

    return Tearsheet(
        initial_capital=float(initial_capital),
        final_equity=final,
        total_return_pct=total_ret,
        cagr_pct=cagr,
        max_drawdown_pct=_max_drawdown(equity) * 100.0,
        sharpe=_sharpe(returns, ann),
        sortino=_sortino(returns, ann),
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        profit_factor=pf,
        expectancy=expectancy,
        num_trades=len(pnls),
    )


def format_tearsheet(ts: Tearsheet) -> str:
    return (
        f"Initial:        ${ts.initial_capital:,.2f}\n"
        f"Final:          ${ts.final_equity:,.2f}\n"
        f"Total Return:   {ts.total_return_pct:+.2f}%\n"
        f"CAGR:           {ts.cagr_pct:+.2f}%\n"
        f"Max Drawdown:   {ts.max_drawdown_pct:.2f}%\n"
        f"Sharpe:         {ts.sharpe:.2f}\n"
        f"Sortino:        {ts.sortino:.2f}\n"
        f"Trades:         {ts.num_trades}\n"
        f"Win Rate:       {ts.win_rate:.1%}\n"
        f"Avg Win:        ${ts.avg_win:,.2f}\n"
        f"Avg Loss:       ${ts.avg_loss:,.2f}\n"
        f"Profit Factor:  {ts.profit_factor:.2f}\n"
        f"Expectancy:     ${ts.expectancy:,.2f}/Trade\n"
    )

"""Shared Fixtures: kein Netzwerk, kein Broker, deterministisch."""
from __future__ import annotations

from datetime import datetime, time, timezone

import numpy as np
import pandas as pd
import pytest
import pytz

from core.context import MarketContextService, reset_context_service, set_context_service
from core.models import Bar
from execution.paper_adapter import PaperAdapter

ET = pytz.timezone("America/New_York")


# ── MarketContext ──────────────────────────────────────────────────────

@pytest.fixture()
def context():
    """Frischer MarketContextService für jeden Test."""
    svc = MarketContextService(initial_capital=100_000.0, bar_buffer=200)
    svc.update_account(equity=100_000.0, cash=100_000.0, buying_power=400_000.0)
    set_context_service(svc)
    yield svc
    reset_context_service()


# ── PaperAdapter ──────────────────────────────────────────────────────

@pytest.fixture()
def paper():
    return PaperAdapter(initial_cash=100_000.0, slippage_pct=0.0,
                        commission_pct=0.0)


# ── Zeithelfer ────────────────────────────────────────────────────────

def _et_dt(year: int, month: int, day: int,
           hour: int, minute: int) -> datetime:
    """Erzeuge timezone-aware ET datetime."""
    naive = datetime(year, month, day, hour, minute)
    return ET.localize(naive).astimezone(timezone.utc)


@pytest.fixture()
def trading_day_ts():
    """Timestamps für einen normalen Handelstag (5-Min-Bars, 9:30–16:00 ET)."""
    # Mittwoch 2025-03-12
    day = datetime(2025, 3, 12)
    stamps = []
    for h in range(9, 16):
        start_m = 30 if h == 9 else 0
        end_m = 60
        for m in range(start_m, end_m, 5):
            dt = ET.localize(day.replace(hour=h, minute=m))
            stamps.append(dt.astimezone(timezone.utc))
    return stamps


# ── OHLCV-DataFrames ─────────────────────────────────────────────────

def make_ohlcv(n: int = 100, base: float = 100.0, seed: int = 42,
               start: datetime | None = None,
               freq: str = "5min") -> pd.DataFrame:
    """Deterministischer OHLCV-DataFrame."""
    rng = np.random.default_rng(seed)
    if start is None:
        start = _et_dt(2025, 3, 12, 9, 30)

    idx = pd.date_range(start, periods=n, freq=freq, tz="UTC")
    close = base + np.cumsum(rng.normal(0.0, 0.3, n))
    high = close + rng.uniform(0.05, 0.5, n)
    low = close - rng.uniform(0.05, 0.5, n)
    opn = close + rng.normal(0.0, 0.15, n)
    vol = rng.integers(10_000, 500_000, n)

    return pd.DataFrame({
        "Open": opn,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": vol,
    }, index=idx)


@pytest.fixture()
def ohlcv_5m():
    """100 5-Min-Bars ab 9:30 ET."""
    return make_ohlcv(100)


@pytest.fixture()
def ohlcv_daily():
    """200 Daily-Bars."""
    start = _et_dt(2024, 6, 1, 16, 0)
    return make_ohlcv(200, freq="1D", start=start, seed=99)


@pytest.fixture()
def spy_df():
    """50-bar SPY OHLCV (5m) mit EMA-freundlichem Trend."""
    df = make_ohlcv(50, base=520.0, seed=7)
    # Aufwärtstrend: sicherstellen dass Close > EMA(20)
    df["Close"] = df["Close"] + np.linspace(0, 5, len(df))
    df["High"] = df["Close"] + 0.5
    df["Low"] = df["Close"] - 0.5
    return df


# ── Bar-Erzeugung ────────────────────────────────────────────────────

def make_bar(symbol: str = "AAPL",
             ts: datetime | None = None,
             o: float = 100.0, h: float = 101.0,
             l: float = 99.0, c: float = 100.5,
             v: int = 100_000) -> Bar:
    if ts is None:
        ts = _et_dt(2025, 3, 12, 10, 5)
    return Bar(symbol=symbol, timestamp=ts, open=o, high=h, low=l,
               close=c, volume=v)

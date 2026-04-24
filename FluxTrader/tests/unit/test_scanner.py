"""Tests für den Premarket-Scanner – provider-agnostischer Pfad.

Regression zu Live-Incident 2026-04-24: Der Scanner war an Alpaca gebunden
und lieferte bei IBKR-Config stumm ``[]``. Nach dem Refactor bietet er einen
Bars-Fallback-Pfad für beliebige ``DataProvider``-Instanzen (IBKR/yfinance/
Paper).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pytest

from live.scanner import PremarketScanner, ScanResult, _extract_prev_close


class _FakeProvider:
    """Minimaler DataProvider-Stub: liefert vordefinierte DataFrames je
    (symbol, timeframe). ``calls`` sammelt alle get_bars-Aufrufe zwecks
    Assertions."""

    def __init__(self) -> None:
        self.bars: dict[tuple[str, str], pd.DataFrame] = {}
        self.calls: list[tuple[str, str]] = []

    def set(self, symbol: str, tf: str, df: pd.DataFrame) -> None:
        self.bars[(symbol.upper(), tf)] = df

    async def get_bars(self, symbol, start, end, timeframe):  # noqa: D401
        self.calls.append((symbol.upper(), timeframe))
        return self.bars.get(
            (symbol.upper(), timeframe),
            pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"]),
        )


def _daily(prev_close: float, latest_close: float) -> pd.DataFrame:
    idx = pd.DatetimeIndex(
        [datetime(2026, 4, 21, 21, 0, tzinfo=timezone.utc),
         datetime(2026, 4, 22, 21, 0, tzinfo=timezone.utc)],
        tz="UTC",
    )
    return pd.DataFrame(
        {
            "Open": [prev_close, latest_close],
            "High": [prev_close, latest_close],
            "Low": [prev_close, latest_close],
            "Close": [prev_close, latest_close],
            "Volume": [1_000_000, 500_000],
        },
        index=idx,
    )


def _intraday(price: float, volume: int) -> pd.DataFrame:
    idx = pd.DatetimeIndex(
        [datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)],
        tz="UTC",
    )
    return pd.DataFrame(
        {
            "Open": [price],
            "High": [price],
            "Low": [price],
            "Close": [price],
            "Volume": [volume],
        },
        index=idx,
    )


class TestScannerBarsFallback:
    @pytest.mark.asyncio
    async def test_scan_gap_up_via_bars(self):
        provider = _FakeProvider()
        # Prev close 100, Premarket 105 → +5 % Gap
        provider.set("NVDA", "1Day", _daily(prev_close=100.0, latest_close=104.0))
        provider.set("NVDA", "1Min", _intraday(price=105.0, volume=120_000))

        scanner = PremarketScanner(
            watchlist=["NVDA"],
            min_gap_pct=0.02,
            max_gap_pct=0.10,
            min_premarket_vol=50_000,
            data_provider=provider,
            preferred_source="ibkr",
        )
        results = await scanner.scan()

        assert len(results) == 1
        r = results[0]
        assert isinstance(r, ScanResult)
        assert r.symbol == "NVDA"
        assert r.direction == "up"
        assert r.gap_pct == pytest.approx((105.0 - 100.0) / 100.0)
        assert r.premarket_volume == 120_000

    @pytest.mark.asyncio
    async def test_scan_skips_when_volume_below_threshold(self):
        provider = _FakeProvider()
        provider.set("AMD", "1Day", _daily(100.0, 103.0))
        provider.set("AMD", "1Min", _intraday(105.0, volume=1_000))

        scanner = PremarketScanner(
            watchlist=["AMD"],
            min_gap_pct=0.02,
            max_gap_pct=0.10,
            min_premarket_vol=50_000,
            data_provider=provider,
            preferred_source="bars",
        )
        results = await scanner.scan()
        assert results == []

    @pytest.mark.asyncio
    async def test_scan_skips_when_gap_too_small(self):
        provider = _FakeProvider()
        provider.set("MU", "1Day", _daily(100.0, 100.5))
        provider.set("MU", "1Min", _intraday(100.5, volume=500_000))

        scanner = PremarketScanner(
            watchlist=["MU"],
            min_gap_pct=0.02,
            max_gap_pct=0.10,
            min_premarket_vol=50_000,
            data_provider=provider,
            preferred_source="bars",
        )
        results = await scanner.scan()
        assert results == []

    @pytest.mark.asyncio
    async def test_scan_returns_empty_when_no_data_source(self):
        scanner = PremarketScanner(
            watchlist=["NVDA"],
            preferred_source="bars",
            data_provider=None,
        )
        # Kein Provider, kein Alpaca-Client → no_data_source-Warnung, []
        results = await scanner.scan()
        assert results == []

    @pytest.mark.asyncio
    async def test_intraday_missing_falls_back_to_daily_close(self):
        provider = _FakeProvider()
        # Tages-DF vorhanden, Intraday leer → Scanner nutzt letzten Daily-Close
        provider.set("GOOGL", "1Day", _daily(100.0, 105.0))
        # Kein Intraday-Eintrag → FakeProvider liefert leeren DataFrame
        scanner = PremarketScanner(
            watchlist=["GOOGL"],
            min_gap_pct=0.02,
            min_premarket_vol=10_000,
            data_provider=provider,
            preferred_source="bars",
        )
        results = await scanner.scan()
        # Daily-Volume in _daily = 500_000 > 10_000 → Resultat erwartet
        assert len(results) == 1
        assert results[0].symbol == "GOOGL"
        assert results[0].direction == "up"


class TestExtractPrevClose:
    def test_two_days_returns_penultimate(self):
        df = _daily(99.0, 104.0)
        assert _extract_prev_close(df) == 99.0

    def test_one_day_returns_that_day(self):
        idx = pd.DatetimeIndex(
            [datetime(2026, 4, 22, 21, 0, tzinfo=timezone.utc)], tz="UTC"
        )
        df = pd.DataFrame(
            {"Open": [100.0], "High": [101.0], "Low": [99.0],
             "Close": [100.5], "Volume": [1_000]},
            index=idx,
        )
        assert _extract_prev_close(df) == 100.5

    def test_empty_returns_none(self):
        df = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
        assert _extract_prev_close(df) is None

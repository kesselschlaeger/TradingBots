"""YFinanceDataProvider – kostenlose Backtest-Datenquelle (yfinance)."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

import pandas as pd
import pytz

from core.logging import get_logger
from core.models import Bar
from data.providers.base import DataProvider

log = get_logger(__name__)

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False


_INTERVAL_MAP = {
    "1min": "1m", "1m": "1m",
    "2min": "2m", "2m": "2m",
    "5min": "5m", "5m": "5m",
    "15min": "15m", "15m": "15m",
    "30min": "30m", "30m": "30m",
    "60min": "60m", "1hour": "60m", "1h": "60m",
    "1day": "1d", "day": "1d", "1d": "1d",
}


def _to_yf_interval(tf: str) -> str:
    return _INTERVAL_MAP.get(tf.lower().strip(), "5m")


def _flatten_columns(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        try:
            df = df.xs(symbol, axis=1, level=1)
        except (KeyError, ValueError):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    keep = ["Open", "High", "Low", "Close", "Volume"]
    cols = [c for c in keep if c in df.columns]
    return df[cols].copy()


class YFinanceDataProvider(DataProvider):
    """Backtest-Daten via yfinance.

    Beachte: yfinance liefert Intraday-Bars nur ~60 Tage zurück.
    Für längere Backtests Daily-Bars verwenden.
    """

    def __init__(self, prepost: bool = False):
        if not YF_AVAILABLE:
            raise RuntimeError("yfinance fehlt – pip install yfinance")
        self.prepost = prepost

    async def get_bars(self, symbol: str, start: datetime, end: datetime,
                       timeframe: str = "5Min") -> pd.DataFrame:
        interval = _to_yf_interval(timeframe)
        loop = asyncio.get_event_loop()

        def _fetch():
            df = yf.download(
                symbol, start=start, end=end, interval=interval,
                prepost=self.prepost, progress=False, auto_adjust=False,
                threads=False,
            )
            if df is None or df.empty:
                return pd.DataFrame()
            df = _flatten_columns(df, symbol)
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
            return df

        try:
            return await loop.run_in_executor(None, _fetch)
        except Exception as e:  # noqa: BLE001
            log.warning("yfinance.fetch_failed", symbol=symbol, error=str(e))
            return pd.DataFrame()

    async def get_bars_bulk(self, symbols: list[str], start: datetime,
                            end: datetime, timeframe: str = "5Min",
                            ) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        for sym in symbols:
            df = await self.get_bars(sym, start, end, timeframe)
            if not df.empty:
                result[sym] = df
                log.info("yfinance.loaded", symbol=sym, bars=len(df))
        return result

    def check_bar_freshness(self, df: pd.DataFrame,
                            max_delay_minutes: int = 60) -> bool:
        if df.empty:
            return False
        last = df.index[-1]
        if hasattr(last, "to_pydatetime"):
            last = last.to_pydatetime()
        if last.tzinfo is None:
            last = pytz.utc.localize(last)
        delay = (datetime.now(timezone.utc) - last).total_seconds() / 60
        return delay <= max_delay_minutes

    async def stream_bars(self, symbols: list[str], timeframe: str = "5Min",
                          ) -> AsyncIterator[Bar]:
        raise NotImplementedError(
            "YFinanceDataProvider unterstützt keinen Live-Stream"
        )
        if False:  # pragma: no cover
            yield  # type: ignore[unreachable]

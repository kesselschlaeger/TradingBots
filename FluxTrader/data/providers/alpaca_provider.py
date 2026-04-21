"""AlpacaDataProvider – historische + Live-Bars via alpaca-py."""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Optional

import pandas as pd
import pytz

from core.logging import get_logger
from core.models import Bar
from data.providers.base import DataProvider

log = get_logger(__name__)

try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    ALPACA_DATA_AVAILABLE = True
except ImportError:
    ALPACA_DATA_AVAILABLE = False


_CHUNK_DAYS = 30
_MAX_RETRIES = 5


def _parse_timeframe(tf: str):
    """'5Min' -> TimeFrame(5, Minute); '1Day' -> TimeFrame.Day."""
    tf_lower = tf.lower().strip()
    if tf_lower in ("1day", "day", "1d"):
        return TimeFrame.Day
    if tf_lower in ("1hour", "hour", "1h"):
        return TimeFrame.Hour
    import re
    m = re.match(r"(\d+)(min|m)$", tf_lower)
    if m:
        n = int(m.group(1))
        try:
            return TimeFrame(n, TimeFrameUnit.Minute)
        except Exception:
            if n == 5 and hasattr(TimeFrame, "Minute5"):
                return TimeFrame.Minute5
            return TimeFrame.Minute
    return TimeFrame.Minute


def _rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume",
    })[["Open", "High", "Low", "Close", "Volume"]]


class AlpacaDataProvider(DataProvider):
    def __init__(self,
                 api_key: Optional[str] = None,
                 secret_key: Optional[str] = None,
                 feed: str = "iex"):
        if not ALPACA_DATA_AVAILABLE:
            raise RuntimeError("alpaca-py fehlt – pip install alpaca-py")
        api_key = api_key or os.getenv("APCA_API_KEY_ID")
        secret_key = secret_key or os.getenv("APCA_API_SECRET_KEY")
        if not api_key or not secret_key:
            raise RuntimeError(
                "APCA_API_KEY_ID und APCA_API_SECRET_KEY müssen gesetzt sein"
            )
        self._client = StockHistoricalDataClient(api_key=api_key,
                                                 secret_key=secret_key)
        self.feed = feed

    async def get_bars(self, symbol: str, start: datetime, end: datetime,
                       timeframe: str = "5Min") -> pd.DataFrame:
        tf = _parse_timeframe(timeframe)
        loop = asyncio.get_event_loop()

        def _fetch():
            req = StockBarsRequest(
                symbol_or_symbols=symbol, timeframe=tf,
                start=start, end=end, adjustment="raw", feed=self.feed,
            )
            bars = self._client.get_stock_bars(req)
            if bars.df is None or bars.df.empty:
                return pd.DataFrame()
            if isinstance(bars.df.index, pd.MultiIndex):
                df = bars.df.loc[symbol].copy()
            else:
                df = bars.df.copy()
            return _rename_cols(df)

        try:
            return await loop.run_in_executor(None, _fetch)
        except Exception as e:  # noqa: BLE001
            log.warning("alpaca_data.fetch_failed", symbol=symbol, error=str(e))
            return pd.DataFrame()

    async def get_bars_bulk(self, symbols: list[str], start: datetime,
                            end: datetime, timeframe: str = "5Min",
                            ) -> dict[str, pd.DataFrame]:
        """Chunked, robust gegen Pagination/429."""
        tf = _parse_timeframe(timeframe)
        loop = asyncio.get_event_loop()
        result: dict[str, pd.DataFrame] = {}

        for sym in symbols:
            chunks: list[pd.DataFrame] = []
            cur = start
            while cur <= end:
                nxt = min(cur + timedelta(days=_CHUNK_DAYS) - timedelta(microseconds=1),
                          end)
                for attempt in range(1, _MAX_RETRIES + 1):
                    try:
                        def _fetch(s=sym, a=cur, b=nxt):
                            req = StockBarsRequest(
                                symbol_or_symbols=s, timeframe=tf,
                                start=a, end=b, adjustment="raw", feed=self.feed,
                            )
                            bars = self._client.get_stock_bars(req)
                            if bars.df is None or bars.df.empty:
                                return pd.DataFrame()
                            if isinstance(bars.df.index, pd.MultiIndex):
                                return _rename_cols(bars.df.loc[s].copy())
                            return _rename_cols(bars.df.copy())

                        df = await loop.run_in_executor(None, _fetch)
                        if not df.empty:
                            chunks.append(df)
                        break
                    except Exception as e:  # noqa: BLE001
                        if attempt == _MAX_RETRIES:
                            log.warning("alpaca_data.chunk_failed",
                                        symbol=sym, start=str(cur),
                                        end=str(nxt), error=str(e))
                            break
                        await asyncio.sleep(min(2 ** (attempt - 1), 16))
                cur = nxt + timedelta(microseconds=1)

            if chunks:
                merged = pd.concat(chunks, axis=0)
                merged = merged[~merged.index.duplicated(keep="last")].sort_index()
                result[sym] = merged
                log.info("alpaca_data.loaded", symbol=sym, bars=len(merged))
        return result

    def check_bar_freshness(self, df: pd.DataFrame,
                            max_delay_minutes: int = 20) -> bool:
        if df.empty:
            return False
        last = df.index[-1]
        if hasattr(last, "to_pydatetime"):
            last = last.to_pydatetime()
        if last.tzinfo is None:
            last = pytz.utc.localize(last)
        delay = (datetime.now(timezone.utc) - last).total_seconds() / 60
        if delay > max_delay_minutes:
            log.warning("alpaca_data.stale", delay_min=delay,
                        max_delay_min=max_delay_minutes)
            return False
        return True

    async def stream_bars(self, symbols: list[str], timeframe: str = "5Min",
                          ) -> AsyncIterator[Bar]:
        """Minimale Polling-Schleife. Wer echten WebSocket-Stream will,
        erweitert dies um alpaca.data.live.StockDataStream."""
        last_seen: dict[str, datetime] = {}
        while True:
            end = datetime.now(timezone.utc)
            start = end - timedelta(minutes=30)
            for sym in symbols:
                df = await self.get_bars(sym, start, end, timeframe)
                if df.empty:
                    continue

                # Alle Bars seit letzter Sichtung yielden – nicht nur den
                # letzten. Sonst gehen Bars verloren, wenn das Poll-
                # Intervall größer ist als das Bar-Intervall.
                cutoff = last_seen.get(sym)
                if cutoff is not None:
                    df = df[df.index > cutoff]
                if df.empty:
                    continue

                for ts, row in df.iterrows():
                    py_ts = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                    if py_ts.tzinfo is None:
                        py_ts = pytz.utc.localize(py_ts)
                    yield Bar(
                        symbol=sym,
                        timestamp=py_ts,
                        open=float(row["Open"]),
                        high=float(row["High"]),
                        low=float(row["Low"]),
                        close=float(row["Close"]),
                        volume=int(row["Volume"]),
                    )
                    last_seen[sym] = py_ts
            await asyncio.sleep(30)

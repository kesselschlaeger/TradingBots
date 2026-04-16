"""IBKRDataProvider – historische + Live-Bars via ib_insync."""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import AsyncIterator, Optional

import pandas as pd

from core.logging import get_logger
from core.models import Bar
from data.providers.base import DataProvider

log = get_logger(__name__)

try:
    from ib_insync import IB, Stock, util
    IBKR_AVAILABLE = True
except ImportError:
    IBKR_AVAILABLE = False


_BAR_SIZE_MAP = {
    "1min": "1 min",
    "1m": "1 min",
    "2min": "2 mins",
    "2m": "2 mins",
    "5min": "5 mins",
    "5m": "5 mins",
    "15min": "15 mins",
    "15m": "15 mins",
    "30min": "30 mins",
    "30m": "30 mins",
    "60min": "1 hour",
    "1hour": "1 hour",
    "1h": "1 hour",
    "1day": "1 day",
    "day": "1 day",
    "1d": "1 day",
}


def _to_bar_size_setting(timeframe: str) -> str:
    return _BAR_SIZE_MAP.get(timeframe.lower().strip(), "5 mins")


def _to_duration_str(start: datetime, end: datetime) -> str:
    total_seconds = max((end - start).total_seconds(), 1)
    days = max(1, ceil(total_seconds / 86400.0))
    return f"{days} D"


def _normalize_df(raw: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()

    cols = ["date", "open", "high", "low", "close", "volume"]
    cols = [c for c in cols if c in raw.columns]
    if not cols:
        return pd.DataFrame()

    df = raw[cols].copy()
    if "date" not in df.columns:
        return pd.DataFrame()

    idx = pd.to_datetime(df.pop("date"), utc=True)
    df.index = idx

    rename_map = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    }
    df = df.rename(columns=rename_map)
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c not in df.columns:
            df[c] = 0.0

    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    df = df[(df.index >= start_utc) & (df.index <= end_utc)]
    df = df[["Open", "High", "Low", "Close", "Volume"]]
    return df[~df.index.duplicated(keep="last")].sort_index()


class IBKRDataProvider(DataProvider):
    """IBKR-Datenprovider über TWS/Gateway mit ib_insync.

    Nutzt eine eigene IBKR-Verbindung, daher standardmäßig separater
    client_id (broker_client_id + 100 empfohlen).
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        client_id: Optional[int] = None,
        use_rth: bool = True,
    ):
        if not IBKR_AVAILABLE:
            raise RuntimeError("ib_insync fehlt – pip install ib_insync")

        self._host = host or os.getenv("IBKR_HOST", "127.0.0.1")
        self._port = port or int(os.getenv("IBKR_PORT", "4002"))
        if client_id is None:
            env_id = os.getenv("IBKR_DATA_CLIENT_ID")
            if env_id:
                client_id = int(env_id)
            else:
                client_id = int(os.getenv("IBKR_CLIENT_ID", "1")) + 100
        self._client_id = client_id
        self._use_rth = use_rth

        self.ib = IB()
        try:
            asyncio.get_running_loop()
            log.info("ibkr_data.connect_deferred", client_id=self._client_id)
        except RuntimeError:
            self._connect_sync()

    @staticmethod
    def _ensure_thread_event_loop() -> None:
        """ib_insync braucht in jedem aufrufenden Thread einen gesetzten Loop."""
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def _connect_sync(self) -> None:
        import time as _time
        self._ensure_thread_event_loop()

        for attempt in range(1, 4):
            try:
                self.ib.connect(
                    self._host,
                    self._port,
                    clientId=self._client_id,
                    timeout=20,
                    readonly=True,
                )
                log.info(
                    "ibkr_data.connected",
                    host=self._host,
                    port=self._port,
                    client_id=self._client_id,
                    use_rth=self._use_rth,
                )
                return
            except Exception as e:  # noqa: BLE001
                if "326" in str(e):
                    raise RuntimeError(
                        f"IBKR Data Client-ID {self._client_id} bereits belegt. "
                        "Bitte eine freie IBKR_DATA_CLIENT_ID verwenden."
                    ) from e
                if attempt == 3:
                    raise
                _time.sleep(attempt * 5)

    def _ensure_connected(self) -> None:
        if not self.ib.isConnected():
            log.warning("ibkr_data.reconnect")
            self._connect_sync()

    async def _run(self, func, *args, **kwargs):
        loop = asyncio.get_event_loop()

        def call():
            self._ensure_thread_event_loop()
            self._ensure_connected()
            return func(*args, **kwargs)

        return await loop.run_in_executor(None, call)

    async def get_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "5Min",
    ) -> pd.DataFrame:
        bar_size = _to_bar_size_setting(timeframe)
        duration = _to_duration_str(start, end)

        def _fetch() -> pd.DataFrame:
            self._ensure_connected()
            contract = Stock(symbol.upper(), "SMART", "USD")
            self.ib.qualifyContracts(contract)
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=end,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=self._use_rth,
                formatDate=1,
                keepUpToDate=False,
            )
            return util.df(bars)

        try:
            raw = await self._run(_fetch)
            return _normalize_df(raw, start, end)
        except Exception as e:  # noqa: BLE001
            log.warning("ibkr_data.fetch_failed", symbol=symbol, error=str(e))
            return pd.DataFrame()

    async def get_bars_bulk(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
        timeframe: str = "5Min",
    ) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        for sym in symbols:
            df = await self.get_bars(sym, start, end, timeframe)
            if not df.empty:
                result[sym] = df
                log.info("ibkr_data.loaded", symbol=sym, bars=len(df))
        return result

    def check_bar_freshness(
        self,
        df: pd.DataFrame,
        max_delay_minutes: int = 20,
    ) -> bool:
        if df.empty:
            return False
        last = df.index[-1]
        if hasattr(last, "to_pydatetime"):
            last = last.to_pydatetime()
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        delay = (datetime.now(timezone.utc) - last).total_seconds() / 60
        if delay > max_delay_minutes:
            log.warning(
                "ibkr_data.stale",
                delay_min=delay,
                max_delay_min=max_delay_minutes,
            )
            return False
        return True

    async def stream_bars(
        self,
        symbols: list[str],
        timeframe: str = "5Min",
    ) -> AsyncIterator[Bar]:
        last_seen: dict[str, datetime] = {}

        while True:
            end = datetime.now(timezone.utc)
            start = end - timedelta(minutes=30)
            for sym in symbols:
                df = await self.get_bars(sym, start, end, timeframe)
                if df.empty:
                    continue

                last = df.index[-1]
                if hasattr(last, "to_pydatetime"):
                    last = last.to_pydatetime()
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                if last_seen.get(sym) == last:
                    continue

                last_seen[sym] = last
                row = df.iloc[-1]
                yield Bar(
                    symbol=sym,
                    timestamp=last,
                    open=float(row["Open"]),
                    high=float(row["High"]),
                    low=float(row["Low"]),
                    close=float(row["Close"]),
                    volume=int(row.get("Volume", 0) or 0),
                )
            await asyncio.sleep(30)

    async def close(self) -> None:
        if self.ib.isConnected():
            await asyncio.get_event_loop().run_in_executor(None, self.ib.disconnect)
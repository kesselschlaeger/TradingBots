"""BybitDataProvider – historische Bars via pybit v5 (sync → async).

Liefert OHLCV-Daten aus der Bybit Kline-API als pandas DataFrame.
Kein Live-Stream; der LiveRunner fällt auf Polling-Modus zurück.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from core.logging import get_logger
from data.providers.base import DataProvider

log = get_logger(__name__)

try:
    from pybit.unified_trading import HTTP
    PYBIT_AVAILABLE = True
except ImportError:
    PYBIT_AVAILABLE = False


_TIMEFRAME_MAP: dict[str, str] = {
    "1min": "1", "1m": "1",
    "3min": "3", "3m": "3",
    "5min": "5", "5m": "5",
    "15min": "15", "15m": "15",
    "30min": "30", "30m": "30",
    "60min": "60", "1h": "60",
    "1hour": "60",
    "1day": "D", "day": "D", "1d": "D",
}

_MAX_LIMIT = 200  # Bybit-Maximum pro Request


def _map_interval(timeframe: str) -> str:
    return _TIMEFRAME_MAP.get(timeframe.lower().strip(), "5")


class BybitDataProvider(DataProvider):
    """Datenprovider für Bybit-Spot- und Linear-Märkte."""

    def __init__(
        self,
        testnet: bool = True,
        api_key: str = "",
        api_secret: str = "",
        category: str = "spot",        # "spot" | "linear" | "inverse"
    ) -> None:
        if not PYBIT_AVAILABLE:
            raise RuntimeError("pybit fehlt – pip install pybit")
        self._http = HTTP(testnet=testnet, api_key=api_key, api_secret=api_secret)
        self._category = category
        log.info("bybit_provider.init", category=category,
                 mode="TESTNET" if testnet else "MAINNET")

    # ── DataProvider-Interface ───────────────────────────────────────

    async def get_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "5Min",
    ) -> pd.DataFrame:
        """Lädt OHLCV-Bars für das Zeitfenster [start, end].

        Holt mehrere Seiten wenn nötig (Bybit liefert max. 200 Bars/Request).
        """
        interval = _map_interval(timeframe)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        all_rows: list[list] = []
        cursor_end = end_ms

        loop = asyncio.get_running_loop()

        while True:
            try:
                result = await loop.run_in_executor(None, lambda ce=cursor_end: self._http.get_kline(
                    category=self._category,
                    symbol=symbol,
                    interval=interval,
                    end=ce,
                    limit=_MAX_LIMIT,
                ))
            except Exception as e:
                log.error("bybit_provider.get_kline_exception",
                          symbol=symbol, error=str(e))
                break

            if result.get("retCode") != 0:
                log.error("bybit_provider.get_kline_failed",
                          symbol=symbol, retMsg=result.get("retMsg"))
                break

            rows: list[list] = result["result"].get("list", [])
            if not rows:
                break

            # Bybit liefert absteigende Zeitreihe; ältester Bar zuletzt
            oldest_ts_ms = int(rows[-1][0])
            all_rows.extend(rows)

            if oldest_ts_ms <= start_ms:
                break  # Zielzeitfenster vollständig abgedeckt

            # Nächste Seite: Ende = ältester Bar dieser Seite minus 1 ms
            cursor_end = oldest_ts_ms - 1

        if not all_rows:
            return pd.DataFrame()

        df = _build_dataframe(all_rows)
        df = df[(df.index >= start) & (df.index <= end)]
        return df

    def check_bar_freshness(
        self,
        df: pd.DataFrame,
        max_delay_minutes: int = 20,
    ) -> bool:
        if df.empty:
            return False
        last_ts = df.index[-1]
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize("UTC")
        delay = (datetime.now(timezone.utc) - last_ts).total_seconds() / 60
        return delay <= max_delay_minutes


def _build_dataframe(rows: list[list]) -> pd.DataFrame:
    """Baut DataFrame aus Bybit-Kline-Rows auf.

    Row-Format: [startTime, open, high, low, close, volume, turnover]
    Zeitreihe kommt absteigend → aufsteigend sortieren.
    """
    records = []
    for row in rows:
        ts_ms = int(row[0])
        records.append({
            "timestamp": pd.Timestamp(ts_ms, unit="ms", tz="UTC"),
            "Open": float(row[1]),
            "High": float(row[2]),
            "Low": float(row[3]),
            "Close": float(row[4]),
            "Volume": float(row[5]),
        })

    df = pd.DataFrame(records).set_index("timestamp").sort_index()
    return df

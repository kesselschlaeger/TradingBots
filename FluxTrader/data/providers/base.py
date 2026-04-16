"""DataProvider – abstraktes Interface für Marktdaten."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import AsyncIterator, Optional

import pandas as pd

from core.models import Bar


class DataProvider(ABC):
    """Liefert historische Bars (Backtest) und optional Live-Streams."""

    @abstractmethod
    async def get_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "5Min",
    ) -> pd.DataFrame:
        """OHLCV-DataFrame mit DatetimeIndex (UTC).

        Spalten: Open, High, Low, Close, Volume.
        Leerer DataFrame bei keinen Daten.
        """

    async def get_bars_bulk(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
        timeframe: str = "5Min",
    ) -> dict[str, pd.DataFrame]:
        """Default: get_bars für jedes Symbol."""
        result: dict[str, pd.DataFrame] = {}
        for s in symbols:
            df = await self.get_bars(s, start, end, timeframe)
            if not df.empty:
                result[s] = df
        return result

    @abstractmethod
    def check_bar_freshness(self,
                            df: pd.DataFrame,
                            max_delay_minutes: int = 20) -> bool:
        """True wenn letzter Bar frisch genug."""

    async def stream_bars(self,
                          symbols: list[str],
                          timeframe: str = "5Min",
                          ) -> AsyncIterator[Bar]:
        """Live-Bar-Stream. Default-Raise; Adapter, die nicht streamen
        können, werfen NotImplementedError (Runner wählt Polling-Fallback)."""
        raise NotImplementedError(
            f"{self.__class__.__name__} unterstützt keinen Live-Stream"
        )
        # pragma: no cover – yield ist nötig, damit Signature async iter bleibt
        if False:  # noqa: E501
            yield  # type: ignore[unreachable]

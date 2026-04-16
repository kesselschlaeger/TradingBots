"""Premarket Gap-Scanner – identifiziert Kandidaten für ORB-Strategien.

Scannt vor Market-Open die Watchlist nach Symbolen mit signifikantem
Gap-Up/Down und ausreichendem Premarket-Volume. Nutzt AlpacaDataProvider
oder Snapshot-API.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.logging import get_logger

log = get_logger(__name__)

try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockSnapshotRequest
    ALPACA_SNAPSHOT_AVAILABLE = True
except ImportError:
    ALPACA_SNAPSHOT_AVAILABLE = False


@dataclass
class ScanResult:
    symbol: str
    prev_close: float
    premarket_price: float
    gap_pct: float
    premarket_volume: int
    direction: str            # "up" | "down"


class PremarketScanner:
    """Gap-Scanner via Alpaca Snapshot-API."""

    def __init__(self,
                 watchlist: list[str],
                 min_gap_pct: float = 0.02,
                 max_gap_pct: float = 0.10,
                 min_premarket_vol: int = 50_000,
                 api_key: Optional[str] = None,
                 secret_key: Optional[str] = None,
                 feed: str = "iex"):
        self.watchlist = [s.upper() for s in watchlist]
        self.min_gap_pct = min_gap_pct
        self.max_gap_pct = max_gap_pct
        self.min_premarket_vol = min_premarket_vol
        self.feed = feed

        if ALPACA_SNAPSHOT_AVAILABLE:
            key = api_key or os.getenv("APCA_API_KEY_ID", "")
            secret = secret_key or os.getenv("APCA_API_SECRET_KEY", "")
            if key and secret:
                self._client = StockHistoricalDataClient(
                    api_key=key, secret_key=secret,
                )
            else:
                self._client = None
        else:
            self._client = None

    async def scan(self) -> list[ScanResult]:
        if self._client is None:
            log.warning("scanner.no_client")
            return []

        loop = asyncio.get_event_loop()

        def _fetch():
            req = StockSnapshotRequest(
                symbol_or_symbols=self.watchlist, feed=self.feed,
            )
            return self._client.get_stock_snapshot(req)

        try:
            snapshots = await loop.run_in_executor(None, _fetch)
        except Exception as e:  # noqa: BLE001
            log.warning("scanner.fetch_failed", error=str(e))
            return []

        results: list[ScanResult] = []
        for sym, snap in snapshots.items():
            try:
                prev_close = float(snap.previous_daily_bar.close)
                current = float(snap.latest_trade.price) if snap.latest_trade else None
                pm_vol = int(snap.minute_bar.volume) if snap.minute_bar else 0

                if current is None or prev_close <= 0:
                    continue

                gap_pct = (current - prev_close) / prev_close
                abs_gap = abs(gap_pct)

                if abs_gap < self.min_gap_pct or abs_gap > self.max_gap_pct:
                    continue
                if pm_vol < self.min_premarket_vol:
                    continue

                results.append(ScanResult(
                    symbol=sym,
                    prev_close=prev_close,
                    premarket_price=current,
                    gap_pct=gap_pct,
                    premarket_volume=pm_vol,
                    direction="up" if gap_pct > 0 else "down",
                ))
            except (AttributeError, TypeError, ValueError) as e:
                log.debug("scanner.parse_skip", symbol=sym, error=str(e))
                continue

        results.sort(key=lambda r: abs(r.gap_pct), reverse=True)
        log.info("scanner.results", count=len(results),
                 symbols=[r.symbol for r in results[:10]])
        return results

    async def scan_filtered(self,
                            max_results: int = 10,
                            exclude: Optional[set[str]] = None,
                            ) -> list[ScanResult]:
        all_results = await self.scan()
        exclude = exclude or set()
        filtered = [r for r in all_results if r.symbol not in exclude]
        return filtered[:max_results]

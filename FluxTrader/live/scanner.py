"""Premarket Gap-Scanner – provider-agnostisch.

Identifiziert Symbole mit signifikantem Premarket-Gap. Unterstützt zwei
Datenpfade:

1. **Snapshot-Pfad** (Alpaca) – schnell, nutzt ``get_stock_snapshot``.
2. **Bars-Pfad** (IBKR / yfinance / generisch) – zwei ``DataProvider.get_bars``-
   Calls: zuerst der letzte Tages-Close, dann die letzte Premarket-Bar. So
   arbeitet der Scanner auch in Setups ohne Alpaca-Credentials (der häufigste
   Grund für das stille ``scanner.no_client`` im bisherigen Live-Betrieb).

Der Scanner wählt den Pfad anhand der übergebenen ``DataProvider``-Instanz:
bietet sie ``get_snapshot`` an (Duck-Typing), wird dieser bevorzugt; sonst
fällt er auf zwei ``get_bars``-Calls zurück.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd

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
    """Gap-Scanner, provider-agnostisch."""

    def __init__(self,
                 watchlist: list[str],
                 min_gap_pct: float = 0.02,
                 max_gap_pct: float = 0.10,
                 min_premarket_vol: int = 50_000,
                 api_key: Optional[str] = None,
                 secret_key: Optional[str] = None,
                 feed: str = "iex",
                 data_provider: Optional[Any] = None,
                 preferred_source: str = "auto",
                 premarket_hours: int = 4):
        """Initialisiert Scanner.

        ``data_provider``: optionaler ``DataProvider`` (IBKR/yfinance/etc.).
        Wenn gesetzt, wird der Bars-Pfad aktiv. ``preferred_source`` erlaubt
        das explizite Erzwingen von ``"alpaca"``, ``"ibkr"`` oder ``"auto"``
        (Default: Snapshot wenn vorhanden, sonst Bars-Provider).
        ``premarket_hours`` steuert, wie weit in die Vergangenheit der
        Bars-Pfad für den Premarket-Snapshot geht.
        """
        self.watchlist = [s.upper() for s in watchlist]
        self.min_gap_pct = min_gap_pct
        self.max_gap_pct = max_gap_pct
        self.min_premarket_vol = min_premarket_vol
        self.feed = feed
        self.data_provider = data_provider
        self.preferred_source = preferred_source
        self.premarket_hours = max(1, int(premarket_hours))

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

    # ── Source-Wahl ─────────────────────────────────────────────────

    def _resolve_source(self) -> str:
        """Liefert 'alpaca' | 'bars' | 'none' anhand preferred_source und
        Verfügbarkeit von Alpaca-Client/DataProvider."""
        src = (self.preferred_source or "auto").lower()
        if src == "alpaca":
            return "alpaca" if self._client is not None else "none"
        if src in {"ibkr", "yfinance", "bars"}:
            return "bars" if self.data_provider is not None else "none"
        # auto
        if self._client is not None:
            return "alpaca"
        if self.data_provider is not None:
            return "bars"
        return "none"

    # ── Public API ──────────────────────────────────────────────────

    async def scan(self) -> list[ScanResult]:
        source = self._resolve_source()
        if source == "alpaca":
            return await self._scan_alpaca_snapshot()
        if source == "bars":
            return await self._scan_via_bars()
        log.warning("scanner.no_data_source",
                    preferred=self.preferred_source,
                    alpaca_client=self._client is not None,
                    has_provider=self.data_provider is not None)
        return []

    async def scan_filtered(self,
                            max_results: int = 10,
                            exclude: Optional[set[str]] = None,
                            ) -> list[ScanResult]:
        all_results = await self.scan()
        exclude = exclude or set()
        filtered = [r for r in all_results if r.symbol not in exclude]
        return filtered[:max_results]

    # ── Alpaca-Snapshot-Pfad ────────────────────────────────────────

    async def _scan_alpaca_snapshot(self) -> list[ScanResult]:
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

                result = self._classify_gap(
                    symbol=sym,
                    prev_close=prev_close,
                    premarket_price=current,
                    pm_vol=pm_vol,
                )
                if result is not None:
                    results.append(result)
            except (AttributeError, TypeError, ValueError) as e:
                log.debug("scanner.parse_skip", symbol=sym, error=str(e))
                continue

        results.sort(key=lambda r: abs(r.gap_pct), reverse=True)
        log.info("scanner.results", count=len(results),
                 source="alpaca",
                 symbols=[r.symbol for r in results[:10]])
        return results

    # ── Bars-Fallback-Pfad ──────────────────────────────────────────

    async def _scan_via_bars(self) -> list[ScanResult]:
        if self.data_provider is None:
            log.warning("scanner.no_provider")
            return []

        now = datetime.now(timezone.utc)
        daily_start = now - timedelta(days=10)
        intraday_start = now - timedelta(hours=self.premarket_hours)

        results: list[ScanResult] = []
        fetch_errors: list[str] = []

        for sym in self.watchlist:
            try:
                daily_df = await self.data_provider.get_bars(
                    sym, daily_start, now, "1Day",
                )
            except Exception as e:  # noqa: BLE001
                fetch_errors.append(f"{sym}:daily={e}")
                continue
            if daily_df is None or daily_df.empty:
                continue

            prev_close = _extract_prev_close(daily_df)
            if prev_close is None or prev_close <= 0:
                continue

            try:
                intraday_df = await self.data_provider.get_bars(
                    sym, intraday_start, now, "1Min",
                )
            except Exception:
                intraday_df = None
            if intraday_df is None or (
                hasattr(intraday_df, "empty") and intraday_df.empty
            ):
                # Intraday nicht verfügbar → fallback auf letzten 1Day-Close.
                pm_price = float(daily_df["Close"].iloc[-1])
                pm_vol = int(daily_df["Volume"].iloc[-1])
            else:
                pm_price = float(intraday_df["Close"].iloc[-1])
                pm_vol = int(intraday_df["Volume"].sum())

            result = self._classify_gap(
                symbol=sym,
                prev_close=prev_close,
                premarket_price=pm_price,
                pm_vol=pm_vol,
            )
            if result is not None:
                results.append(result)

        if fetch_errors:
            log.warning("scanner.bars_fetch_errors",
                        errors=fetch_errors[:5], total=len(fetch_errors))

        results.sort(key=lambda r: abs(r.gap_pct), reverse=True)
        log.info("scanner.results", count=len(results),
                 source="bars",
                 symbols=[r.symbol for r in results[:10]])
        return results

    # ── Klassifikation ──────────────────────────────────────────────

    def _classify_gap(
        self,
        *,
        symbol: str,
        prev_close: float,
        premarket_price: float,
        pm_vol: int,
    ) -> Optional[ScanResult]:
        gap_pct = (premarket_price - prev_close) / prev_close
        abs_gap = abs(gap_pct)

        if abs_gap < self.min_gap_pct or abs_gap > self.max_gap_pct:
            return None
        if pm_vol < self.min_premarket_vol:
            return None

        return ScanResult(
            symbol=symbol,
            prev_close=float(prev_close),
            premarket_price=float(premarket_price),
            gap_pct=float(gap_pct),
            premarket_volume=int(pm_vol),
            direction="up" if gap_pct > 0 else "down",
        )


def _extract_prev_close(daily_df: "pd.DataFrame") -> Optional[float]:
    """Hole den vorletzten Daily-Close (= prev_close gegenüber heute).

    Wenn nur ein Tag vorhanden ist, nimm diesen. So funktioniert der Scanner
    auch bei sehr kurzen Daten-Lookbacks.
    """
    if daily_df is None or daily_df.empty:
        return None
    closes = daily_df["Close"]
    if len(closes) >= 2:
        return float(closes.iloc[-2])
    return float(closes.iloc[-1])

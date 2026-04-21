"""PairEngine – dedizierter Execution-Pfad für Pair-Strategien.

Läuft als separate Task im selben Event-Loop wie LiveRunner.
Keine Subklasse von LiveRunner – eigenständige Komponente.

Verantwortlichkeiten:
  - Holt Bars für beide Symbole synchron (gleicher Timestamp)
  - Ruft strategy._generate_pair_signal(bar_a, bar_b, context) → PairSignal
  - Leitet PairSignal an MLFilter weiter (wenn aktiv)
  - Übergibt an broker.execute_pair_signal()
  - Registriert beide Legs im TradeManager
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.context import MarketContextService
from core.logging import get_logger
from core.ml_filter import MLFilter
from core.models import Bar, PairSignal
from core.trade_manager import ManagedTrade, TradeManager
from data.providers.base import DataProvider
from execution.port import BrokerPort
from live.health import HealthState
from strategy.base import PairStrategy

log = get_logger(__name__)


class PairEngine:
    """Dedizierter Execution-Pfad für Pair-Strategien."""

    def __init__(
        self,
        strategy: PairStrategy,
        broker: BrokerPort,
        data_provider: DataProvider,
        context: MarketContextService,
        ml_filter: MLFilter,
        config: dict,
        health_state: Optional[HealthState] = None,
    ):
        self.strategy = strategy
        self.broker = broker
        self.data = data_provider
        self.ctx = context
        self.ml_filter = ml_filter
        self.cfg = config
        self.health: Optional[HealthState] = health_state

        self.tm = TradeManager(
            use_trailing=False,
            eod_close_time=config.get("eod_close_time"),
        )

        self._running = False
        self._has_position = False

        # Pair-Status-Reporting: Strategie meldet pro-Paar-Status
        # (WAIT_WARMUP, WAIT_Z, SIGNAL) sync in HealthState.
        if self.health is not None and hasattr(self.strategy, "set_status_sink"):
            strat_name = self.strategy.name
            self.strategy.set_status_sink(
                lambda key, code, reason:
                    self.health.set_symbol_status(strat_name, key, code, reason)
            )

    async def run(self) -> None:
        """Hauptloop: pollt Bars für beide Symbole und verarbeitet PairSignals."""
        self._running = True
        sym_a = self.strategy.symbol_a
        sym_b = self.strategy.symbol_b
        tf = str(self.cfg.get("timeframe", "1D"))
        poll_interval = int(self.cfg.get("pair_poll_interval_s", 60))

        log.info("pair_engine.started", symbol_a=sym_a, symbol_b=sym_b,
                 strategy=self.strategy.name)

        last_ts: Optional[datetime] = None

        while self._running:
            try:
                bar_a, bar_b = await self._fetch_bars(sym_a, sym_b, tf)
                if bar_a is None or bar_b is None:
                    await asyncio.sleep(poll_interval)
                    continue

                # Nur neue Bars verarbeiten
                if last_ts and bar_a.timestamp <= last_ts:
                    await asyncio.sleep(poll_interval)
                    continue
                last_ts = bar_a.timestamp

                await self.run_bar(bar_a, bar_b)

            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                log.error("pair_engine.error", error=str(e))

            await asyncio.sleep(poll_interval)

        log.info("pair_engine.stopped")

    async def run_bar(self, bar_a: Bar, bar_b: Bar) -> Optional[PairSignal]:
        """Verarbeite ein synchrones Bar-Paar."""
        self.ctx.set_now(bar_a.timestamp)
        self.broker.update_price(bar_a.symbol, bar_a.close)
        self.broker.update_price(bar_b.symbol, bar_b.close)

        snapshot = self.ctx.snapshot()
        signal = self.strategy._generate_pair_signal(bar_a, bar_b, snapshot)

        if signal.action == "HOLD":
            return signal

        # ML-Filter
        if not self.ml_filter.passes(signal):
            log.info("pair_engine.ml_filter_rejected",
                     z_score=signal.z_score,
                     action=signal.action)
            return signal

        if signal.action == "ENTER" and not self._has_position:
            await self._execute_enter(signal)
        elif signal.action == "EXIT" and self._has_position:
            await self._execute_exit(signal)

        return signal

    async def _execute_enter(self, signal: PairSignal) -> None:
        acct = await self.broker.get_account()
        equity = float(acct["equity"])

        long_r, short_r = await self.broker.execute_pair_signal(signal, equity)
        if long_r is None:
            log.warning("pair_engine.enter_failed",
                        long=signal.long_symbol, short=signal.short_symbol)
            return

        self._has_position = True

        # Beide Legs im TradeManager registrieren
        for leg, side in [(long_r, "long"), (short_r, "short")]:
            self.tm.register(ManagedTrade(
                symbol=leg["symbol"],
                side=side,
                entry=0.0,
                stop=0.0,
                target=None,
                qty=float(leg["qty"]),
                strategy_id=self.strategy.name,
                opened_at=signal.timestamp,
                metadata={"pair": True, "z_score": signal.z_score},
            ))

        log.info("pair_engine.entered",
                 long=signal.long_symbol, short=signal.short_symbol,
                 z_score=signal.z_score, qty=long_r["qty"])

    async def _execute_exit(self, signal: PairSignal) -> None:
        # Beide Positionen schließen
        for sym in [signal.long_symbol, signal.short_symbol]:
            try:
                await self.broker.close_position(sym)
            except Exception as e:  # noqa: BLE001
                log.warning("pair_engine.exit_leg_failed",
                            symbol=sym, error=str(e))
            self.tm.forget(sym)

        self._has_position = False
        log.info("pair_engine.exited",
                 long=signal.long_symbol, short=signal.short_symbol,
                 z_score=signal.z_score)

    async def _fetch_bars(
        self,
        sym_a: str,
        sym_b: str,
        tf: str,
    ) -> tuple[Optional[Bar], Optional[Bar]]:
        """Holt den jeweils letzten Bar für beide Symbole."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=5)

        try:
            df_a = await self.data.get_bars(sym_a, start, now, tf)
            df_b = await self.data.get_bars(sym_b, start, now, tf)
        except Exception as e:  # noqa: BLE001
            log.warning("pair_engine.fetch_failed", error=str(e))
            return None, None

        if df_a.empty or df_b.empty:
            return None, None

        def _last_bar(df, sym):
            row = df.iloc[-1]
            ts = df.index[-1]
            if hasattr(ts, "to_pydatetime"):
                ts = ts.to_pydatetime()
            return Bar(
                symbol=sym, timestamp=ts,
                open=float(row["Open"]), high=float(row["High"]),
                low=float(row["Low"]), close=float(row["Close"]),
                volume=int(row.get("Volume", 0) or 0),
            )

        return _last_bar(df_a, sym_a), _last_bar(df_b, sym_b)

    def stop(self) -> None:
        self._running = False

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
from live.state import PersistentState
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
        state: PersistentState,
        config: dict,
        health_state: Optional[HealthState] = None,
        bot_name: str = "",
    ):
        self.strategy = strategy
        self.broker = broker
        self.data = data_provider
        self._context = context
        self.ml_filter = ml_filter
        self.state = state
        self.cfg = config
        self.health: Optional[HealthState] = health_state
        self._bot_name: str = bot_name or strategy.name
        self._adapter_name = type(broker).__name__.replace("Adapter", "").lower()

        self.tm = TradeManager(
            use_trailing=False,
            eod_close_time=config.get("eod_close_time"),
        )

        self._running = False
        self._has_position = False
        self._pending_pair_submit: bool = False  # Pre-Submit-Guard gegen Doppel-Entry

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
                lag_ms = max(
                    0.0,
                    (datetime.now(timezone.utc) - bar_a.timestamp.astimezone(timezone.utc)).total_seconds() * 1000.0,
                ) if bar_a.timestamp.tzinfo is not None else max(
                    0.0,
                    (datetime.now(timezone.utc) - bar_a.timestamp.replace(tzinfo=timezone.utc)).total_seconds() * 1000.0,
                )

                if self.health is not None:
                    await self.health.set_last_bar(self.strategy.name, bar_a.timestamp, lag_ms)

                await self.state.upsert_bot_heartbeat(
                    bot_name=self._bot_name,
                    strategy=self.strategy.name,
                    last_bar_ts=bar_a.timestamp,
                    last_bar_lag_ms=lag_ms,
                    broker_connected=(
                        self.health.is_broker_connected() if self.health is not None else True
                    ),
                    broker_adapter=self._adapter_name,
                    circuit_breaker=(
                        self.health.is_circuit_breaker_active() if self.health is not None else False
                    ),
                    symbol_status=(
                        self.health.get_symbol_status(self.strategy.name)
                        if self.health is not None else {}
                    ),
                )

            except asyncio.CancelledError:
                self._running = False
                break
            except Exception as e:  # noqa: BLE001
                log.error("pair_engine.error", error=str(e))

            try:
                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                self._running = False
                break

        # Shutdown-Heartbeat via shield, damit er auch bei gecanceltem
        # Task zu Ende schreibt.
        try:
            await asyncio.shield(self.state.upsert_bot_heartbeat(
                bot_name=self._bot_name,
                strategy=self.strategy.name,
                broker_connected=False,
                circuit_breaker=False,
                broker_adapter=self._adapter_name,
            ))
        except (asyncio.CancelledError, Exception) as e:  # noqa: BLE001
            log.warning("pair_engine.heartbeat_shutdown_failed", error=str(e))

        log.info("pair_engine.stopped")

    async def run_bar(self, bar_a: Bar, bar_b: Bar) -> Optional[PairSignal]:
        """Verarbeite ein synchrones Bar-Paar."""
        self._context.set_now(bar_a.timestamp)
        self.broker.update_price(bar_a.symbol, bar_a.close)
        self.broker.update_price(bar_b.symbol, bar_b.close)

        # Reconcile: prüfe ob beide Legs noch konsistent sind
        await self._reconcile_pair()

        snapshot = self._context.snapshot()
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
        # Pre-Submit-Guard: verhindert Doppel-Entry (re-entrant oder parallel)
        if self._pending_pair_submit:
            log.info("pair_engine.enter_skipped_in_flight",
                     long=signal.long_symbol, short=signal.short_symbol)
            return

        # Broker-Position-Check: beide Legs dürfen nicht schon offen sein
        try:
            existing_long = await self.broker.get_position(signal.long_symbol)
            existing_short = await self.broker.get_position(signal.short_symbol)
        except Exception as e:  # noqa: BLE001
            log.warning("pair_engine.pre_submit_position_check_failed", error=str(e))
            existing_long = existing_short = None

        if existing_long is not None or existing_short is not None:
            log.info("pair_engine.enter_skipped_broker_open",
                     long=signal.long_symbol, short=signal.short_symbol,
                     long_exists=existing_long is not None,
                     short_exists=existing_short is not None)
            self._has_position = True  # Sync In-Memory-Flag mit Broker
            return

        self._pending_pair_submit = True
        try:
            acct = await self.broker.get_account()
            equity = float(acct["equity"])
            long_r, short_r = await self.broker.execute_pair_signal(signal, equity)
        except Exception as e:  # noqa: BLE001
            log.error("pair_engine.enter_failed", error=str(e),
                      long=signal.long_symbol, short=signal.short_symbol)
            return
        finally:
            self._pending_pair_submit = False

        if long_r is None:
            log.warning("pair_engine.enter_failed_null_result",
                        long=signal.long_symbol, short=signal.short_symbol)
            return

        self._has_position = True

        # Fill-Preise aus Broker holen (kurzes Warten auf Settlement)
        fill_prices: dict[str, float] = {}
        await asyncio.sleep(0.5)
        try:
            closes = await self.broker.get_recent_closes(
                [signal.long_symbol, signal.short_symbol]
            )
            for sym, ce in closes.items():
                fill_prices[sym.upper()] = float(ce.fill_price)
        except Exception as e:  # noqa: BLE001
            log.warning("pair_engine.fill_price_fetch_failed", error=str(e))

        group_name = f"pair_{signal.long_symbol}_{signal.short_symbol}"

        # Beide Legs persistent im TradeManager registrieren
        for leg, side in [(long_r, "long"), (short_r, "short")]:
            sym = leg["symbol"]
            entry_price = fill_prices.get(sym.upper(), 0.0)
            trade = ManagedTrade(
                symbol=sym,
                side=side,
                entry=entry_price,
                stop=0.0,
                target=None,
                qty=float(leg["qty"]),
                strategy_id=self.strategy.name,
                opened_at=signal.timestamp,
                metadata={
                    "pair": True,
                    "z_score": signal.z_score,
                    "reserve_group": group_name,
                    "bot_name": self._bot_name,
                },
            )
            try:
                await self.tm.register_and_persist(
                    trade,
                    signal=signal,
                    bot_name=self._bot_name,
                    broker_order_id=leg["id"],
                )
            except Exception as e:  # noqa: BLE001
                log.warning("pair_engine.register_persist_failed",
                            symbol=sym, error=str(e))
                self.tm.register(trade)  # Fallback: nur In-Memory

        log.info("pair_engine.entered",
                 long=signal.long_symbol, short=signal.short_symbol,
                 z_score=signal.z_score, qty=long_r["qty"],
                 long_fill=fill_prices.get(signal.long_symbol.upper(), 0.0),
                 short_fill=fill_prices.get(signal.short_symbol.upper(), 0.0))

    async def _execute_exit(self, signal: PairSignal) -> None:
        now = datetime.now(timezone.utc)
        symbols = [signal.long_symbol, signal.short_symbol]

        # Echte Fill-Preise holen, bevor Positionen geschlossen werden
        fill_prices: dict[str, float] = {}
        try:
            closes = await self.broker.get_recent_closes(symbols)
            for sym, ce in closes.items():
                fill_prices[sym.upper()] = float(ce.fill_price)
        except Exception as e:  # noqa: BLE001
            log.warning("pair_engine.exit_fill_fetch_failed", error=str(e))

        for sym in symbols:
            try:
                await self.broker.close_position(sym)
            except Exception as e:  # noqa: BLE001
                log.warning("pair_engine.exit_leg_failed",
                            symbol=sym, error=str(e))

        # Kurz warten, dann echte Exit-Fills holen
        await asyncio.sleep(0.5)
        exit_fills: dict[str, float] = {}
        try:
            closes = await self.broker.get_recent_closes(symbols)
            for sym, ce in closes.items():
                exit_fills[sym.upper()] = float(ce.fill_price)
        except Exception as e:  # noqa: BLE001
            log.warning("pair_engine.exit_fill_fetch2_failed", error=str(e))

        for sym in symbols:
            tracked = self.tm.get(sym)
            exit_price = exit_fills.get(sym.upper(), fill_prices.get(sym.upper(), 0.0))
            pnl: float = 0.0
            if tracked is not None and tracked.entry > 0 and exit_price > 0:
                direction = 1 if tracked.side == "long" else -1
                pnl = (exit_price - tracked.entry) * float(tracked.qty) * direction
            try:
                await self.tm.close_trade(
                    sym,
                    exit_price=exit_price,
                    exit_ts=now,
                    pnl=pnl,
                    reason=f"pair_exit z={signal.z_score:.2f}",
                    tracked=tracked,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("pair_engine.close_trade_failed",
                            symbol=sym, error=str(e))
                self.tm.forget(sym)

        self._has_position = False
        log.info("pair_engine.exited",
                 long=signal.long_symbol, short=signal.short_symbol,
                 z_score=signal.z_score,
                 long_exit=exit_fills.get(signal.long_symbol.upper(), 0.0),
                 short_exit=exit_fills.get(signal.short_symbol.upper(), 0.0))

    async def _reconcile_pair(self) -> None:
        """Prüft ob beide Pair-Legs noch im Broker stehen.

        Falls nur ein Leg fehlt → orphan_leg-AnomalyEvent + Versuch das
        verbleibende Leg ebenfalls zu schließen (kein silent Drift).
        """
        if not self._has_position:
            return
        sym_a = self.strategy.symbol_a
        sym_b = self.strategy.symbol_b
        try:
            pos_a = await self.broker.get_position(sym_a)
            pos_b = await self.broker.get_position(sym_b)
        except Exception as e:  # noqa: BLE001
            log.warning("pair_engine.reconcile_failed", error=str(e))
            return

        a_missing = pos_a is None and self.tm.get(sym_a) is not None
        b_missing = pos_b is None and self.tm.get(sym_b) is not None

        if not a_missing and not b_missing:
            return  # beide Legs konsistent

        missing = [s for s, m in [(sym_a, a_missing), (sym_b, b_missing)] if m]
        orphan = [s for s, m in [(sym_a, not a_missing), (sym_b, not b_missing)]
                  if m and self.tm.get(s) is not None]

        log.error("pair_engine.orphan_leg_detected",
                  missing=missing, orphan=orphan,
                  long=sym_a, short=sym_b,
                  check="pair_leg_orphan", severity="CRITICAL")

        # Verbleibendes Leg schließen, damit keine Einzel-Exposure hängt
        now = datetime.now(timezone.utc)
        for sym in orphan:
            try:
                await self.broker.close_position(sym)
            except Exception as e:  # noqa: BLE001
                log.warning("pair_engine.orphan_close_failed",
                            symbol=sym, error=str(e))
            tracked = self.tm.get(sym)
            try:
                await self.tm.close_trade(
                    sym,
                    exit_price=tracked.entry if tracked else 0.0,
                    exit_ts=now,
                    pnl=0.0,
                    reason="UNKNOWN (pair_leg_orphan)",
                    tracked=tracked,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("pair_engine.orphan_close_trade_failed",
                            symbol=sym, error=str(e))
                self.tm.forget(sym)

        self._has_position = False

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

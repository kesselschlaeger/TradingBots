"""LiveRunner – asyncio Event-Loop für den Production-Betrieb.

Verantwortlichkeiten:
  - Scheduler starten (premarket/open/eod/postmarket)
  - Bars per Polling oder Stream empfangen
  - Strategie aufrufen → Signale → Broker ausführen
  - TradeManager: Trailing + EOD-Close
  - State persistieren (aiosqlite)
  - Telegram-Benachrichtigungen
"""
from __future__ import annotations

import asyncio
import json
import signal as signal_mod
import time as _time
from dataclasses import asdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from core.context import MarketContextService, set_context_service
from core.logging import get_logger
from core.models import AlertLevel, Bar, OrderRequest, OrderSide, Signal
from core.trade_manager import ManagedTrade, TradeManager
from data.providers.base import DataProvider
from execution.port import BrokerPort
from live.anomaly import AnomalyDetector
from live.health import HealthState
from live.metrics import MetricsCollector
from live.notifier import TelegramNotifier
from live.scanner import PremarketScanner
from live.scheduler import TradingScheduler
from live.state import PersistentState
from strategy.base import BaseStrategy

log = get_logger(__name__)
ET_TZ = ZoneInfo("America/New_York")


class LiveRunner:
    """Production Event-Loop: Scheduler → DataStream → Strategy → Broker."""

    def __init__(
        self,
        strategy: BaseStrategy,
        broker: BrokerPort,
        data_provider: DataProvider,
        context: MarketContextService,
        state: PersistentState,
        notifier: TelegramNotifier,
        symbols: list[str],
        config: dict,
        health_state: Optional[HealthState] = None,
        metrics_collector: Optional[Any] = None,
        anomaly_detector: Optional[AnomalyDetector] = None,
        alerts_cfg: Any = None,
    ):
        self.strategy = strategy
        self.broker = broker
        self.data = data_provider
        self.ctx = context
        self.state = state
        self.notifier = notifier
        self.symbols = [s.upper() for s in symbols]
        self.cfg = config

        # Monitoring (alle optional – Runner laeuft auch ohne)
        self.health: Optional[HealthState] = health_state
        self.metrics = metrics_collector or MetricsCollector.create(enabled=False)
        self.anomaly: Optional[AnomalyDetector] = anomaly_detector
        self.alerts_cfg = alerts_cfg

        self.tm = TradeManager(
            trail_after_r=float(config.get("trail_after_r", 1.0)),
            trail_distance_r=float(config.get("trail_distance_r", 0.6)),
            use_trailing=bool(config.get("use_trailing", False)),
            eod_close_time=config.get("eod_close_time"),
            state=state,
        )

        self._scanner: Optional[PremarketScanner] = None
        self._scheduler: Optional[TradingScheduler] = None
        self._running = False
        self._pending_exit_next_open: set[str] = set()
        self._daily_trades_count: int = 0
        self._last_health_status: Optional[str] = None
        self._last_alert_ts: float = 0.0
        self._alert_cooldown_s = 60
        self._status_sink: Optional[callable] = None

        # Symbol-Status-Reporting: Strategie meldet pro-Symbol-Status
        # (WAIT_ORB, GAP_BLOCK, ...) sync in HealthState.
        if self.health is not None:
            if hasattr(self.strategy, "set_status_sink"):
                strat_name = self.strategy.name
                log.info("runner.status_sink_installed", strategy=strat_name)
                def _status_sink(sym: str, code: str, reason: str = "") -> None:
                    try:
                        if self.health is not None:
                            self.health.set_symbol_status(strat_name, sym, code, reason)
                            log.debug("runner.symbol_status_recorded", strategy=strat_name, symbol=sym, code=code, reason=reason)
                    except Exception as e:
                        log.error("runner.symbol_status_error", error=str(e), strategy=strat_name, symbol=sym, code=code)
                self._status_sink = _status_sink
                self.strategy.set_status_sink(_status_sink)
            else:
                log.warning("runner.strategy_no_set_status_sink", strategy_type=type(self.strategy).__name__)
        else:
            log.info("runner.no_health_state")

        set_context_service(context)

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.state.ensure_schema()
        self._running = True

        # Account-Snapshot laden
        acct = await self.broker.get_account()
        equity0 = float(acct["equity"])
        self.ctx.update_account(
            equity=equity0,
            cash=float(acct.get("cash", 0)),
            buying_power=float(acct.get("buying_power", 0)),
        )
        peak0 = await self.state.update_peak_equity(equity0)
        dd0 = 0.0 if peak0 <= 0 else (equity0 - peak0) / peak0 * 100.0
        await self.state.save_equity_snapshot(
            strategy=self.strategy.name,
            ts=datetime.now(timezone.utc),
            equity=equity0,
            cash=float(acct.get("cash", 0)),
            drawdown_pct=dd0,
            peak_equity=peak0,
        )
        if self.health is not None:
            await self.health.set_broker_status(
                connected=True,
                adapter=type(self.broker).__name__,
            )

        # Scheduler aufbauen (None-Zeiten → Job wird übersprungen)
        premarket_t = self.cfg.get("premarket_time", time(9, 0))
        market_open_t = self.cfg.get("market_open_time", time(9, 30))
        eod_close_t = self.cfg.get("eod_close_time", time(15, 27))
        post_market_t = self.cfg.get("post_market_time", time(16, 5))

        self._scheduler = TradingScheduler()
        self._scheduler.schedule_trading_day(
            premarket_scan=self._on_premarket if premarket_t else None,
            on_market_open=self._on_market_open if market_open_t else None,
            on_eod_close=self._on_eod_close if eod_close_t else None,
            on_post_market=self._on_post_market if post_market_t else None,
            premarket_time=premarket_t or time(9, 0),
            market_open_time=market_open_t or time(9, 30),
            eod_close_time=eod_close_t or time(15, 27),
            post_market_time=post_market_t or time(16, 5),
        )
        self._scheduler.start()

        log.info("runner.started", symbols=self.symbols,
                 strategy=self.strategy.name,
                 broker=self.broker.__class__.__name__)

        # Historische Session-Bars in den Strategie-Buffer spielen, bevor
        # Live-Bars eintreffen. Ohne Warmup könnte die Strategie z. B.
        # ORB-Levels (Opening-Range 09:30–09:50 ET) nie berechnen, wenn der
        # Runner nach Session-Open startet, weil stream_bars/_polling nur
        # neue Bars liefern.
        try:
            await self._warmup()
        except Exception as e:  # noqa: BLE001
            log.warning("runner.warmup_failed", error=str(e))

        # Graceful Shutdown via SIGINT / SIGTERM
        loop = asyncio.get_event_loop()
        for sig in (signal_mod.SIGINT, signal_mod.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
            except NotImplementedError:
                pass

        # Hauptloop: Bars streamen/pollt
        try:
            await self._bar_loop()
        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info("runner.interrupted")
        finally:
            await self.stop()

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._scheduler:
            self._scheduler.stop()
        log.info("runner.stopping")
        try:
            await self.notifier.send("*Bot stopped*")
        except Exception as e:  # noqa: BLE001
            log.warning("runner.notifier_close_error", error=str(e))
        close_fn = getattr(self.data, "close", None)
        if close_fn is not None:
            try:
                await close_fn()
            except Exception as e:  # noqa: BLE001
                log.warning("runner.data_close_error", error=str(e))

    # ── Scheduled Callbacks ───────────────────────────────────────────

    async def _on_premarket(self) -> None:
        log.info("runner.premarket_scan")
        if self._scanner is None:
            self._scanner = PremarketScanner(
                watchlist=self.symbols,
                min_gap_pct=float(self.cfg.get("scanner_min_gap", 0.02)),
                max_gap_pct=float(self.cfg.get("scanner_max_gap", 0.10)),
                min_premarket_vol=int(self.cfg.get("scanner_min_vol", 50_000)),
            )
        results = await self._scanner.scan_filtered(max_results=10)
        if results:
            msg = "*Premarket Gaps*\n" + "\n".join(
                f"`{r.symbol}` {r.gap_pct:+.1%} (vol {r.premarket_volume:,})"
                for r in results
            )
            await self.notifier.send(msg)
            # Dynamische Symbolliste erweitern (optional, je nach Config)
            if self.cfg.get("auto_add_scanned", False):
                for r in results:
                    if r.symbol not in self.symbols:
                        self.symbols.append(r.symbol)
                        log.info("runner.symbol_added", symbol=r.symbol)

    async def _on_market_open(self) -> None:
        log.info("runner.market_open")

        now_et = datetime.now(ET_TZ).time()
        exit_time = self.cfg.get("obb_exit_open_time", time(9, 15))
        if isinstance(exit_time, str):
            hh, mm = exit_time.split(":")
            exit_time = time(int(hh), int(mm))
        if now_et < exit_time:
            log.info("runner.exit_next_open_waiting", now=str(now_et),
                     exit_after=str(exit_time))
            return

        # Positionen mit exit_next_open schließen (OBB-Pattern)
        remaining_pending: set[str] = set()
        exit_order_type = str(self.cfg.get("obb_exit_order_type", "market"))
        exit_tif = str(self.cfg.get("obb_exit_time_in_force", "opg")).lower()

        for sym in list(self._pending_exit_next_open):
            pos = await self.broker.get_position(sym)
            if pos is None:
                continue
            close_side = OrderSide.SELL if pos.side == "long" else OrderSide.BUY
            qty = max(1, int(round(pos.qty)))
            ok = False
            try:
                await self.broker.submit_order(OrderRequest(
                    symbol=sym,
                    side=close_side,
                    qty=qty,
                    order_type=exit_order_type,
                    time_in_force=exit_tif,
                ))
                ok = True
            except Exception as e:  # noqa: BLE001
                log.warning("runner.exit_next_open_failed", symbol=sym,
                            error=str(e), order_type=exit_order_type,
                            time_in_force=exit_tif)

            if ok:
                log.info("runner.exit_next_open", symbol=sym)
                closes = await self.broker.get_recent_closes([sym])
                close_exec = closes.get(sym.upper()) or closes.get(sym)
                await self.notifier.send(
                    f"*Exit-Next-Open* `{sym}`\n"
                    f"Order: {exit_order_type.upper()} / {exit_tif.upper()}\n"
                    "Reason: OBB overnight exit at next market open"
                )
                exit_price = (
                    float(close_exec.fill_price)
                    if close_exec and close_exec.fill_price > 0
                    else self._safe_exit_price(
                        pos.current_price,
                        pos.entry_price,
                    )
                )
                qty_closed = (
                    float(close_exec.qty)
                    if close_exec and close_exec.qty > 0
                    else float(qty)
                )
                pnl = (
                    float(close_exec.realized_pnl)
                    if close_exec and close_exec.realized_pnl is not None
                    else self._compute_pnl(
                        side=pos.side,
                        entry=float(pos.entry_price),
                        exit_price=exit_price,
                        qty=qty_closed,
                    )
                )
                await self.notifier.trade_closed(
                    symbol=sym,
                    side=pos.side,
                    exit_price=exit_price,
                    pnl=pnl,
                    reason="Exit-Next-Open",
                    qty=qty_closed,
                    order_id=(close_exec.order_id if close_exec else ""),
                )
                await self.tm.close_trade(
                    sym, exit_price=exit_price,
                    exit_ts=datetime.now(timezone.utc),
                    pnl=pnl, reason="Exit-Next-Open",
                )
            else:
                remaining_pending.add(sym)
        self._pending_exit_next_open = remaining_pending

        if not self._pending_exit_next_open and bool(
            self.cfg.get("obb_stop_after_open_exit", False)
        ):
            log.info("runner.stopping_after_open_exit")
            await self.stop()
            return

        self.strategy.reset()
        self.tm.reset()
        self.ctx.clear_reserved_groups()
        await self.state.reset_day(date.today(), strategy=self.strategy.name)
        self._daily_trades_count = 0

        # Reserved Groups aus State wiederherstellen (falls Restart mitten am Tag)
        for g in await self.state.reserved_groups(date.today(),
                                                  strategy=self.strategy.name):
            self.ctx.reserve_group(g)

    async def _on_eod_close(self) -> None:
        log.info("runner.eod_close")
        positions_before = await self.broker.get_positions()
        result = await self.broker.close_all_positions()
        attempted = list(result.get("attempted", []))
        remaining = set(result.get("remaining", []))
        close_execs = await self.broker.get_recent_closes(attempted)

        for sym in attempted:
            if sym in remaining:
                continue
            pos = positions_before.get(sym)
            tracked = self.tm.get(sym)
            close_exec = close_execs.get(sym.upper()) or close_execs.get(sym)

            side = pos.side if pos else (tracked.side if tracked else "long")
            exit_price = (
                float(close_exec.fill_price)
                if close_exec and close_exec.fill_price > 0
                else self._safe_exit_price(
                    pos.current_price if pos else None,
                    tracked.entry if tracked else None,
                )
            )
            qty = (
                float(close_exec.qty)
                if close_exec and close_exec.qty > 0
                else (float(pos.qty) if pos else (float(tracked.qty) if tracked else None))
            )
            pnl = (
                float(close_exec.realized_pnl)
                if close_exec and close_exec.realized_pnl is not None
                else self._compute_pnl(
                    side=side,
                    entry=float(tracked.entry) if tracked else exit_price,
                    exit_price=exit_price,
                    qty=float(qty or 0.0),
                )
            )
            await self.notifier.trade_closed(
                symbol=sym,
                side=side,
                exit_price=exit_price,
                pnl=pnl,
                reason="EOD close all",
                qty=qty,
                order_id=(close_exec.order_id if close_exec else ""),
            )
            await self.tm.close_trade(
                sym, exit_price=exit_price,
                exit_ts=datetime.now(timezone.utc),
                pnl=pnl, reason="EOD close all",
            )

        if result.get("remaining"):
            await self.notifier.error("eod_close",
                                      f"Remaining: {result['remaining']}")
        else:
            log.info("runner.eod_closed", attempted=result.get("attempted", []))

    async def _on_post_market(self) -> None:
        log.info("runner.post_market")
        acct = await self.broker.get_account()
        equity = float(acct["equity"])
        pnl = await self.state.daily_pnl(date.today())
        trades = await self.state.trades_today(date.today())
        await self.notifier.daily_summary(
            day=date.today().isoformat(),
            pnl=pnl,
            trades=sum(trades.values()),
            equity=equity,
        )

    # ── Warmup (Historische Session-Bars) ────────────────────────────

    async def _warmup(self) -> None:
        """Lade historische Bars und SPY-Tageshistorie in den Context.

        Aufruf einmalig vor ``_bar_loop``. Liest die letzten ``warmup_days``
        per ``DataProvider.get_bars_bulk`` und spielt sie chronologisch in
        den Strategie-Buffer (über ``strategy.warmup_bar``) – ohne dabei
        Signale zu erzeugen oder Orders auszulösen.

        SPY (oder ``benchmark``) wird zusätzlich daily-aggregiert in den
        ``MarketContextService`` gelegt, damit der Trend-Filter live
        identisch zum Backtest arbeitet.
        """
        tf = str(self.cfg.get("timeframe", "5Min"))
        is_daily = "day" in tf.lower() or "1d" in tf.lower()
        warmup_days = int(self.cfg.get(
            "warmup_days", 60 if is_daily else 5,
        ))

        end = datetime.now(timezone.utc)
        start = end - timedelta(days=max(1, warmup_days))

        log.info("runner.warmup_start", symbols=self.symbols,
                 start=start.isoformat(), end=end.isoformat(),
                 timeframe=tf, days=warmup_days)

        try:
            data = await self.data.get_bars_bulk(
                self.symbols, start, end, tf,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("runner.warmup_bulk_failed", error=str(e))
            return

        # SPY/Benchmark fuer Trend-Filter laden (Daily-aggregiert)
        benchmark = str(self.cfg.get("benchmark", "SPY")).upper()
        spy_df = data.get(benchmark)
        if spy_df is None or spy_df.empty:
            try:
                spy_df = await self.data.get_bars(
                    benchmark, start, end, tf,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("runner.warmup_spy_failed",
                            benchmark=benchmark, error=str(e))
                spy_df = None
        if spy_df is not None and not spy_df.empty:
            try:
                from core.indicators import ensure_daily
                self.ctx.set_spy_df(ensure_daily(spy_df))
                log.info("runner.warmup_spy_loaded",
                         benchmark=benchmark, bars=len(spy_df))
            except Exception as e:  # noqa: BLE001
                log.warning("runner.warmup_spy_set_failed", error=str(e))

        # Strategie-Buffer befuellen (Bars chronologisch)
        if not hasattr(self.strategy, "warmup_bar"):
            log.warning("runner.warmup_no_warmup_bar",
                        strategy=type(self.strategy).__name__)
            return

        entries: list[tuple[datetime, str, int]] = []
        sym_dfs: dict[str, Any] = {}
        for sym, df in data.items():
            if df is None or df.empty:
                continue
            sym_dfs[sym] = df
            for i, ts in enumerate(df.index):
                py_ts = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                if py_ts.tzinfo is None:
                    py_ts = py_ts.replace(tzinfo=timezone.utc)
                entries.append((py_ts, sym, i))
        entries.sort(key=lambda x: x[0])

        for py_ts, sym, idx in entries:
            row = sym_dfs[sym].iloc[idx]
            bar = Bar(
                symbol=sym,
                timestamp=py_ts,
                open=float(row["Open"]),
                high=float(row["High"]),
                low=float(row["Low"]),
                close=float(row["Close"]),
                volume=int(row.get("Volume", 0) or 0),
            )
            self.strategy.warmup_bar(bar)

        if entries:
            self.ctx.set_now(entries[-1][0])

        log.info("runner.warmup_done",
                 bars_loaded=len(entries),
                 symbols_loaded=list(sym_dfs.keys()))

    # ── Bar-Loop (Polling-Fallback) ──────────────────────────────────

    async def _bar_loop(self) -> None:
        """Polling-Modus: holt periodisch neue Bars über DataProvider."""
        tf = str(self.cfg.get("timeframe", "5Min"))

        try:
            async for bar in self.data.stream_bars(self.symbols, tf):
                if not self._running:
                    break
                await self._process_bar(bar)
        except NotImplementedError:
            log.info("runner.polling_mode", interval_s=30)
            await self._polling_fallback(tf)

    async def _polling_fallback(self, tf: str) -> None:
        last_seen: dict[str, datetime] = {}
        is_daily = "day" in tf.lower() or "1d" in tf.lower()

        while self._running:
            now = datetime.now(timezone.utc)
            start = now - timedelta(days=5) if is_daily else now - timedelta(minutes=60)

            for sym in self.symbols:
                try:
                    df = await self.data.get_bars(sym, start, now, tf)
                except Exception as e:  # noqa: BLE001
                    log.warning("runner.poll_error", symbol=sym, error=str(e))
                    if self._status_sink:
                        self._status_sink(sym, "POLL_ERROR", str(e))
                    continue

                if df.empty:
                    if self._status_sink:
                        self._status_sink(sym, "NO_DATA", "keine Bars verfügbar")
                    continue

                # Alle Bars seit letzter Sichtung verarbeiten – nicht nur
                # den letzten. Sonst gehen Bars verloren, wenn das Poll-
                # Intervall größer ist als das Bar-Intervall oder wenn
                # eine kurze Verbindungsstörung aufgetreten ist.
                cutoff = last_seen.get(sym)
                if cutoff is not None:
                    df = df[df.index > cutoff]
                if df.empty:
                    continue

                for ts, row in df.iterrows():
                    py_ts = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                    if py_ts.tzinfo is None:
                        py_ts = py_ts.replace(tzinfo=timezone.utc)
                    bar = Bar(
                        symbol=sym, timestamp=py_ts,
                        open=float(row["Open"]), high=float(row["High"]),
                        low=float(row["Low"]), close=float(row["Close"]),
                        volume=int(row.get("Volume", 0) or 0),
                    )
                    await self._process_bar(bar)
                    last_seen[sym] = py_ts

            await asyncio.sleep(int(self.cfg.get("poll_interval_s", 30)))

    # ── Bar-Processing ────────────────────────────────────────────────

    async def _process_bar(self, bar: Bar) -> None:
        self.ctx.set_now(bar.timestamp)

        # ── Monitoring: Bar-Lag + Heartbeat ──
        lag_ms = 0.0
        bar_ts = bar.timestamp
        if bar_ts is not None:
            if bar_ts.tzinfo is None:
                bar_ts = bar_ts.replace(tzinfo=timezone.utc)
            lag_ms = max(0.0, (datetime.now(timezone.utc) - bar_ts).total_seconds() * 1000.0)
        if self.health is not None:
            await self.health.set_last_bar(self.strategy.name, bar_ts, lag_ms)
            # Alert bei fehlenden Bars während Handelszeiten
            if self.health.should_alert_on_not_ready():
                now_ts = _time.time()
                if now_ts - self._last_alert_ts > self._alert_cooldown_s:
                    await self.notifier.send_readiness(
                        "🚨 *Readiness Alert* – Keine aktuellen Kurse während "
                        f"Handelszeiten. Health-Status: {self.health.overall_status()}"
                    )
                    self._last_alert_ts = now_ts
                    log.warning("runner.health_alert_sent", reason="bar_stream_failure")
        self.metrics.set_bar_lag(self.strategy.name, lag_ms)
        if self.anomaly is not None:
            await self.anomaly.check_heartbeat(self.strategy.name, bar_ts)

        # Marktpreis für PaperAdapter (No-Op bei echten Brokern)
        self.broker.update_price(bar.symbol, bar.close)

        # Trailing auf bestehende Trades
        new_stop = self.tm.on_price(bar.symbol, bar.close)
        if new_stop is not None:
            log.info("runner.trailing_update", symbol=bar.symbol,
                     new_stop=new_stop)

        # EOD-Check
        if self.tm.should_eod_close(bar.timestamp):
            await self._on_eod_close()
            return

        # Reconcile mit Broker (SL/TP könnte serverseitig gefüllt sein)
        tracked_before = {
            symbol: self.tm.get(symbol)
            for symbol in self.tm.all_symbols()
        }
        broker_positions = await self.broker.get_positions()
        self.ctx.set_open_symbols(list(broker_positions.keys()))

        closed_symbols = [
            symbol for symbol in tracked_before
            if symbol not in broker_positions
        ]
        close_execs = await self.broker.get_recent_closes(closed_symbols)
        for sym in closed_symbols:
            tracked = tracked_before.get(sym)
            if tracked is None:
                continue
            close_exec = close_execs.get(sym.upper()) or close_execs.get(sym)
            exit_price = (
                float(close_exec.fill_price)
                if close_exec and close_exec.fill_price > 0
                else self._safe_exit_price(
                    bar.close if bar.symbol == sym else None,
                    tracked.entry,
                )
            )
            reason = self._infer_close_reason(tracked, bar)
            pnl = self._compute_pnl(
                side=tracked.side,
                entry=tracked.entry,
                exit_price=exit_price,
                qty=(
                    float(close_exec.qty)
                    if close_exec and close_exec.qty > 0
                    else tracked.qty
                ),
            )
            if close_exec and close_exec.realized_pnl is not None:
                pnl = float(close_exec.realized_pnl)
            await self.notifier.trade_closed(
                symbol=sym,
                side=tracked.side,
                exit_price=exit_price,
                pnl=pnl,
                reason=reason,
                qty=(
                    float(close_exec.qty)
                    if close_exec and close_exec.qty > 0
                    else tracked.qty
                ),
                order_id=(close_exec.order_id if close_exec else ""),
            )
            await self.tm.close_trade(
                sym, exit_price=exit_price,
                exit_ts=datetime.now(timezone.utc),
                pnl=pnl, reason=reason, tracked=tracked,
            )

        # Stale-Cleanup für Trades, deren Close der Runner nicht explizit
        # verarbeitet hat (z.B. gar kein Eintrag in tracked_before).
        self.tm.reconcile_with_broker(broker_positions)

        # Strategie
        signals = self.strategy.on_bar(bar)
        for sig in signals:
            await self._execute_signal(sig)

        # Account-Update + Monitoring
        acct = await self.broker.get_account()
        equity = float(acct["equity"])
        cash = float(acct.get("cash", 0))
        self.ctx.update_account(
            equity=equity,
            cash=cash,
            buying_power=float(acct.get("buying_power", 0)),
        )

        # Drawdown + Circuit-Breaker-Alerts
        peak = await self.state.update_peak_equity(equity)
        dd_pct = 0.0 if peak <= 0 else (equity - peak) / peak * 100.0
        positions_count = len(self.ctx.open_symbols)
        self.metrics.set_equity(self.strategy.name, equity)
        self.metrics.set_drawdown(self.strategy.name, dd_pct)
        self.metrics.set_open_positions(self.strategy.name, positions_count)
        if self.health is not None:
            await self.health.update_portfolio(
                equity=equity, cash=cash, drawdown_pct=dd_pct,
                open_positions=positions_count, peak_equity=peak,
            )
        await self._check_drawdown_alerts(dd_pct)

        # Equity-Snapshot + Live-Positions-Spiegelung (Dashboard-Feed)
        unrealized_total = 0.0
        for sym, pos in broker_positions.items():
            upnl = float(getattr(pos, "unrealized_pnl", 0.0) or 0.0)
            unrealized_total += upnl
            managed = self.tm.get(sym)
            held_min: Optional[int] = None
            if managed is not None and managed.opened_at is not None:
                try:
                    now_ts = bar.timestamp
                    open_ts = managed.opened_at
                    if now_ts.tzinfo is None:
                        now_ts = now_ts.replace(tzinfo=timezone.utc)
                    if open_ts.tzinfo is None:
                        open_ts = open_ts.replace(tzinfo=timezone.utc)
                    held_min = max(0, int((now_ts - open_ts).total_seconds() // 60))
                except Exception:  # noqa: BLE001
                    held_min = None
            try:
                await self.state.update_or_create_position(
                    strategy=self.strategy.name,
                    symbol=sym,
                    side=pos.side,
                    entry_price=float(pos.entry_price),
                    qty=float(pos.qty),
                    stop_price=(managed.current_stop if managed else None),
                    current_price=float(pos.current_price or bar.close),
                    unrealized_pnl=upnl,
                    unrealized_pnl_pct=(
                        upnl / (float(pos.entry_price) * float(pos.qty)) * 100.0
                        if pos.entry_price and pos.qty else None
                    ),
                    held_minutes=held_min,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("runner.position_persist_failed",
                            symbol=sym, error=str(e))
        try:
            await self.state.save_equity_snapshot(
                strategy=self.strategy.name,
                ts=bar.timestamp,
                equity=equity,
                cash=cash,
                drawdown_pct=dd_pct,
                peak_equity=peak,
                unrealized_pnl_total=unrealized_total,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("runner.equity_persist_failed", error=str(e))

    async def _check_drawdown_alerts(self, dd_pct: float) -> None:
        """Feuert Drawdown-Warn/Critical + Circuit-Breaker, wenn Schwellen
        aus ``alerts_cfg`` ueberschritten werden."""
        if self.alerts_cfg is None:
            return
        warn = float(getattr(self.alerts_cfg, "drawdown_warning_pct", -10.0))
        crit = float(getattr(self.alerts_cfg, "drawdown_critical_pct", -15.0))

        if dd_pct <= crit:
            if self.health is not None:
                await self.health.set_circuit_breaker(True)
            self.metrics.set_circuit_breaker(self.strategy.name, True)
            await self.notifier.alert(
                level=AlertLevel.CRITICAL, event="circuit_break",
                rate_limit_key="circuit_break", threshold=abs(crit),
            )
        elif dd_pct <= warn:
            await self.notifier.alert(
                level=AlertLevel.WARNING, event="drawdown_warn",
                rate_limit_key="drawdown_warn",
                drawdown=dd_pct, threshold=abs(warn),
            )
        else:
            if self.health is not None:
                await self.health.set_circuit_breaker(False)
            self.metrics.set_circuit_breaker(self.strategy.name, False)

    async def _execute_signal(self, sig: Signal) -> None:
        if sig.direction == 0:
            return

        # Anomaly-Check vor Ausfuehrung
        if self.anomaly is not None:
            events = await self.anomaly.check_signal(sig)
            if self.anomaly.should_block(events):
                log.warning("runner.signal_blocked",
                            symbol=sig.symbol, strategy=sig.strategy,
                            reason="duplicate_hard_block")
                self.metrics.record_signal(
                    sig.strategy,
                    action=("LONG" if sig.direction > 0 else "SHORT"),
                    filtered_by="duplicate_guard",
                )
                if self.health is not None:
                    await self.health.record_signal(sig.strategy, filtered=True)
                return

        # ── Core-Limits (gelten für alle Strategien) ──────────────────
        max_daily = int(self.cfg.get("max_daily_trades", 0))
        if max_daily > 0 and self._daily_trades_count >= max_daily:
            log.info("runner.max_daily_trades_reached",
                     symbol=sig.symbol,
                     trades_today=self._daily_trades_count,
                     limit=max_daily)
            return

        max_concurrent = int(self.cfg.get("max_concurrent_positions", 0))
        if max_concurrent > 0 and len(self.ctx.open_symbols) >= max_concurrent:
            log.info("runner.max_concurrent_positions_reached",
                     symbol=sig.symbol,
                     open_positions=len(self.ctx.open_symbols),
                     limit=max_concurrent)
            return

        if sig.metadata.get("exit_next_open"):
            ts = sig.timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            ts_et = ts.astimezone(ET_TZ)
            now_et = ts_et.time()

            entry_cutoff = self.cfg.get("obb_entry_cutoff_time", time(15, 59))
            market_close = self.cfg.get("obb_close_time", time(16, 0))
            if isinstance(entry_cutoff, str):
                hh, mm = entry_cutoff.split(":")
                entry_cutoff = time(int(hh), int(mm))
            if isinstance(market_close, str):
                hh, mm = market_close.split(":")
                market_close = time(int(hh), int(mm))

            if now_et >= market_close:
                log.info("runner.obb_entry_skipped_after_close", symbol=sig.symbol,
                         signal_time=str(now_et), close_time=str(market_close))
                return
            if now_et > entry_cutoff:
                log.info("runner.obb_entry_skipped_after_cutoff", symbol=sig.symbol,
                         signal_time=str(now_et), cutoff_time=str(entry_cutoff))
                return

            meta = dict(sig.metadata)
            meta.setdefault("order_type",
                            str(self.cfg.get("obb_entry_order_type", "market")))
            meta.setdefault(
                "time_in_force",
                str(self.cfg.get("obb_entry_time_in_force", "cls")).lower(),
            )
            sig = Signal(
                strategy=sig.strategy,
                symbol=sig.symbol,
                direction=sig.direction,
                strength=sig.strength,
                stop_price=sig.stop_price,
                target_price=sig.target_price,
                timestamp=sig.timestamp,
                metadata=meta,
            )

        acct = await self.broker.get_account()
        equity = float(acct["equity"])

        _t0 = datetime.now(timezone.utc)
        execution = await self.broker.execute_signal(
            sig, account_equity=equity,
            risk_pct=float(self.cfg.get("risk_pct", 0.01)),
            max_equity_at_risk=float(self.cfg.get("max_equity_at_risk", 0.05)),
            max_position_value_pct=float(self.cfg.get("max_position_value_pct", 0.25)),
        )
        if not execution:
            self.metrics.record_signal(
                sig.strategy,
                action=("LONG" if sig.direction > 0 else "SHORT"),
                filtered_by="broker_reject",
            )
            if self.health is not None:
                await self.health.record_signal(sig.strategy, filtered=True)
            return
        self._daily_trades_count += 1

        # Monitoring: Signal + Order-Latenz + Trade
        latency_ms = max(0.0, (datetime.now(timezone.utc) - _t0).total_seconds() * 1000.0)
        broker_name = type(self.broker).__name__
        self.metrics.record_order_latency(broker_name, latency_ms)
        side_str = "LONG" if sig.direction > 0 else "SHORT"
        self.metrics.record_signal(sig.strategy, action=side_str, filtered_by="")
        self.metrics.record_trade_opened(sig.strategy, sig.symbol, side_str)
        if self.health is not None:
            await self.health.record_signal(sig.strategy, filtered=False)
            await self.health.set_broker_status(
                connected=True, adapter=broker_name, last_order_ms=latency_ms,
            )

        side = "long" if sig.direction > 0 else "short"
        entry = float(sig.metadata.get("entry_price", 0.0))
        stop = float(sig.stop_price or 0.0)
        target = float(sig.target_price) if sig.target_price else None

        # TradeManager + zentrale DB-Persistenz
        managed = ManagedTrade(
            symbol=sig.symbol, side=side, entry=entry,
            stop=stop, target=target,
            qty=float(execution.qty),
            strategy_id=sig.strategy,
            opened_at=sig.timestamp,
            metadata=dict(sig.metadata),
        )
        await self.tm.register_and_persist(managed, sig)

        # Signal zur probabilistischen Auswertung spiegeln
        try:
            feat_json = (
                json.dumps(asdict(sig.features), default=str)
                if sig.features is not None else None
            )
        except Exception:  # noqa: BLE001
            feat_json = None
        try:
            await self.state.save_signal(
                strategy=sig.strategy,
                symbol=sig.symbol,
                ts=sig.timestamp,
                action=side_str,
                strength=float(sig.strength or 0.0),
                filtered_by="",
                mit_passed=True,
                ev_value=sig.metadata.get("ev_estimate"),
                features_json=feat_json,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("runner.persist_signal_failed",
                        symbol=sig.symbol, error=str(e))

        # Position am nächsten Open schließen (exit_next_open-Flag)
        if sig.metadata.get("exit_next_open"):
            self._pending_exit_next_open.add(sig.symbol)

        # Context: Reserve Group
        group = sig.metadata.get("reserve_group")
        if group:
            self.ctx.reserve_group(group)
            day_key = (sig.timestamp.date()
                       if hasattr(sig.timestamp, "date") else date.today())
            await self.state.reserve_group(group, day_key,
                                           strategy=self.strategy.name)

        # Notification
        await self.notifier.trade_opened(
            sig.symbol,
            side,
            managed.qty,
            entry,
            stop,
            target,
            reason=str(sig.metadata.get("reason", "")),
            order_id=execution.order_id,
            order_type=execution.order_type,
            time_in_force=execution.time_in_force,
        )
        log.info("runner.signal_executed", symbol=sig.symbol,
                 side=side, order_id=execution.order_id,
                 reason=sig.metadata.get("reason", ""))

    @staticmethod
    def _safe_exit_price(
        primary: Optional[float],
        fallback: Optional[float],
    ) -> float:
        if primary is not None and primary > 0:
            return float(primary)
        if fallback is not None and fallback > 0:
            return float(fallback)
        return 0.0

    @staticmethod
    def _compute_pnl(side: str, entry: float, exit_price: float, qty: float) -> float:
        if qty <= 0:
            return 0.0
        if side == "long":
            return (exit_price - entry) * qty
        return (entry - exit_price) * qty

    @staticmethod
    def _infer_close_reason(trade: ManagedTrade, bar: Bar) -> str:
        if trade.side == "long":
            if trade.current_stop > 0 and bar.low <= trade.current_stop:
                return "STOP (server/bracket)"
            if trade.target is not None and bar.high >= trade.target:
                return "TARGET (server/bracket)"
        else:
            if trade.current_stop > 0 and bar.high >= trade.current_stop:
                return "STOP (server/bracket)"
            if trade.target is not None and bar.low <= trade.target:
                return "TARGET (server/bracket)"
        return "Position closed at broker"

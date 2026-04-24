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
from dataclasses import asdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from core.context import MarketContextService
from core.logging import get_logger
from core.models import AlertLevel, Bar, OrderRequest, OrderSide, Signal
from core.trade_manager import ManagedTrade, TradeManager
from data.providers.base import DataProvider
from execution.port import BrokerPort
from live.anomaly import AnomalyDetector
from core.models import AnomalyEvent
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
        monitoring_cfg: Any = None,
        execution_cfg: Any = None,
        bot_name: str = "",
    ):
        self.strategy = strategy
        self.broker = broker
        self.data = data_provider
        self._context = context
        self.state = state
        self._adapter_name = type(broker).__name__.replace("Adapter", "").lower()
        self.notifier = notifier
        self.symbols = [s.upper() for s in symbols]
        self.cfg = config
        self._bot_name: str = bot_name or strategy.name

        # Monitoring (alle optional – Runner laeuft auch ohne)
        self.health: Optional[HealthState] = health_state
        self.metrics = metrics_collector or MetricsCollector.create(enabled=False)
        self.anomaly: Optional[AnomalyDetector] = anomaly_detector
        self.alerts_cfg = alerts_cfg
        self.monitoring_cfg = monitoring_cfg

        self.tm = TradeManager(
            trail_after_r=float(config.get("trail_after_r", 1.0)),
            trail_distance_r=float(config.get("trail_distance_r", 0.6)),
            use_trailing=bool(config.get("use_trailing", False)),
            eod_close_time=config.get("eod_close_time"),
            state=state,
            bot_name=self._bot_name,
        )

        self._scanner: Optional[PremarketScanner] = None
        self._scheduler: Optional[TradingScheduler] = None
        self._running = False
        self._pending_exit_next_open: set[str] = set()
        self._daily_trades_count: int = 0
        # Submit-Idempotenz: Symbole, für die aktuell eine Order-Submission
        # läuft. Verhindert Race-Conditions zwischen parallel verarbeiteten
        # Bars (async execute_signal → submit_order).
        self._pending_submit: set[str] = set()
        # Orphan-Close-Tracking: wenn Broker eine Position nicht mehr meldet,
        # aber kein passender Fill vorliegt, geben wir dem System Zeit, den
        # Fill nachzureichen, bevor wir den Trade im Zweifel als "UNKNOWN"
        # schließen. Key: Symbol → Zeitpunkt der ersten fehlenden Sichtung.
        self._orphan_close_since: dict[str, datetime] = {}
        # Execution-Config (aus AppConfig.execution) – steuert Reconcile-
        # Hardening und Orphan-Close-Timeout. Fallback auf Defaults, wenn
        # kein Block übergeben wurde (z.B. Tests).
        self._execution_cfg = execution_cfg
        self._close_verification_timeout_s: float = float(
            getattr(execution_cfg, "close_verification_timeout_s", 120.0)
            if execution_cfg is not None else 120.0
        )
        self._reconcile_require_healthy_session: bool = bool(
            getattr(execution_cfg, "reconcile_require_healthy_session", True)
            if execution_cfg is not None else True
        )
        self._last_health_status: Optional[str] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._watchdog_interval_s: int = int(
            getattr(self.monitoring_cfg, "watchdog_interval_s", 15) if self.monitoring_cfg is not None else 15
        )
        self._health_reminder_min: int = int(
            getattr(self.monitoring_cfg, "reminder_interval_min", 30) if self.monitoring_cfg is not None else 30
        )
        self._status_sink: Optional[callable] = None
        # Lock verhindert concurrent Ausführung von _on_eod_close (Scheduler +
        # Bar-Level-Check können simultan feuern). Locked() → laufender Call
        # wird übersprungen statt zu warten; so bleibt der Bar-Loop frei.
        self._eod_close_lock = asyncio.Lock()
        self._eod_close_done: bool = False
        # Letzter Warmup-Bar pro Symbol – Bars bis einschließlich dieses
        # Timestamps wurden bereits still in den Buffer geladen und dürfen
        # keine Signale auslösen (verhindert Doppelverarbeitung beim ersten
        # stream_bars-Zyklus, der den gleichen 30-min-Fenster fetcht).
        self._warmup_last_seen: dict[str, datetime] = {}

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

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.state.ensure_schema()
        self._running = True

        # Account-Snapshot laden
        acct = await self.broker.get_account()
        equity0 = float(acct["equity"])
        self._context.update_account(
            equity=equity0,
            cash=float(acct.get("cash", 0)),
            buying_power=float(acct.get("buying_power", 0)),
        )
        peak0 = await self.state.update_peak_equity(self._bot_name, self.strategy.name, equity0)
        dd0 = 0.0 if peak0 <= 0 else (equity0 - peak0) / peak0 * 100.0
        await self.state.save_equity_snapshot(
            bot_name=self._bot_name,
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

        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

        # Hauptloop: Bars streamen/pollt. Shutdown via main.py –
        # dort wird der Main-Task bei SIGINT/SIGTERM gecancelt, was
        # hier als CancelledError ankommt. ``shield`` stellt sicher,
        # dass ``self.stop()`` im finally-Block zu Ende läuft, auch
        # wenn der Task selbst gerade gecancelt wird.
        try:
            await self._bar_loop()
        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info("runner.interrupted")
        finally:
            await asyncio.shield(self.stop())

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False

        try:
            await self.state.upsert_bot_heartbeat(
                bot_name=self._bot_name,
                strategy=self.strategy.name,
                broker_connected=False,
                circuit_breaker=False,
                broker_adapter=self._adapter_name,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("runner.heartbeat_shutdown_failed", error=str(e))

        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except (asyncio.CancelledError, Exception):
                pass

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
            preferred = "auto"
            premarket_hours = 4
            if self._execution_cfg is not None:
                preferred = str(getattr(self._execution_cfg, "scanner_provider", "auto"))
                premarket_hours = int(getattr(self._execution_cfg, "scanner_premarket_hours", 4))
            self._scanner = PremarketScanner(
                watchlist=self.symbols,
                min_gap_pct=float(self.cfg.get("scanner_min_gap", 0.02)),
                max_gap_pct=float(self.cfg.get("scanner_max_gap", 0.10)),
                min_premarket_vol=int(self.cfg.get("scanner_min_vol", 50_000)),
                data_provider=self.data,
                preferred_source=preferred,
                premarket_hours=premarket_hours,
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
        self._context.clear_reserved_groups()
        await self.state.reset_day(self._bot_name, self.strategy.name, date.today())
        self._daily_trades_count = 0
        self._eod_close_done = False

        # Reserved Groups aus State wiederherstellen (falls Restart mitten am Tag)
        for g in await self.state.reserved_groups(self._bot_name, self.strategy.name,
                                                   date.today()):
            self._context.reserve_group(g)

    async def _on_eod_close(self) -> None:
        if self._eod_close_done:
            log.debug("runner.eod_close_skipped", reason="already_done")
            return
        # Concurrent-Guard: APScheduler und Bar-Level-Check können beide um
        # 15:27 feuern. Wenn ein Call bereits läuft (Lock ist gehalten),
        # überspringen – der laufende Call schließt alle Positionen.
        if self._eod_close_lock.locked():
            log.debug("runner.eod_close_skipped", reason="already_running")
            return
        async with self._eod_close_lock:
            log.info("runner.eod_close")
            _all_positions = await self.broker.get_positions()
            # Multi-Bot: nur eigene Positionen schließen (close_all_positions()
            # würde den gesamten Account treffen – gefährlich bei gemeinsamem
            # IBKR-Paper-Account mit mehreren Bots).
            _own_set = {s.upper() for s in self.symbols} | {s.upper() for s in self.tm.all_symbols()}
            positions_before = {k: v for k, v in _all_positions.items()
                                if k.upper() in _own_set}
            # Eigene Positionen einzeln schließen statt close_all_positions()
            attempted_list: list[str] = list(positions_before.keys())
            for sym in attempted_list:
                try:
                    await self.broker.close_position(sym)
                except Exception as e:  # noqa: BLE001
                    log.warning("runner.eod_close_position_failed",
                                symbol=sym, error=str(e))
            await asyncio.sleep(3)
            _after = await self.broker.get_positions()
            remaining_set = {s for s in attempted_list if s in _after}
            result = {"attempted": attempted_list,
                      "remaining": list(remaining_set),
                      "ok": not remaining_set}
            attempted = attempted_list
            remaining = remaining_set
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
                log.info("runner.eod_close_retry", remaining=list(remaining_set))
            else:
                log.info("runner.eod_closed", attempted=result.get("attempted", []))
                self._eod_close_done = True

    async def _on_post_market(self) -> None:
        log.info("runner.post_market")
        acct = await self.broker.get_account()
        equity = float(acct["equity"])
        pnl = await self.state.daily_pnl(self._bot_name, self.strategy.name, date.today())
        trades = await self.state.trades_today(self._bot_name, self.strategy.name, date.today())
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

        # Asset-aware Trend-Referenz laden:
        # - equity ohne expliziten Ref-Asset: benchmark/SPY als Daily-DF
        # - futures/crypto oder expliziter Ref-Asset: Referenzsymbol-Bars in Context
        asset_class = str(self.cfg.get("asset_class", "equity")).lower()
        use_trend_filter = bool(self.cfg.get("use_trend_filter", False))
        explicit_ref = self.cfg.get("trend_reference_asset")
        if use_trend_filter or "benchmark" in self.cfg:
            if asset_class == "equity" and not explicit_ref:
                benchmark = str(self.cfg.get("benchmark", "SPY")).upper()

                # Für den EMA-Trend-Filter werden mindestens trend_ema_period
                # Daily-Bars benötigt. warmup_days (typisch 5) reicht nach
                # Resampling von 5-Min auf Daily nicht aus (5 Tage → 5 Bars,
                # EMA(20) nicht sinnvoll). Daher: immer mit Daily-Timeframe
                # und eigenem Lookback laden.
                ema_period = int(self.cfg.get("trend_ema_period", 20))
                spy_daily_days = max(warmup_days, ema_period + 15)
                spy_start = end - timedelta(days=spy_daily_days)

                spy_df = None
                try:
                    spy_df = await self.data.get_bars(
                        benchmark, spy_start, end, "1Day",
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning("runner.warmup_spy_failed",
                                benchmark=benchmark, error=str(e))

                if spy_df is not None and not spy_df.empty:
                    try:
                        from core.indicators import ensure_daily
                        self._context.set_spy_df(ensure_daily(spy_df))
                        log.info("runner.warmup_spy_loaded",
                                 benchmark=benchmark,
                                 daily_bars=len(spy_df),
                                 lookback_days=spy_daily_days)
                    except Exception as e:  # noqa: BLE001
                        log.warning("runner.warmup_spy_set_failed", error=str(e))
            else:
                ref_symbol = None
                if hasattr(self.strategy, "_trend_reference_asset_key"):
                    try:
                        ref_symbol = self.strategy._trend_reference_asset_key()
                    except Exception:  # noqa: BLE001
                        ref_symbol = None
                if ref_symbol and str(ref_symbol).upper() not in self.symbols:
                    try:
                        ref_df = await self.data.get_bars(
                            str(ref_symbol).upper(), start, end, tf,
                        )
                        if ref_df is not None and not ref_df.empty:
                            for ts, row in ref_df.iterrows():
                                py_ts = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                                if py_ts.tzinfo is None:
                                    py_ts = py_ts.replace(tzinfo=timezone.utc)
                                self._context.push_bar(Bar(
                                    symbol=str(ref_symbol).upper(),
                                    timestamp=py_ts,
                                    open=float(row["Open"]),
                                    high=float(row["High"]),
                                    low=float(row["Low"]),
                                    close=float(row["Close"]),
                                    volume=int(row.get("Volume", 0) or 0),
                                ))
                            log.info("runner.warmup_ref_loaded",
                                     reference_symbol=str(ref_symbol).upper(),
                                     bars=len(ref_df), timeframe=tf)
                    except Exception as e:  # noqa: BLE001
                        log.warning("runner.warmup_ref_failed",
                                    reference_symbol=str(ref_symbol).upper(),
                                    error=str(e))

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
            # Letzten Bar-Timestamp je Symbol merken – stream_bars-Zyklus
            # liefert denselben Zeitraum nochmals; diese Timestamps dienen
            # als Cutoff, damit kein Warmup-Bar Signal-Generierung auslöst.
            self._warmup_last_seen[sym] = py_ts

        if entries:
            self._context.set_now(entries[-1][0])

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
        # Warmup-Cutoffs als Startpunkt verwenden – so wird der Zeitraum
        # der letzten warmup_days nicht nochmals mit Signal-Generierung
        # verarbeitet.
        last_seen: dict[str, datetime] = dict(self._warmup_last_seen)
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
        # ── Warmup-Duplikat-Guard ─────────────────────────────────────
        # stream_bars startet mit leerem last_seen und fetcht im ersten
        # Zyklus denselben Zeitraum wie der Warmup. Bars bis zum letzten
        # Warmup-Timestamp würden sonst nochmals mit Signal-Generierung
        # verarbeitet und sofort eine Order auslösen (z. B. SPY als erstes
        # Symbol in der Liste). Nur Kontext/Preis updaten, kein on_bar.
        bar_ts_tz = bar.timestamp
        if bar_ts_tz.tzinfo is None:
            bar_ts_tz = bar_ts_tz.replace(tzinfo=timezone.utc)
        warmup_cut = self._warmup_last_seen.get(bar.symbol)
        if warmup_cut is not None and bar_ts_tz <= warmup_cut:
            self._context.set_now(bar.timestamp)
            self.broker.update_price(bar.symbol, bar.close)
            return

        self._context.set_now(bar.timestamp)

        # ── Monitoring: Bar-Lag + Heartbeat ──
        lag_ms = 0.0
        bar_ts = bar.timestamp
        if bar_ts is not None:
            if bar_ts.tzinfo is None:
                bar_ts = bar_ts.replace(tzinfo=timezone.utc)
            lag_ms = max(0.0, (datetime.now(timezone.utc) - bar_ts).total_seconds() * 1000.0)
        if self.health is not None:
            await self.health.set_last_bar(self.strategy.name, bar_ts, lag_ms)
        try:
            await self.state.upsert_bot_heartbeat(
                bot_name=self._bot_name,
                strategy=self.strategy.name,
                last_bar_ts=bar_ts,
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
        except Exception as e:  # noqa: BLE001
            log.warning("runner.heartbeat_persist_failed", error=str(e))
        self.metrics.set_bar_lag(self.strategy.name, lag_ms)
        await self._emit_health_alerts()

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
            try:
                acct = await self.broker.get_account()
                equity = float(acct["equity"])
                cash = float(acct.get("cash", 0))
                self._context.update_account(
                    equity=equity,
                    cash=cash,
                    buying_power=float(acct.get("buying_power", 0)),
                )
                peak = await self.state.update_peak_equity(
                    self._bot_name, self.strategy.name, equity,
                )
                dd_pct = 0.0 if peak <= 0 else (equity - peak) / peak * 100.0
                await self.state.save_equity_snapshot(
                    bot_name=self._bot_name,
                    strategy=self.strategy.name,
                    ts=bar.timestamp,
                    equity=equity,
                    cash=cash,
                    drawdown_pct=dd_pct,
                    peak_equity=peak,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("runner.eod_equity_snapshot_failed", error=str(e))
            return

        # Reconcile mit Broker (SL/TP könnte serverseitig gefüllt sein)
        tracked_before = {
            symbol: self.tm.get(symbol)
            for symbol in self.tm.all_symbols()
        }
        _all_positions = await self.broker.get_positions()
        # Multi-Bot auf gemeinsamem Account (z. B. IBKR Paper): nur eigene
        # Symbole berücksichtigen. Eigene Symbole = Symbolliste dieses Bots
        # + aktuell vom TradeManager verwaltete (für den Fall, dass ein Symbol
        # zur Laufzeit nicht mehr in self.symbols steht, aber noch offen ist).
        _own_set = {s.upper() for s in self.symbols} | {s.upper() for s in self.tm.all_symbols()}
        broker_positions = {k: v for k, v in _all_positions.items()
                            if k.upper() in _own_set}
        self._context.set_open_symbols(list(broker_positions.keys()))

        missing_symbols = [
            symbol for symbol in tracked_before
            if symbol not in broker_positions
        ]
        await self._reconcile_missing_positions(
            missing_symbols=missing_symbols,
            tracked_before=tracked_before,
            bar=bar,
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
        self._context.update_account(
            equity=equity,
            cash=cash,
            buying_power=float(acct.get("buying_power", 0)),
        )

        # Drawdown + Circuit-Breaker-Alerts
        peak = await self.state.update_peak_equity(self._bot_name, self.strategy.name, equity)
        dd_pct = 0.0 if peak <= 0 else (equity - peak) / peak * 100.0
        positions_count = len(self._context.open_symbols)
        self.metrics.set_equity(self.strategy.name, equity)
        self.metrics.set_drawdown(self.strategy.name, dd_pct)
        self.metrics.set_open_positions(self.strategy.name, positions_count)
        if self.health is not None:
            await self.health.update_portfolio(
                equity=equity, cash=cash, drawdown_pct=dd_pct,
                open_positions=positions_count, peak_equity=peak,
            )
        await self._check_drawdown_alerts(dd_pct)
        await self._emit_health_alerts()

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
                    bot_name=self._bot_name,
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
                    entry_signal=(
                        ("LONG" if managed.side == "long" else "SHORT")
                        if managed is not None else None
                    ),
                    entry_reason=(
                        str(managed.metadata.get("reason"))
                        if managed is not None and managed.metadata.get("reason")
                        else None
                    ),
                    broker_order_id=(
                        str(managed.metadata.get("broker_order_id"))
                        if managed is not None and managed.metadata.get("broker_order_id")
                        else None
                    ),
                    order_reference=(
                        str(managed.metadata.get("order_reference"))
                        if managed is not None and managed.metadata.get("order_reference")
                        else None
                    ),
                )
            except Exception as e:  # noqa: BLE001
                log.warning("runner.position_persist_failed",
                            symbol=sym, error=str(e))
        try:
            await self.state.save_equity_snapshot(
                bot_name=self._bot_name,
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

    async def _reconcile_missing_positions(
        self,
        *,
        missing_symbols: list[str],
        tracked_before: dict[str, Optional[ManagedTrade]],
        bar: Bar,
    ) -> None:
        """Behandle Trades, deren Position der Broker nicht mehr meldet.

        Order-Lifecycle-Invariante: Ein Trade wird **nur** dann in der DB
        geschlossen, wenn einer der folgenden Fälle zutrifft:

        1. Broker meldet einen echten Fill (``get_recent_closes``).
        2. Broker-Session ist gesund, der Fill fehlt aber länger als
           ``close_verification_timeout_s`` – dann wird der Trade als
           ``UNKNOWN (reconcile timeout)`` geschlossen und ein
           ``orphan_close``-Anomaly-Event eskaliert.

        Bei ungesunder Session (z. B. IBKR-Gateway disconnected) wird
        **kein** Auto-Close ausgeführt; das in der Vergangenheit genutzte
        ``_infer_close_reason`` ("TARGET (server/bracket)") war die Haupt-
        Ursache der Fantasie-Fills vom 2026-04-24 und entfällt damit als
        alleinige Quelle.
        """
        if not missing_symbols:
            # Alle getrackten Symbole weiterhin bei Broker sichtbar – Orphan-
            # Tracking zurücksetzen.
            self._orphan_close_since.clear()
            return

        health = await self.broker.health()
        session_healthy = bool(health.get("session_healthy", True))
        now = datetime.now(timezone.utc)

        close_execs: dict[str, Any] = {}
        try:
            close_execs = await self.broker.get_recent_closes(missing_symbols)
        except Exception as e:  # noqa: BLE001
            log.warning("runner.get_recent_closes_failed", error=str(e))
            close_execs = {}

        for sym in missing_symbols:
            tracked = tracked_before.get(sym)
            if tracked is None:
                continue

            close_exec = close_execs.get(sym.upper()) or close_execs.get(sym)

            if close_exec is not None:
                # ── Echter Fill liegt vor: Trade sauber schließen ──────
                self._orphan_close_since.pop(sym, None)
                exit_price = (
                    float(close_exec.fill_price)
                    if close_exec.fill_price > 0
                    else self._safe_exit_price(None, tracked.entry)
                )
                qty_closed = (
                    float(close_exec.qty)
                    if close_exec.qty > 0 else float(tracked.qty)
                )
                pnl = (
                    float(close_exec.realized_pnl)
                    if close_exec.realized_pnl is not None
                    else self._compute_pnl(
                        side=tracked.side,
                        entry=tracked.entry,
                        exit_price=exit_price,
                        qty=qty_closed,
                    )
                )
                reason = "Position closed at broker (fill verified)"
                await self.notifier.trade_closed(
                    symbol=sym,
                    side=tracked.side,
                    exit_price=exit_price,
                    pnl=pnl,
                    reason=reason,
                    qty=qty_closed,
                    order_id=close_exec.order_id or "",
                )
                await self.tm.close_trade(
                    sym, exit_price=exit_price,
                    exit_ts=now,
                    pnl=pnl, reason=reason, tracked=tracked,
                )
                continue

            # ── Kein Fill vorhanden ────────────────────────────────────
            if self._reconcile_require_healthy_session and not session_healthy:
                # Session ungesund → Position könnte noch existieren;
                # Auto-Close wäre ein Fantasie-Fill wie am 2026-04-24.
                log.warning(
                    "runner.reconcile_skipped_unhealthy",
                    symbol=sym,
                    last_error_code=health.get("last_error_code"),
                    last_error_msg=str(health.get("last_error_msg", ""))[:160],
                )
                continue

            first_seen = self._orphan_close_since.setdefault(sym, now)
            age_s = (now - first_seen).total_seconds()
            if age_s < self._close_verification_timeout_s:
                log.info(
                    "runner.orphan_close_pending",
                    symbol=sym,
                    age_s=int(age_s),
                    timeout_s=int(self._close_verification_timeout_s),
                )
                continue

            # ── Timeout überschritten: als UNKNOWN eskalieren ─────────
            exit_price = self._safe_exit_price(None, tracked.entry)
            pnl = self._compute_pnl(
                side=tracked.side,
                entry=tracked.entry,
                exit_price=exit_price,
                qty=float(tracked.qty),
            )
            reason = "UNKNOWN (reconcile timeout)"
            log.error(
                "runner.orphan_close_timeout",
                symbol=sym,
                age_s=int(age_s),
                timeout_s=int(self._close_verification_timeout_s),
            )
            try:
                if self.anomaly is not None:
                    ev = AnomalyEvent(
                        timestamp=now, check_name="orphan_close",
                        severity=AlertLevel.CRITICAL,
                        symbol=sym, strategy=self.strategy.name,
                        bot_name=self._bot_name,
                        message=(
                            f"Position {sym} seit {int(age_s)}s unsichtbar, "
                            f"kein Fill verfügbar → als UNKNOWN geschlossen"
                        ),
                        context={"age_s": age_s,
                                 "timeout_s": self._close_verification_timeout_s},
                    )
                    await self.anomaly._emit(ev)  # noqa: SLF001
            except Exception as e:  # noqa: BLE001
                log.warning("runner.orphan_close_emit_failed", error=str(e))

            await self.notifier.trade_closed(
                symbol=sym,
                side=tracked.side,
                exit_price=exit_price,
                pnl=pnl,
                reason=reason,
                qty=float(tracked.qty),
                order_id="",
            )
            await self.tm.close_trade(
                sym, exit_price=exit_price,
                exit_ts=now,
                pnl=pnl, reason=reason, tracked=tracked,
            )
            self._orphan_close_since.pop(sym, None)

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

    async def _watchdog_loop(self) -> None:
        while self._running:
            now = datetime.now(timezone.utc)
            try:
                if self.health is not None:
                    await self.health.set_watchdog(self.strategy.name, now)
                await self.state.upsert_bot_heartbeat(
                    bot_name=self._bot_name,
                    strategy=self.strategy.name,
                    last_watchdog_ts=now,
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
            except Exception as e:  # noqa: BLE001
                log.warning("runner.watchdog_persist_failed", error=str(e))
            await asyncio.sleep(max(1, int(self._watchdog_interval_s)))

    async def _emit_health_alerts(self) -> None:
        if self.health is None:
            return
        state = self.health.overall_state(self.strategy.name)
        await self.notifier.alert_health(
            "data_stale",
            level=AlertLevel.WARNING,
            bot_name=self._bot_name,
            strategy=self.strategy.name,
            check_name="data_stale",
            is_firing=(state == "DATA_STALE"),
            details=f"state={state}",
            reminder_interval_min=self._health_reminder_min,
        )
        await self.notifier.alert_health(
            "circuit_break",
            level=AlertLevel.CRITICAL,
            bot_name=self._bot_name,
            strategy=self.strategy.name,
            check_name="circuit_break",
            is_firing=(state == "CIRCUIT_BREAK"),
            details=f"state={state}",
            reminder_interval_min=self._health_reminder_min,
        )

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
                try:
                    await self.state.save_signal(
                        bot_name=self._bot_name,
                        strategy=sig.strategy,
                        symbol=sig.symbol,
                        ts=sig.timestamp,
                        action=("LONG" if sig.direction > 0 else "SHORT"),
                        strength=float(sig.strength or 0.0),
                        filtered_by="duplicate_guard",
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning("runner.persist_filtered_signal_failed",
                                symbol=sig.symbol, error=str(e))
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
        if max_concurrent > 0 and len(self._context.open_symbols) >= max_concurrent:
            log.info("runner.max_concurrent_positions_reached",
                     symbol=sig.symbol,
                     open_positions=len(self._context.open_symbols),
                     limit=max_concurrent)
            return

        # ── Submit-Idempotenz: keine doppelten Orders pro Symbol ──────
        # Drei Stufen, in dieser Reihenfolge:
        #   1. Aktuell laufende submit_order-Call für das Symbol?
        #   2. In-Memory ManagedTrade bereits offen?
        #   3. Broker-Seite: Position bereits existent?
        # Der Guard ist intentional *vor* duplicate_trade-Check platziert,
        # weil eine bestehende Position auch nach Ablauf des Duplicate-
        # Fensters keinen zweiten Entry rechtfertigt.
        if sig.symbol in self._pending_submit:
            log.info("runner.submit_skipped_in_flight", symbol=sig.symbol)
            return
        if self.tm.get(sig.symbol) is not None:
            log.info("runner.submit_skipped_tm_open", symbol=sig.symbol)
            return
        if not bool(sig.metadata.get("allow_scale_in", False)):
            existing_pos = None
            try:
                existing_pos = await self.broker.get_position(sig.symbol)
            except Exception as e:  # noqa: BLE001
                log.warning("runner.get_position_failed",
                            symbol=sig.symbol, error=str(e))
            if existing_pos is not None:
                log.info("runner.submit_skipped_broker_open",
                         symbol=sig.symbol,
                         qty=float(existing_pos.qty),
                         side=existing_pos.side)
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
        self._pending_submit.add(sig.symbol)
        try:
            execution = await self.broker.execute_signal(
                sig, account_equity=equity,
                risk_pct=float(self.cfg.get("risk_pct", 0.01)),
                max_equity_at_risk=float(self.cfg.get("max_equity_at_risk", 0.05)),
                max_position_value_pct=float(self.cfg.get("max_position_value_pct", 0.25)),
            )
        finally:
            self._pending_submit.discard(sig.symbol)
        if not execution:
            self.metrics.record_signal(
                sig.strategy,
                action=("LONG" if sig.direction > 0 else "SHORT"),
                filtered_by="broker_reject",
            )
            if self.health is not None:
                await self.health.record_signal(sig.strategy, filtered=True)
            try:
                await self.state.save_signal(
                    bot_name=self._bot_name,
                    strategy=sig.strategy,
                    symbol=sig.symbol,
                    ts=sig.timestamp,
                    action=("LONG" if sig.direction > 0 else "SHORT"),
                    strength=float(sig.strength or 0.0),
                    filtered_by="broker_reject",
                )
            except Exception as e:  # noqa: BLE001
                log.warning("runner.persist_filtered_signal_failed",
                            symbol=sig.symbol, error=str(e))
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
        signal_meta = dict(sig.metadata)
        signal_meta.setdefault("entry_signal", side_str)
        signal_meta.setdefault(
            "order_reference",
            signal_meta.get("client_order_id") or execution.order_id,
        )
        signal_meta.setdefault("broker_order_id", execution.order_id)

        # TradeManager + zentrale DB-Persistenz
        managed = ManagedTrade(
            symbol=sig.symbol, side=side, entry=entry,
            stop=stop, target=target,
            qty=float(execution.qty),
            strategy_id=sig.strategy,
            opened_at=sig.timestamp,
            metadata=signal_meta,
        )
        await self.tm.register_and_persist(
            managed,
            sig,
            bot_name=self._bot_name,
            broker_order_id=execution.order_id,
            order_reference=str(signal_meta.get("order_reference") or execution.order_id),
        )

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
                bot_name=self._bot_name,
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
            self._context.reserve_group(group)
            day_key = (sig.timestamp.date()
                       if hasattr(sig.timestamp, "date") else date.today())
            await self.state.reserve_group(
                bot_name=self._bot_name,
                strategy=self.strategy.name,
                group=group,
                day=day_key,
            )

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

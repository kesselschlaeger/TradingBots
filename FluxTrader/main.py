"""FluxTrader CLI Entrypoint.

Usage:
    python main.py live   --config configs/orb_live.yaml
    python main.py paper  --config configs/orb_paper.yaml
    python main.py backtest --config configs/orb_backtest.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import signal as signal_mod
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# FluxTrader muss im sys.path sein
sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.config import AppConfig, load_config, load_env
from core.context import MarketContextService, set_context_service
from core.logging import get_logger, setup_logging

log = get_logger(__name__)


def _resolve_bot_name(cfg: AppConfig) -> str:
    """Effektiver bot_name für PersistentState: YAML-Feld > ibkr_bot_id > Ableitung."""
    if cfg.bot_name.strip():
        return cfg.bot_name.strip()
    if cfg.broker.type == "ibkr" and cfg.broker.ibkr_bot_id.strip():
        return cfg.broker.ibkr_bot_id.strip()
    mode_suffix = "paper" if cfg.broker.paper else "live"
    return f"{cfg.strategy.name}_{cfg.broker.type}_{mode_suffix}"


def _resolve_notifier_bot_name(cfg: AppConfig) -> str:
    configured = cfg.notifications.bot_name.strip()
    if configured:
        return configured
    return _resolve_bot_name(cfg).upper()


# ─────────────────────────── Strategy-Factory ─────────────────────────────

def _build_strategy(cfg: AppConfig, ctx: MarketContextService):
    import strategy  # noqa: F401 – trigger @register
    from strategy.registry import StrategyRegistry
    return StrategyRegistry.get(cfg.strategy.name, cfg.strategy.params, context=ctx)


def _build_ml_filter(cfg: AppConfig):
    from core.ml_filter import build_ml_filter
    ml_cfg = getattr(cfg, "ml", None)
    if ml_cfg is None:
        ml_raw = cfg.model_extra.get("ml", {}) if cfg.model_extra else {}
    else:
        ml_raw = ml_cfg if isinstance(ml_cfg, dict) else {}
    return build_ml_filter(
        enabled=bool(ml_raw.get("enabled", False)),
        model_path=ml_raw.get("model_path"),
        threshold=float(ml_raw.get("threshold", 0.6)),
    )


# ─────────────────────────── Broker-Factory ───────────────────────────────

def _build_broker(cfg: AppConfig):
    env = load_env()
    bt = cfg.broker.type

    if bt == "paper":
        from execution.paper_adapter import PaperAdapter
        return PaperAdapter(initial_cash=cfg.initial_capital)

    if bt == "alpaca":
        from execution.alpaca_adapter import AlpacaAdapter
        return AlpacaAdapter(
            api_key=env.APCA_API_KEY_ID,
            secret_key=env.APCA_API_SECRET_KEY,
            paper=cfg.broker.paper,
        )

    if bt == "ibkr":
        from execution.ibkr_adapter import IBKRAdapter
        return IBKRAdapter(
            host=cfg.broker.ibkr_host,
            port=cfg.broker.ibkr_port,
            client_id=cfg.broker.ibkr_client_id,
            paper=cfg.broker.paper,
            bot_id=cfg.broker.ibkr_bot_id,
            order_confirm_timeout_s=float(cfg.execution.order_confirm_timeout_s),
        )

    if bt == "bybit":
        from execution.bybit_adapter import BybitAdapter
        return BybitAdapter(cfg)

    raise ValueError(f"Unknown broker type: {bt}")


# ─────────────────────────── Data-Factory ─────────────────────────────────

def _build_data_provider(cfg: AppConfig):
    env = load_env()

    if cfg.data.provider == "alpaca":
        from data.providers.alpaca_provider import AlpacaDataProvider
        return AlpacaDataProvider(
            api_key=env.APCA_API_KEY_ID,
            secret_key=env.APCA_API_SECRET_KEY,
            feed=cfg.broker.alpaca_data_feed,
        )

    if cfg.data.provider == "yfinance":
        from data.providers.yfinance_provider import YFinanceDataProvider
        return YFinanceDataProvider()

    if cfg.data.provider == "ibkr":
        from data.providers.ibkr_provider import IBKRDataProvider
        strategy_params = cfg.strategy.params or {}
        data_client_id = (
            cfg.data.ibkr_data_client_id
            if cfg.data.ibkr_data_client_id is not None
            else cfg.broker.ibkr_client_id + 100
        )
        return IBKRDataProvider(
            host=cfg.broker.ibkr_host,
            port=cfg.broker.ibkr_port,
            client_id=data_client_id,
            use_rth=bool(cfg.data.model_extra.get("ibkr_use_rth", True)),
            asset_class=str(strategy_params.get("asset_class", "equity")),
            contract_cfg=dict(strategy_params),
        )

    if cfg.data.provider == "bybit":
        from data.providers.bybit_provider import BybitDataProvider
        params = cfg.broker_params or {}
        return BybitDataProvider(
            testnet=bool(params.get("testnet", True)),
            api_key=str(params.get("api_key", "")),
            api_secret=str(params.get("api_secret", "")),
            category=str(params.get("category", "spot")),
        )

    raise ValueError(f"Unknown data provider: {cfg.data.provider}")


# ─────────────────────────── Commands ─────────────────────────────────────

async def cmd_backtest(cfg: AppConfig) -> None:
    from backtest.engine import BacktestConfig, BarByBarEngine
    from backtest.report import (
        build_exit_reason_stats,
        build_tearsheet,
        export_trades,
        format_exit_reason_stats,
        format_tearsheet,
    )
    from execution.paper_adapter import PaperAdapter

    ctx = MarketContextService(initial_capital=cfg.initial_capital)
    ctx.update_account(equity=cfg.initial_capital, cash=cfg.initial_capital,
                       buying_power=cfg.initial_capital * 4)
    set_context_service(ctx)

    strategy = _build_strategy(cfg, ctx)
    data_prov = _build_data_provider(cfg)
    paper = PaperAdapter(initial_cash=cfg.initial_capital,
                         slippage_pct=0.0002, commission_pct=0.00005)

    symbols = cfg.strategy.symbols
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=cfg.data.lookback_days)

    log.info("backtest.loading_data", symbols=symbols, start=str(start),
             end=str(end))
    data = await data_prov.get_bars_bulk(symbols, start, end,
                                         cfg.data.timeframe)
    log.info("backtest.data_loaded", symbols=list(data.keys()),
             total_bars=sum(len(d) for d in data.values()))

    # SPY für Trendfilter
    spy_df = None
    if cfg.benchmark in data:
        spy_df = data[cfg.benchmark]
    elif cfg.benchmark not in symbols:
        spy_df_raw = await data_prov.get_bars(cfg.benchmark, start, end,
                                               cfg.data.timeframe)
        if not spy_df_raw.empty:
            spy_df = spy_df_raw

    eod_str = cfg.strategy.params.get("eod_close_time")
    eod_time = eod_str if isinstance(eod_str, type(None)) is False else None

    bt_cfg = BacktestConfig(
        initial_capital=cfg.initial_capital,
        risk_pct=cfg.strategy.risk_pct,
        eod_close_time=eod_time if not isinstance(eod_time, str) else None,
    )
    engine = BarByBarEngine(strategy, paper, ctx, bt_cfg)
    result = await engine.run(data=data, spy_df=spy_df)

    ts = build_tearsheet(result.equity_curve, result.trades,
                         result.initial_capital)
    print("\n" + "=" * 50)
    print("BACKTEST RESULT")
    print("=" * 50)
    print(format_tearsheet(
        ts,
        start_ts=result.start_ts,
        end_ts=result.end_ts,
        strategy_name=result.strategy_name,
        allow_shorts=result.allow_shorts,
        mit_enabled=result.mit_enabled,
        enriched_trades=result.enriched_trades or None,
    ))
    print(f"Bars processed: {result.bars_processed}")
    print(f"Trades: {len(result.trades)}")

    # ── Exit-Reason-Statistik ────────────────────────────────────────
    exp = cfg.backtest_export
    if exp.show_exit_stats and result.enriched_trades:
        stats_df = build_exit_reason_stats(result.enriched_trades)
        if not stats_df.empty:
            print()
            print(format_exit_reason_stats(
                stats_df, total_trades=len(result.enriched_trades),
            ))

    # ── Trade-Export (CSV / Excel) ───────────────────────────────────
    if exp.export_trades != "none" and result.enriched_trades:
        ts_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"{cfg.strategy.name}_backtest_{ts_stamp}"
        paths = export_trades(
            result.enriched_trades,
            output_dir=Path(exp.export_dir),
            fmt=exp.export_trades,
            filename_base=base_name,
        )
        for p in paths:
            print(f"Export: {p}")


def cmd_wfo(cfg: AppConfig) -> None:
    """Walk-Forward-Optimierung (sync – WFO läuft CPU-bound ohne Event-Loop)."""
    from backtest.wfo import WalkForwardOptimizer, run_flux_backtest

    wfo_raw: dict = {}
    if cfg.model_extra:
        wfo_raw = cfg.model_extra.get("wfo", {}) or {}

    param_grid = dict(wfo_raw.get("param_grid") or {})
    if not param_grid:
        log.error("wfo.no_param_grid",
                  hint="configs/*.yaml muss 'wfo.param_grid' definieren")
        return

    is_days = int(wfo_raw.get("is_days", 120))
    oos_days = int(wfo_raw.get("oos_days", 30))
    step_days = int(wfo_raw.get("step_days", 20))
    metric = str(wfo_raw.get("metric", "sharpe"))
    min_trades_is = int(wfo_raw.get("min_trades_is", 20))
    n_workers = int(wfo_raw.get("n_workers", 0))

    symbols = cfg.strategy.symbols
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=cfg.data.lookback_days)

    async def _load_data():
        data_prov = _build_data_provider(cfg)
        data = await data_prov.get_bars_bulk(
            symbols, start, end, cfg.data.timeframe,
        )
        spy = None
        if cfg.benchmark in data:
            spy = data[cfg.benchmark]
        elif cfg.benchmark:
            raw = await data_prov.get_bars(
                cfg.benchmark, start, end, cfg.data.timeframe,
            )
            if not raw.empty:
                spy = raw
        return data, spy

    log.info("wfo.loading_data", symbols=symbols,
             start=str(start), end=str(end))
    data, spy_df = asyncio.run(_load_data())
    log.info("wfo.data_loaded",
             symbols=list(data.keys()),
             total_bars=sum(len(d) for d in data.values()))

    wfo = WalkForwardOptimizer(
        data_dict=data,
        vix_series=None,
        base_cfg=cfg,
        param_grid=param_grid,
        backtest_func=run_flux_backtest,
        is_days=is_days,
        oos_days=oos_days,
        step_days=step_days,
        metric=metric,
        min_trades_is=min_trades_is,
        spy_df=spy_df,
        n_workers=n_workers,
    )
    wfo.run()

    summary = wfo.summary_frame()
    stability = wfo.stability_report()

    print("\n" + "=" * 60)
    print("WFO SUMMARY")
    print("=" * 60)
    if summary.empty:
        print("(keine Fenster ausgewertet)")
    else:
        print(summary.to_string(index=False))

    if not stability.empty:
        print("\n" + "=" * 60)
        print("PARAMETER STABILITY")
        print("=" * 60)
        print(stability.to_string(index=False))

    oos_eq = wfo.combined_oos_equity()
    if not oos_eq.empty:
        total = (oos_eq.iloc[-1] - oos_eq.iloc[0]) / oos_eq.iloc[0] * 100.0
        print(f"\nKombinierte OOS-Gesamtrendite: {total:+.2f}%")


async def cmd_live(cfg: AppConfig) -> None:
    from live.anomaly import AnomalyDetector
    from live.health import HealthState, start_health_server
    from live.metrics import MetricsCollector
    from live.notifier import TelegramNotifier
    from live.runner import LiveRunner
    from live.state import PersistentState
    from strategy.base import PairStrategy

    env = load_env()
    ctx = MarketContextService(initial_capital=cfg.initial_capital)

    strategy = _build_strategy(cfg, ctx)
    broker = _build_broker(cfg)
    data_prov = _build_data_provider(cfg)
    ml_filter = _build_ml_filter(cfg)

    state = PersistentState(
        Path(cfg.persistence.data_dir) / cfg.persistence.state_db
    )
    # Zentrale DB vor Runner/Scheduler/Notifier bereitstellen – damit
    # Dashboard + Prometheus + AnomalyDetector sofort gegen das Schema
    # lesen/schreiben können und Concurrency (WAL) aktiv ist.
    await state.ensure_schema()
    notifier = TelegramNotifier(
        bot_token=env.TELEGRAM_TOKEN or cfg.notifications.telegram_token,
        chat_id=env.TELEGRAM_CHAT_ID or cfg.notifications.telegram_chat_id,
        health_bot_token=(
            env.TELEGRAM_HEALTH_TOKEN
            or cfg.notifications.health_telegram_token
        ),
        readiness_bot_token=(
            env.TELEGRAM_READINESS_TOKEN
            or cfg.notifications.readiness_telegram_token
        ),
        health_chat_id=(
            env.TELEGRAM_HEALTH_CHAT_ID
            or cfg.notifications.health_telegram_chat_id
        ),
        readiness_chat_id=(
            env.TELEGRAM_READINESS_CHAT_ID
            or cfg.notifications.readiness_telegram_chat_id
        ),
        enabled=cfg.notifications.enabled,
        bot_name=_resolve_notifier_bot_name(cfg),
        strategy_name=cfg.strategy.name,
        broker_name=(
            f"{cfg.broker.type}-paper" if cfg.broker.paper
            else f"{cfg.broker.type}-live"
        ),
        alerts_cfg=cfg.alerts,
    )

    # ── Monitoring-Infrastruktur ─────────────────────────────────────
    effective_bot_name = _resolve_bot_name(cfg)
    health_state = HealthState(
        strategy_config=cfg.strategy.params,
        monitoring_config=cfg.monitoring,
        persistent_state=state,
        bot_name=effective_bot_name,
        data_timeframe=str(cfg.data.timeframe or "5Min"),
    )
    metrics_collector = MetricsCollector.create(
        enabled=cfg.monitoring.prometheus_enabled
    )
    anomaly_detector = AnomalyDetector(notifier=notifier, state=state, cfg=cfg,
                                       bot_name=effective_bot_name)
    await health_state.set_broker_status(
        connected=False, adapter=cfg.broker.type,
    )
    monitoring_extra = cfg.monitoring.model_extra or {}
    fallback_ports = tuple(
        int(p)
        for p in (monitoring_extra.get("health_fallback_ports", []) or [])
    )
    health_task = asyncio.create_task(start_health_server(
        health_state=health_state,
        metrics_collector=metrics_collector,
        port=cfg.monitoring.health_port,
        fallback_ports=fallback_ports,
    ))

    log.info("live.starting", mode=cfg.mode,
             strategy=cfg.strategy.name,
             broker=cfg.broker.type,
             symbols=cfg.strategy.symbols,
             health_port=cfg.monitoring.health_port)

    try:
        # Pair-Pfad: PairEngine als separate Task
        if isinstance(strategy, PairStrategy):
            from live.pair_runner import PairEngine
            pair_engine = PairEngine(
                strategy=strategy,
                broker=broker,
                data_provider=data_prov,
                context=ctx,
                ml_filter=ml_filter,
                state=state,
                config=cfg.strategy.params,
                health_state=health_state,
                bot_name=effective_bot_name,
            )
            await pair_engine.run()
        else:
            # Standard-Pfad: LiveRunner
            runner = LiveRunner(
                strategy=strategy,
                broker=broker,
                data_provider=data_prov,
                context=ctx,
                state=state,
                notifier=notifier,
                symbols=cfg.strategy.symbols,
                config=cfg.strategy.params,
                health_state=health_state,
                metrics_collector=metrics_collector,
                anomaly_detector=anomaly_detector,
                alerts_cfg=cfg.alerts,
                monitoring_cfg=cfg.monitoring,
                execution_cfg=cfg.execution,
                bot_name=effective_bot_name,
                data_cfg=cfg.data,
            )
            await runner.start()
    finally:
        # Health-Server mit-cancellen, sonst hängt Shutdown auf Windows
        health_task.cancel()
        try:
            await health_task
        except (asyncio.CancelledError, Exception):
            pass


async def cmd_healthcheck(cfg: AppConfig) -> int:
    """Externer Dead-Man's-Switch: liest DB, bewertet Health, sendet Alerts."""
    from core.models import AlertLevel
    from live.health_eval import evaluate_liveness
    from live.notifier import TelegramNotifier
    from live.state import PersistentState

    env = load_env()
    state = PersistentState(Path(cfg.persistence.data_dir) / cfg.persistence.state_db)
    await state.ensure_schema()

    notifier = TelegramNotifier(
        bot_token=env.TELEGRAM_TOKEN or cfg.notifications.telegram_token,
        chat_id=env.TELEGRAM_CHAT_ID or cfg.notifications.telegram_chat_id,
        health_bot_token=(
            env.TELEGRAM_HEALTH_TOKEN
            or cfg.notifications.health_telegram_token
        ),
        readiness_bot_token=(
            env.TELEGRAM_READINESS_TOKEN
            or cfg.notifications.readiness_telegram_token
        ),
        health_chat_id=(
            env.TELEGRAM_HEALTH_CHAT_ID
            or cfg.notifications.health_telegram_chat_id
        ),
        readiness_chat_id=(
            env.TELEGRAM_READINESS_CHAT_ID
            or cfg.notifications.readiness_telegram_chat_id
        ),
        enabled=cfg.notifications.enabled,
        bot_name=_resolve_notifier_bot_name(cfg),
        strategy_name=cfg.strategy.name,
        broker_name="healthcheck",
        alerts_cfg=cfg.alerts,
    )

    rows = await state.get_liveness_view()
    now = datetime.now(timezone.utc)
    has_critical = False

    for row in rows:
        eval_row = evaluate_liveness(
            row=row,
            strategy_cfg=dict(cfg.strategy.params or {}),
            monitoring_cfg=cfg.monitoring,
            now=now,
        )
        overall = str(eval_row["overall_state"])
        bot_name = str(row.get("bot_name", ""))
        strategy = str(row.get("strategy", ""))

        if overall in {"PROCESS_DEAD", "DATA_STALE"}:
            has_critical = True

        await notifier.alert_health(
            "process_dead",
            level=AlertLevel.CRITICAL,
            bot_name=bot_name,
            strategy=strategy,
            check_name="process_dead",
            is_firing=(overall == "PROCESS_DEAD"),
            details=f"overall={overall}",
            reminder_interval_min=cfg.monitoring.reminder_interval_min,
        )
        await notifier.alert_health(
            "data_stale",
            level=AlertLevel.WARNING,
            bot_name=bot_name,
            strategy=strategy,
            check_name="data_stale",
            is_firing=(overall == "DATA_STALE"),
            details=(
                f"overall={overall}, next_expected={eval_row.get('next_expected_bar_at')}"
            ),
            reminder_interval_min=cfg.monitoring.reminder_interval_min,
        )

    return 1 if has_critical else 0


async def _run_with_shutdown(coro) -> None:
    """Startet ``coro`` als Task und cancelt ihn bei SIGINT/SIGTERM.

    Windows unterstützt ``loop.add_signal_handler`` nicht (ProactorEventLoop);
    dort fällt der Code auf ``signal.signal`` zurück. In beiden Fällen wird der
    Main-Task sauber gecancelt, sodass ``finally``-Blöcke (``runner.stop`` etc.)
    laufen können und die Shutdown-Kette greift.
    """
    loop = asyncio.get_running_loop()
    main_task = asyncio.create_task(coro)

    def _request_shutdown() -> None:
        if not main_task.done():
            log.info("main.shutdown_requested")
            main_task.cancel()

    try:
        loop.add_signal_handler(signal_mod.SIGINT, _request_shutdown)
        loop.add_signal_handler(signal_mod.SIGTERM, _request_shutdown)
    except NotImplementedError:
        # Windows: signal.signal() wird im Main-Thread aufgerufen
        signal_mod.signal(
            signal_mod.SIGINT,
            lambda *_: loop.call_soon_threadsafe(_request_shutdown),
        )

    try:
        await main_task
    except asyncio.CancelledError:
        log.info("main.cancelled")


def cmd_dashboard(cfg: AppConfig, port: int) -> None:
    """Startet das Web-Dashboard als eigenen Prozess (uvicorn).

    Laeuft NICHT im gleichen Event-Loop wie der LiveRunner – liest das
    SQLite-State read-only + optional den HTTP-Health-Endpunkt des
    Runners.
    """
    try:
        import uvicorn
    except ImportError as e:
        log.error("dashboard.missing_dep",
                  hint="pip install fastapi uvicorn", error=str(e))
        sys.exit(2)

    from dashboard.app import create_app

    app = create_app(cfg, health_state=None)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info", access_log=False)


# ─────────────────────────── CLI ──────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="FluxTrader")
    parser.add_argument(
        "command",
        choices=["live", "paper", "backtest", "wfo", "dashboard", "healthcheck"],
        help="Modus: live, paper, backtest, wfo, dashboard, healthcheck",
    )
    parser.add_argument("--config", "-c", required=True,
                        help="Pfad zur YAML-Config")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-json", action="store_true")
    parser.add_argument("--port", type=int, default=None,
                        help="Port fuer dashboard-Kommando (Default aus Config)")
    parser.add_argument("--export-trades", default=None,
                        choices=["csv", "excel", "both"],
                        help="Trade-Export-Format (nur backtest)")
    parser.add_argument("--export-dir", default=None,
                        help="Zielverzeichnis fuer Trade-Export")
    args = parser.parse_args()

    setup_logging(level=args.log_level.upper(), json_output=args.log_json)
    cfg = load_config(args.config)

    # CLI-Overrides für Backtest-Export
    if args.export_trades:
        cfg.backtest_export.export_trades = args.export_trades
    if args.export_dir:
        cfg.backtest_export.export_dir = Path(args.export_dir)

    if args.command in ("live", "paper"):
        asyncio.run(_run_with_shutdown(cmd_live(cfg)))
    elif args.command == "backtest":
        asyncio.run(_run_with_shutdown(cmd_backtest(cfg)))
    elif args.command == "wfo":
        cmd_wfo(cfg)
    elif args.command == "dashboard":
        port = args.port or cfg.monitoring.dashboard_port
        cmd_dashboard(cfg, port=port)
    elif args.command == "healthcheck":
        code = asyncio.run(cmd_healthcheck(cfg))
        sys.exit(int(code))


if __name__ == "__main__":
    main()

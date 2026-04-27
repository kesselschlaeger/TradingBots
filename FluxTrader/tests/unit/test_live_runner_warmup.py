"""Tests für LiveRunner Warmup-Logik und Fail-Fast bei unzureichendem Lookback.

Spec:
  1. QuickFlipStrategy.required_warmup_days() == QUICK_FLIP_REQUIRED_WARMUP_DAYS (25)
  2. Warmup-Days-Auflösung: params.warmup_days > data_cfg.lookback_days > Default
  3. LiveRunner._warmup() wirft RuntimeError wenn effective_warmup_days < required
  4. Per-Symbol: kein Bot-Abort (nur WARNING) wenn Symbol zu wenig Handelstage hat
  5. _on_market_open ruft _warmup() nach strategy.reset() auf
  6. BarByBarEngine.run() wirft RuntimeError bei unzureichendem Lookback
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock


def _arun(coro):
    """Führt eine Coroutine in einer isolierten Event-Loop aus.

    Verwendet new_event_loop() statt asyncio.run(), damit der Policy-Default-Loop
    nicht gelöscht wird. asyncio.run() setzt den Loop am Ende auf None; das
    bricht test_pair_engine.py, das asyncio.get_event_loop() nach unserem
    Test-Modul aufruft.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

import pandas as pd
import pytest
import pytz

from core.context import MarketContextService, reset_context_service, set_context_service
from strategy.quick_flip import QUICK_FLIP_REQUIRED_WARMUP_DAYS, QuickFlipStrategy

ET_TZ = pytz.timezone("America/New_York")


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _make_strategy(context: MarketContextService) -> QuickFlipStrategy:
    cfg = {
        "min_bars": 2,
        "max_bars_buffer": 500,
        "use_trend_filter": False,
        "use_gap_filter": False,
        "use_mit_overlay": False,
        "use_vix_filter": False,
    }
    return QuickFlipStrategy(cfg, context=context)


def _make_runner(
    strategy: QuickFlipStrategy,
    context: MarketContextService,
    warmup_days: int | None = None,
    data_lookback: int | None = None,
    symbols: list[str] | None = None,
    get_bars_bulk_return: dict | None = None,
):
    """LiveRunner mit vollständig gemockten Abhängigkeiten."""
    from live.runner import LiveRunner

    cfg: dict = {"obb_exit_open_time": time(0, 0)}  # Zeitcheck in _on_market_open immer bestanden
    if warmup_days is not None:
        cfg["warmup_days"] = warmup_days

    data_cfg = None
    if data_lookback is not None:
        data_cfg = MagicMock()
        data_cfg.lookback_days = data_lookback

    broker = AsyncMock()
    broker.get_account = AsyncMock(
        return_value={"equity": 100_000.0, "cash": 100_000.0, "buying_power": 200_000.0})
    broker.get_positions = AsyncMock(return_value={})
    broker.get_position = AsyncMock(return_value=None)

    data_provider = AsyncMock()
    data_provider.get_bars_bulk = AsyncMock(return_value=get_bars_bulk_return or {})
    data_provider.get_bars = AsyncMock(return_value=pd.DataFrame())

    state = AsyncMock()
    state.ensure_schema = AsyncMock()
    state.update_peak_equity = AsyncMock(return_value=100_000.0)
    state.save_equity_snapshot = AsyncMock()
    state.reset_day = AsyncMock()
    state.reserved_groups = AsyncMock(return_value=[])

    notifier = AsyncMock()

    return LiveRunner(
        strategy=strategy,
        broker=broker,
        data_provider=data_provider,
        context=context,
        state=state,
        notifier=notifier,
        symbols=symbols or ["AAPL"],
        config=cfg,
        data_cfg=data_cfg,
    )


def _make_df_with_span(calendar_days: int) -> pd.DataFrame:
    """OHLCV-DataFrame mit genau `calendar_days` Kalender-Tagen Spanne."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=calendar_days)
    idx = pd.date_range(start, end, freq="5min", tz="UTC")
    n = len(idx)
    return pd.DataFrame({
        "Open":   [100.0] * n,
        "High":   [101.0] * n,
        "Low":    [99.0]  * n,
        "Close":  [100.0] * n,
        "Volume": [10_000] * n,
    }, index=idx)


def _make_df_with_completed_trading_days(completed_days: int) -> pd.DataFrame:
    """DataFrame mit exakt `completed_days` abgeschlossenen ET-Handelstagen.

    Ein 'heute'-Bar wird hinzugefügt, damit heute nicht als completed gilt.
    completed_days liegen alle in der Vergangenheit (Mo–Fr, nicht heute).
    """
    now_utc = datetime.now(timezone.utc)
    timestamps: list[datetime] = []
    offset = 1
    found = 0
    while found < completed_days:
        ts = now_utc - timedelta(days=offset)
        if ts.weekday() < 5:  # Wochentag
            timestamps.append(ts.replace(hour=14, minute=30, second=0, microsecond=0))
            found += 1
        offset += 1
    # Heutiger Bar (nicht als completed gezählt)
    timestamps.append(now_utc.replace(hour=14, minute=30, second=0, microsecond=0))
    timestamps.sort()
    idx = pd.DatetimeIndex(timestamps, tz="UTC")
    n = len(idx)
    return pd.DataFrame({
        "Open":   [100.0] * n,
        "High":   [101.0] * n,
        "Low":    [99.0]  * n,
        "Close":  [100.0] * n,
        "Volume": [10_000] * n,
    }, index=idx)


# ── Test 1: required_warmup_days ─────────────────────────────────────────────

class TestRequiredWarmupDays:
    def test_quick_flip_returns_constant(self, context):
        """QuickFlipStrategy.required_warmup_days() == QUICK_FLIP_REQUIRED_WARMUP_DAYS."""
        strat = _make_strategy(context)
        assert strat.required_warmup_days() == QUICK_FLIP_REQUIRED_WARMUP_DAYS
        assert strat.required_warmup_days() == 25

    def test_base_strategy_default_is_none(self, context):
        """BaseStrategy ohne Override gibt None zurück (kein Constraint)."""
        from strategy.base import BaseStrategy

        class _Minimal(BaseStrategy):
            @property
            def name(self): return "minimal"
            def _generate_signals(self, bar): return []

        strat = _Minimal({}, context=context)
        assert strat.required_warmup_days() is None


# ── Test 2 + 3: Warmup-Days-Auflösung + Fail-Fast ───────────────────────────

class TestWarmupDaysResolution:
    def test_warmup_days_from_params_sufficient(self, context):
        """params.warmup_days=30 >= 25 → _warmup() wirft KEINE RuntimeError."""
        strat = _make_strategy(context)
        runner = _make_runner(strat, context, warmup_days=30)
        _arun(runner._warmup())   # darf nicht werfen

    def test_warmup_days_from_data_lookback(self, context):
        """Ohne params.warmup_days: data_cfg.lookback_days=30 wird verwendet → OK."""
        strat = _make_strategy(context)
        runner = _make_runner(strat, context, data_lookback=30)
        _arun(runner._warmup())   # darf nicht werfen

    def test_runner_aborts_on_short_warmup(self, context):
        """effective_warmup_days=5 < required=25 → RuntimeError (Fail-Fast)."""
        strat = _make_strategy(context)
        runner = _make_runner(strat, context, warmup_days=5)
        with pytest.raises(RuntimeError, match="warmup_days"):
            _arun(runner._warmup())

    def test_runner_aborts_when_data_lookback_insufficient(self, context):
        """data_cfg.lookback_days=10 < 25 → RuntimeError."""
        strat = _make_strategy(context)
        runner = _make_runner(strat, context, data_lookback=10)
        with pytest.raises(RuntimeError):
            _arun(runner._warmup())

    def test_params_warmup_takes_precedence_over_data_lookback(self, context):
        """params.warmup_days=30 hat Vorrang über data_lookback=5 (zu kurz)."""
        strat = _make_strategy(context)
        # data_lookback=5 wäre zu kurz, aber params.warmup_days=30 gewinnt → OK
        runner = _make_runner(strat, context, warmup_days=30, data_lookback=5)
        _arun(runner._warmup())   # darf nicht werfen


# ── Test 4: Per-Symbol-Datenprüfung (Warning, kein Abort) ───────────────────

class TestSymbolWarmupCheck:
    def test_thin_symbol_history_does_not_abort_bot(self, context):
        """Symbol mit nur 5 abgeschlossenen Tagen → kein RuntimeError, nur WARNING."""
        strat = _make_strategy(context)
        thin_df = _make_df_with_completed_trading_days(completed_days=5)
        runner = _make_runner(
            strat, context,
            warmup_days=25,
            get_bars_bulk_return={"AAPL": thin_df},
        )
        # Muss ohne RuntimeError durchlaufen (nur WARNING + AnomalyEvent, kein Abort)
        _arun(runner._warmup())

    def test_sufficient_symbol_history_no_issue(self, context):
        """Symbol mit 24 abgeschlossenen Tagen (>= required-1=24) → kein Fehler."""
        strat = _make_strategy(context)
        ok_df = _make_df_with_completed_trading_days(completed_days=24)
        runner = _make_runner(
            strat, context,
            warmup_days=25,
            get_bars_bulk_return={"AAPL": ok_df},
        )
        _arun(runner._warmup())   # darf nicht werfen

    def test_empty_symbol_df_is_skipped_gracefully(self, context):
        """Leerer DataFrame für ein Symbol → kein Crash."""
        strat = _make_strategy(context)
        runner = _make_runner(
            strat, context,
            warmup_days=25,
            get_bars_bulk_return={"AAPL": pd.DataFrame()},
        )
        _arun(runner._warmup())


# ── Test 5: _on_market_open ruft _warmup() nach reset() auf ─────────────────

class TestMarketOpenRewarm:
    def test_market_open_calls_warmup_after_reset(self, context):
        """_on_market_open(): strategy.reset() wird aufgerufen, danach _warmup()."""
        strat = _make_strategy(context)
        runner = _make_runner(strat, context, warmup_days=25)

        call_log: list[str] = []

        async def _mock_warmup() -> None:
            call_log.append("warmup")

        original_reset = strat.reset

        def _tracked_reset() -> None:
            call_log.append("reset")
            original_reset()

        runner._warmup = _mock_warmup
        strat.reset = _tracked_reset

        _arun(runner._on_market_open())

        assert "reset" in call_log, "strategy.reset() wurde nicht aufgerufen"
        assert "warmup" in call_log, "_warmup() wurde nicht aufgerufen"
        # Reset muss VOR Warmup kommen
        reset_idx = call_log.index("reset")
        warmup_idx = call_log.index("warmup")
        assert reset_idx < warmup_idx, "_warmup() muss NACH reset() aufgerufen werden"


# ── Test 6: BarByBarEngine Fail-Fast ─────────────────────────────────────────

class TestBacktestFailFast:
    def test_backtest_aborts_on_short_lookback(self, context):
        """BarByBarEngine.run() wirft RuntimeError wenn Dataspanne < required_warmup_days."""
        from backtest.engine import BacktestConfig, BarByBarEngine
        from execution.paper_adapter import PaperAdapter

        strat = _make_strategy(context)
        broker = PaperAdapter(initial_cash=100_000.0, slippage_pct=0.0, commission_pct=0.0)
        cfg = BacktestConfig(initial_capital=100_000.0)

        # 5 Kalendertage Span < 25 required
        df = _make_df_with_span(calendar_days=5)

        engine = BarByBarEngine(strategy=strat, broker=broker,
                                context=context, config=cfg)

        async def _run():
            await engine.run(data={"AAPL": df})

        with pytest.raises(RuntimeError, match="lookback insufficient"):
            _arun(_run())

    def test_backtest_passes_with_sufficient_lookback(self, context):
        """BarByBarEngine.run() läuft durch wenn Dataspanne >= required_warmup_days."""
        from backtest.engine import BacktestConfig, BarByBarEngine
        from execution.paper_adapter import PaperAdapter

        strat = _make_strategy(context)
        broker = PaperAdapter(initial_cash=100_000.0, slippage_pct=0.0, commission_pct=0.0)
        cfg = BacktestConfig(initial_capital=100_000.0)

        # 30 Kalendertage Span >= 25 required → kein RuntimeError
        df = _make_df_with_span(calendar_days=30)

        engine = BarByBarEngine(strategy=strat, broker=broker,
                                context=context, config=cfg)

        async def _run():
            return await engine.run(data={"AAPL": df})

        result = _arun(_run())   # darf nicht werfen
        assert result is not None

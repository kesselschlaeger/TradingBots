"""Tests für den Reconcile-Pfad des LiveRunners.

Regression zu Live-Incident 2026-04-24: Bei Bar-Stream-Ausfall meldete
``broker.get_positions()`` leer, worauf der Runner Trades als
"TARGET (server/bracket)" abgeschlossen hat – ohne jede Belegung durch
Fills. Die neuen Tests decken genau diesen Fall ab:

* Session ungesund → **kein** Auto-Close (Skip mit Log-Warnung).
* Session gesund + kein Fill + innerhalb Timeout → Pending, kein Close.
* Session gesund + kein Fill + Timeout abgelaufen → UNKNOWN-Close.
* Session gesund + echter Fill → sauberer Close mit Fill-Daten.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pytest

from core.models import Bar, CloseExecution
from core.trade_manager import ManagedTrade


# ── Hilfs-Stubs ─────────────────────────────────────────────────────────


class _FakeBroker:
    def __init__(self, *, session_healthy: bool = True) -> None:
        self._health = {
            "connected": session_healthy,
            "session_healthy": session_healthy,
            "last_error_code": None,
            "last_error_msg": "",
            "managed_accounts": [],
        }
        self._closes: dict[str, CloseExecution] = {}

    def set_health(self, healthy: bool) -> None:
        self._health["session_healthy"] = healthy
        self._health["connected"] = healthy

    def set_close(self, symbol: str, fill_price: float, qty: float) -> None:
        self._closes[symbol.upper()] = CloseExecution(
            symbol=symbol.upper(),
            qty=qty,
            fill_price=fill_price,
            side="sell",
            order_id="fill-42",
            realized_pnl=42.0,
        )

    async def health(self) -> dict:
        return dict(self._health)

    async def get_recent_closes(self, symbols=None):
        if not symbols:
            return dict(self._closes)
        return {s.upper(): self._closes[s.upper()]
                for s in symbols if s.upper() in self._closes}


class _FakeNotifier:
    def __init__(self) -> None:
        self.closed: list[tuple[str, str, float]] = []

    async def trade_closed(self, *, symbol, side, exit_price, pnl,
                           reason, qty, order_id):
        self.closed.append((symbol, reason, float(pnl)))


class _FakeTradeManager:
    def __init__(self, tracked: dict[str, ManagedTrade]) -> None:
        self._tracked = dict(tracked)
        self.closed_calls: list[tuple[str, str, float]] = []

    def get(self, symbol: str) -> Optional[ManagedTrade]:
        return self._tracked.get(symbol)

    def all_symbols(self) -> list[str]:
        return list(self._tracked.keys())

    async def close_trade(self, symbol, *, exit_price, exit_ts,
                          pnl, reason, tracked=None):
        self._tracked.pop(symbol, None)
        self.closed_calls.append((symbol, reason, float(pnl)))


def _make_tracked(symbol: str = "NVDA", side: str = "long",
                  entry: float = 100.0, stop: float = 95.0,
                  target: float = 110.0, qty: float = 10) -> ManagedTrade:
    return ManagedTrade(
        symbol=symbol, side=side, entry=entry, stop=stop, target=target,
        qty=qty, strategy_id="botti_mr",
        opened_at=datetime(2026, 4, 24, 19, 35, tzinfo=timezone.utc),
        current_stop=stop,
        metadata={"reason": "BB Lower Touch", "trade_id": 42,
                  "bot_name": "FLUX_BOTTI_PAPER"},
    )


def _make_bar(symbol: str = "NVDA", high: float = 111.0,
              low: float = 99.0, close: float = 110.5) -> Bar:
    return Bar(
        symbol=symbol,
        timestamp=datetime(2026, 4, 24, 19, 40, tzinfo=timezone.utc),
        open=108.0, high=high, low=low, close=close, volume=100_000,
    )


def _make_runner_stub(*, broker, notifier, tm,
                      timeout_s: float = 120.0,
                      require_healthy: bool = True):
    """Instantiiert ein Test-Double mit allen Attributen, die
    ``_reconcile_missing_positions`` referenziert."""
    from live.runner import LiveRunner

    runner = LiveRunner.__new__(LiveRunner)
    runner.broker = broker
    runner.notifier = notifier
    runner.tm = tm
    runner.anomaly = None
    runner._bot_name = "FLUX_BOTTI_PAPER"
    runner._orphan_close_since = {}
    runner._close_verification_timeout_s = timeout_s
    runner._reconcile_require_healthy_session = require_healthy

    # Stub-Strategy mit Name – anomaly-Event nutzt self.strategy.name.
    runner.strategy = type("_S", (), {"name": "botti"})()
    return runner


# ── Tests ────────────────────────────────────────────────────────────────


class TestReconcileMissingPositions:
    @pytest.mark.asyncio
    async def test_unhealthy_session_skips_close(self):
        """Primärer Regression-Test für Incident 2026-04-24."""
        tracked = _make_tracked("NVDA")
        tm = _FakeTradeManager({"NVDA": tracked})
        broker = _FakeBroker(session_healthy=False)
        notifier = _FakeNotifier()

        runner = _make_runner_stub(broker=broker, notifier=notifier, tm=tm)
        await runner._reconcile_missing_positions(
            missing_symbols=["NVDA"],
            tracked_before={"NVDA": tracked},
            bar=_make_bar("NVDA"),
        )

        assert tm.closed_calls == [], "Kein Close bei ungesunder Session"
        assert notifier.closed == []

    @pytest.mark.asyncio
    async def test_healthy_session_with_real_fill_closes(self):
        tracked = _make_tracked("NVDA")
        tm = _FakeTradeManager({"NVDA": tracked})
        broker = _FakeBroker(session_healthy=True)
        broker.set_close("NVDA", fill_price=109.5, qty=10)
        notifier = _FakeNotifier()

        runner = _make_runner_stub(broker=broker, notifier=notifier, tm=tm)
        await runner._reconcile_missing_positions(
            missing_symbols=["NVDA"],
            tracked_before={"NVDA": tracked},
            bar=_make_bar("NVDA"),
        )

        assert len(tm.closed_calls) == 1
        sym, reason, pnl = tm.closed_calls[0]
        assert sym == "NVDA"
        assert "fill verified" in reason
        assert pnl == 42.0  # realized_pnl aus CloseExecution

    @pytest.mark.asyncio
    async def test_healthy_session_no_fill_within_timeout_stays_pending(self):
        tracked = _make_tracked("NVDA")
        tm = _FakeTradeManager({"NVDA": tracked})
        broker = _FakeBroker(session_healthy=True)  # kein Fill
        notifier = _FakeNotifier()

        runner = _make_runner_stub(broker=broker, notifier=notifier, tm=tm,
                                   timeout_s=120.0)
        # Erster Aufruf: startet Orphan-Tracking
        await runner._reconcile_missing_positions(
            missing_symbols=["NVDA"],
            tracked_before={"NVDA": tracked},
            bar=_make_bar("NVDA"),
        )
        assert "NVDA" in runner._orphan_close_since
        assert tm.closed_calls == []

    @pytest.mark.asyncio
    async def test_healthy_session_no_fill_after_timeout_closes_unknown(self):
        tracked = _make_tracked("NVDA")
        tm = _FakeTradeManager({"NVDA": tracked})
        broker = _FakeBroker(session_healthy=True)
        notifier = _FakeNotifier()

        runner = _make_runner_stub(broker=broker, notifier=notifier, tm=tm,
                                   timeout_s=120.0)
        # Orphan-Start künstlich in die Vergangenheit legen → Timeout überschritten
        past = datetime.now(timezone.utc).replace(year=2020)
        runner._orphan_close_since["NVDA"] = past

        await runner._reconcile_missing_positions(
            missing_symbols=["NVDA"],
            tracked_before={"NVDA": tracked},
            bar=_make_bar("NVDA"),
        )

        assert len(tm.closed_calls) == 1
        sym, reason, _pnl = tm.closed_calls[0]
        assert sym == "NVDA"
        assert "UNKNOWN" in reason
        assert "NVDA" not in runner._orphan_close_since

    @pytest.mark.asyncio
    async def test_empty_missing_clears_orphan_tracking(self):
        tm = _FakeTradeManager({})
        broker = _FakeBroker(session_healthy=True)
        notifier = _FakeNotifier()
        runner = _make_runner_stub(broker=broker, notifier=notifier, tm=tm)
        runner._orphan_close_since["NVDA"] = datetime.now(timezone.utc)

        await runner._reconcile_missing_positions(
            missing_symbols=[],
            tracked_before={},
            bar=_make_bar("NVDA"),
        )
        assert runner._orphan_close_since == {}

    @pytest.mark.asyncio
    async def test_unhealthy_disabled_still_runs_close_path(self):
        """Wenn der Guard per Config deaktiviert wird (Legacy-Verhalten),
        läuft die Timeout-Eskalation auch bei ungesunder Session."""
        tracked = _make_tracked("NVDA")
        tm = _FakeTradeManager({"NVDA": tracked})
        broker = _FakeBroker(session_healthy=False)
        notifier = _FakeNotifier()

        runner = _make_runner_stub(broker=broker, notifier=notifier, tm=tm,
                                   timeout_s=0.0,  # sofort timeout
                                   require_healthy=False)
        await runner._reconcile_missing_positions(
            missing_symbols=["NVDA"],
            tracked_before={"NVDA": tracked},
            bar=_make_bar("NVDA"),
        )
        assert len(tm.closed_calls) == 1
        assert "UNKNOWN" in tm.closed_calls[0][1]

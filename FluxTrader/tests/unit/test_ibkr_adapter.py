"""Tests für IBKRAdapter – Order-Confirmation-Härtung + Isolation.

Kein Netzwerk, kein echtes IBKR-Gateway. ib_insync wird vollständig gemockt,
damit wir die Order-Status-Confirmation-Logik isoliert testen können.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("ib_insync", reason="ib_insync nicht installiert")

from core.models import OrderRequest, OrderSide
from execution import ibkr_adapter as mod
from execution.ibkr_adapter import IBKRAdapter
from execution.port import OrderSubmitError


# ── Fakes für ib_insync ─────────────────────────────────────────────────


class _FakeOrderStatus:
    def __init__(self, status: str = "") -> None:
        self.status = status


class _FakeOrder:
    def __init__(self, order_id: int, ref: str = "") -> None:
        self.orderId = order_id
        self.orderRef = ref
        self.transmit = True


class _FakeTrade:
    def __init__(self, order_id: int, ref: str = "", status: str = "") -> None:
        self.order = _FakeOrder(order_id, ref=ref)
        self.orderStatus = _FakeOrderStatus(status)


class _FakeIB:
    """Minimal-Fake für ib_insync.IB mit steuerbaren orderStatus-Verläufen."""

    def __init__(self) -> None:
        self._connected = True
        self._placed: list[_FakeTrade] = []
        self._cancelled: list[_FakeTrade] = []
        self._next_id = 100
        # Liste von Status-Updates, die bei jedem waitOnUpdate-Call gesetzt werden
        self._status_script: list[tuple[int, str]] = []
        self.errorEvent = MagicMock()
        self.disconnectedEvent = MagicMock()
        self.connectedEvent = MagicMock()

    # API-Oberfläche
    def isConnected(self) -> bool:
        return self._connected

    def disconnect(self) -> None:
        self._connected = False

    def connect(self, *a, **kw) -> None:
        self._connected = True

    def qualifyContracts(self, contract):  # noqa: D401
        return [contract]

    def placeOrder(self, contract, order):  # noqa: D401
        self._next_id += 1
        order.orderId = self._next_id
        trade = _FakeTrade(order.orderId, ref=order.orderRef, status="PendingSubmit")
        self._placed.append(trade)
        return trade

    def cancelOrder(self, order) -> None:
        for t in self._placed:
            if t.order.orderId == order.orderId:
                self._cancelled.append(t)
                t.orderStatus.status = "Cancelled"

    def waitOnUpdate(self, timeout: float = 0.0) -> None:
        # Pop einen Status-Schritt und appliziere ihn auf das neueste Trade-Objekt
        if not self._status_script or not self._placed:
            return
        idx, new_status = self._status_script.pop(0)
        if 0 <= idx < len(self._placed):
            self._placed[idx].orderStatus.status = new_status


@pytest.fixture()
def fake_ib(monkeypatch):
    """Ersetzt ib_insync.IB sowie StopOrder/LimitOrder/MarketOrder durch Fakes."""
    fake = _FakeIB()

    class _StubOrder:
        def __init__(self, *a, **kw):
            self.orderId = 0
            self.orderRef = ""
            self.transmit = True
            self.tif = "DAY"
            self.parentId = None
            self.orderType = ""

    monkeypatch.setattr(mod, "IB", lambda: fake)
    monkeypatch.setattr(mod, "MarketOrder", _StubOrder)
    monkeypatch.setattr(mod, "LimitOrder", _StubOrder)
    monkeypatch.setattr(mod, "StopOrder", _StubOrder)
    monkeypatch.setattr(mod, "Stock", lambda *a, **kw: SimpleNamespace(symbol=a[0] if a else ""))
    monkeypatch.setattr(mod, "qualify_contract",
                        lambda ib, c, ac="equity": c)
    return fake


def _build_adapter(order_confirm_timeout_s: float = 2.0) -> IBKRAdapter:
    adapter = IBKRAdapter(
        host="127.0.0.1", port=4002, client_id=42, paper=True,
        bot_id="TEST", order_prefix="",
        order_confirm_timeout_s=order_confirm_timeout_s,
    )
    adapter._session_healthy = True
    return adapter


class TestSubmitOrderConfirmation:
    @pytest.mark.asyncio
    async def test_submit_happy_path_accepts_submitted_status(self, fake_ib):
        adapter = _build_adapter()
        # Status-Skript: erster waitOnUpdate → Trade 0 wird "Submitted"
        fake_ib._status_script = [(0, "Submitted")]

        req = OrderRequest(symbol="NVDA", side=OrderSide.BUY, qty=10,
                           order_type="market", metadata={"asset_class": "equity"})
        order_id = await adapter.submit_order(req)

        assert order_id is not None
        assert fake_ib._cancelled == []

    @pytest.mark.asyncio
    async def test_submit_raises_on_terminal_rejected(self, fake_ib):
        adapter = _build_adapter()
        # Parent wird sofort abgelehnt
        fake_ib._status_script = [(0, "Rejected")]

        req = OrderRequest(symbol="NVDA", side=OrderSide.BUY, qty=10,
                           order_type="market")
        with pytest.raises(OrderSubmitError) as exc:
            await adapter.submit_order(req)
        assert "Rejected" in exc.value.status

    @pytest.mark.asyncio
    async def test_submit_timeout_triggers_cancel(self, fake_ib):
        adapter = _build_adapter(order_confirm_timeout_s=0.5)
        # Kein Status-Update in der Skript-Queue → Timeout
        fake_ib._status_script = []

        req = OrderRequest(symbol="NVDA", side=OrderSide.BUY, qty=10,
                           order_type="market")
        with pytest.raises(OrderSubmitError) as exc:
            await adapter.submit_order(req)
        # Nach Timeout darf Status weder ein akzeptierter noch ein terminaler
        # Bad-Status sein (PendingSubmit ist typisch, aber auch "" möglich).
        accepted = {"Submitted", "PreSubmitted", "Filled", "ApiPending"}
        assert exc.value.status not in accepted
        assert "nicht innerhalb" in str(exc.value)
        # Parent sollte storniert worden sein
        assert fake_ib._cancelled, "Parent muss nach Timeout storniert werden"

    @pytest.mark.asyncio
    async def test_submit_bracket_cleanup_on_child_failure(self, fake_ib, monkeypatch):
        adapter = _build_adapter()

        # Erster placeOrder (Parent) klappt, zweiter (SL) wirft
        original = fake_ib.placeOrder
        call_count = {"n": 0}

        def place_with_failure(contract, order):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return original(contract, order)
            raise RuntimeError("simulated SL placement failure")

        monkeypatch.setattr(fake_ib, "placeOrder", place_with_failure)

        req = OrderRequest(
            symbol="NVDA", side=OrderSide.BUY, qty=10,
            order_type="market",
            stop_loss=95.0, take_profit=110.0,
        )
        with pytest.raises(OrderSubmitError) as exc:
            await adapter.submit_order(req)
        assert "BracketSetupFailed" in exc.value.status
        # Parent wurde mit transmit=False platziert, muss nun storniert sein
        assert fake_ib._cancelled, "Parent muss bei Bracket-Fehler storniert werden"


class TestErrorEventHandling:
    def test_critical_error_flags_session_unhealthy(self, fake_ib):
        adapter = _build_adapter()
        adapter._on_ib_error(reqId=0, errorCode=1100,
                             errorString="Connectivity between TWS and server lost")
        assert adapter._session_healthy is False
        assert adapter._last_error_code == 1100

    def test_non_critical_error_keeps_session_healthy(self, fake_ib):
        adapter = _build_adapter()
        adapter._on_ib_error(reqId=0, errorCode=2104,
                             errorString="Market data farm connection is OK")
        assert adapter._session_healthy is True
        assert adapter._last_error_code == 2104


class TestOwnsRef:
    def test_own_bot_ref_is_matched(self, fake_ib):
        adapter = _build_adapter()
        assert adapter._owns_ref("TEST|NVDA|BUY|0424-1535") is True

    def test_foreign_bot_ref_is_rejected(self, fake_ib):
        adapter = _build_adapter()
        assert adapter._owns_ref("OTHER|NVDA|BUY|0424-1535") is False

    def test_empty_ref_returns_false(self, fake_ib):
        adapter = _build_adapter()
        assert adapter._owns_ref("") is False

"""Tests fuer execution/alpaca_adapter.py – Order-Lifecycle, health(), bot_id-Filter."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.models import OrderRequest, OrderSide
from execution.port import OrderSubmitError


def _make_adapter(bot_id=None):
    """Erstellt AlpacaAdapter mit gemocktem TradingClient."""
    with patch("execution.alpaca_adapter.ALPACA_AVAILABLE", True), \
         patch("execution.alpaca_adapter.TradingClient") as MockClient:
        from execution.alpaca_adapter import AlpacaAdapter
        adapter = AlpacaAdapter(
            api_key="test_key",
            secret_key="test_secret",
            paper=True,
            bot_id=bot_id,
        )
        adapter._client = MockClient.return_value
        return adapter


def _mock_order_resp(status_value: str = "accepted", order_id: str = "ord-1"):
    resp = MagicMock()
    resp.id = order_id
    status_mock = MagicMock()
    status_mock.value = status_value
    resp.status = status_mock
    return resp


@pytest.fixture
def adapter():
    return _make_adapter()


@pytest.fixture
def adapter_with_bot_id():
    return _make_adapter(bot_id="FLUX_ORB")


def test_submit_order_accepted_returns_id(adapter):
    resp = _mock_order_resp("accepted", "ord-123")
    adapter._client.submit_order = MagicMock(return_value=resp)

    req = OrderRequest(symbol="AAPL", side=OrderSide.BUY, qty=10)

    async def run():
        return await adapter.submit_order(req)

    result = asyncio.get_event_loop().run_until_complete(run())
    assert result == "ord-123"


def test_submit_order_rejected_raises(adapter):
    resp = _mock_order_resp("rejected", "ord-456")
    adapter._client.submit_order = MagicMock(return_value=resp)

    req = OrderRequest(symbol="AAPL", side=OrderSide.BUY, qty=10)

    async def run():
        return await adapter.submit_order(req)

    with pytest.raises(OrderSubmitError) as exc_info:
        asyncio.get_event_loop().run_until_complete(run())
    assert exc_info.value.status == "rejected"


def test_submit_order_canceled_raises(adapter):
    resp = _mock_order_resp("canceled", "ord-789")
    adapter._client.submit_order = MagicMock(return_value=resp)

    req = OrderRequest(symbol="TSLA", side=OrderSide.SELL, qty=5)

    async def run():
        return await adapter.submit_order(req)

    with pytest.raises(OrderSubmitError):
        asyncio.get_event_loop().run_until_complete(run())


def test_health_connected(adapter):
    clock_mock = MagicMock()
    adapter._client.get_clock = MagicMock(return_value=clock_mock)

    async def run():
        return await adapter.health()

    result = asyncio.get_event_loop().run_until_complete(run())
    assert result["connected"] is True
    assert result["session_healthy"] is True


def test_health_failed_returns_disconnected(adapter):
    adapter._client.get_clock = MagicMock(side_effect=RuntimeError("network error"))

    async def run():
        return await adapter.health()

    result = asyncio.get_event_loop().run_until_complete(run())
    assert result["connected"] is False
    assert result["session_healthy"] is False
    assert "network error" in result["last_error_msg"]


def test_get_recent_closes_filters_by_bot_id(adapter_with_bot_id):
    """Fills ohne passenden client_order_id-Präfix werden ignoriert."""
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)

    def _order(sym, coid, status="filled", fill_price=100.0):
        o = MagicMock()
        o.symbol = sym
        o.client_order_id = coid
        status_mock = MagicMock()
        status_mock.value = status
        o.status = status_mock
        o.filled_at = now
        o.filled_avg_price = fill_price
        o.filled_qty = 10
        side_mock = MagicMock()
        side_mock.value = "sell"
        o.side = side_mock
        o.id = "order-id"
        return o

    orders = [
        _order("AAPL", "FLUX_ORB|AAPL|BUY|0425"),   # passt zum Bot
        _order("TSLA", "FLUX_BOTTI|TSLA|SELL|0425"),  # anderer Bot → ignoriert
        _order("MSFT", ""),                            # kein coid → durchgelassen (kein Prefix)
    ]
    adapter_with_bot_id._client.get_orders = MagicMock(return_value=orders)

    async def run():
        return await adapter_with_bot_id.get_recent_closes()

    result = asyncio.get_event_loop().run_until_complete(run())
    # AAPL: passt; TSLA: gefiltert; MSFT: kein coid → durchgelassen
    assert "AAPL" in result
    assert "TSLA" not in result

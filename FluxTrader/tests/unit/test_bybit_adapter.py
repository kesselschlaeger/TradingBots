"""Unit-Tests für BybitAdapter – kein Netzwerk, pybit.unified_trading.HTTP gemockt."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from core.config import AppConfig, BrokerConfig, StrategyConfig
from core.models import OrderRequest, OrderSide
from execution.bybit_adapter import BrokerError, BybitAdapter

# ── Hilfsfunktionen ──────────────────────────────────────────────────────────


def _make_cfg(
    category: str = "spot",
    testnet: bool = True,
    symbols: list[str] | None = None,
) -> AppConfig:
    """Erstellt minimale AppConfig mit Bybit broker_params."""
    return AppConfig(
        strategy=StrategyConfig(
            name="ict_ob_mtf",
            symbols=symbols or ["BTCUSDT"],
        ),
        broker=BrokerConfig(type="bybit"),
        broker_params={
            "api_key": "test_key",
            "api_secret": "test_secret",
            "testnet": testnet,
            "category": category,
            "default_leverage": 1,
        },
    )


def _mock_place_order(retCode: int, ret_msg: str = "OK", order_id: str = "abc123"):
    return {
        "retCode": retCode,
        "retMsg": ret_msg,
        "result": {"orderId": order_id},
    }


# ── Tests ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_market_order_success():
    """Erfolgreiche Market-Order → OrderId wird zurückgegeben."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.place_order.return_value = _mock_place_order(0, order_id="abc123")

        adapter = BybitAdapter(_make_cfg())
        req = OrderRequest(symbol="BTCUSDT", side=OrderSide.BUY, qty=1)
        order_id = await adapter.submit_order(req)

        assert order_id == "abc123"

        call_kwargs = mock_session.place_order.call_args.kwargs
        assert call_kwargs["side"] == "Buy"
        assert call_kwargs["orderType"] == "Market"
        assert call_kwargs["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_submit_sell_order_maps_side():
    """SELL-Direction wird korrekt auf side='Sell' gemappt."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.place_order.return_value = _mock_place_order(0, order_id="sell123")

        adapter = BybitAdapter(_make_cfg())
        req = OrderRequest(symbol="ETHUSDT", side=OrderSide.SELL, qty=2)
        order_id = await adapter.submit_order(req)

        assert order_id == "sell123"
        call_kwargs = mock_session.place_order.call_args.kwargs
        assert call_kwargs["side"] == "Sell"


@pytest.mark.asyncio
async def test_submit_order_rejected():
    """retCode != 0 → BrokerError wird geworfen."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.place_order.return_value = {
            "retCode": 10001,
            "retMsg": "Invalid qty",
            "result": {},
        }

        adapter = BybitAdapter(_make_cfg())
        req = OrderRequest(symbol="BTCUSDT", side=OrderSide.BUY, qty=0)

        with pytest.raises(BrokerError, match="Invalid qty"):
            await adapter.submit_order(req)


@pytest.mark.asyncio
async def test_get_equity_parses_correctly():
    """totalEquity-String wird korrekt zu float geparst."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.get_wallet_balance.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [{"totalEquity": "12345.67"}],
            },
        }

        adapter = BybitAdapter(_make_cfg())
        equity = await adapter.get_equity()

        assert equity == pytest.approx(12345.67)
        mock_session.get_wallet_balance.assert_called_once_with(accountType="UNIFIED")


@pytest.mark.asyncio
async def test_get_equity_returns_zero_on_error():
    """Fehlerhafte API-Antwort → 0.0 (kein Exception-Durchschlag)."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.get_wallet_balance.return_value = {
            "retCode": 10005,
            "retMsg": "Permission denied",
            "result": {},
        }

        adapter = BybitAdapter(_make_cfg())
        equity = await adapter.get_equity()

        assert equity == 0.0


@pytest.mark.asyncio
async def test_cancel_order_success():
    """Erfolgreiche Stornierung → True."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.cancel_order.return_value = {"retCode": 0, "retMsg": "OK",
                                                   "result": {}}

        adapter = BybitAdapter(_make_cfg())
        result = await adapter.cancel_order("order123", symbol="BTCUSDT")

        assert result is True
        mock_session.cancel_order.assert_called_once_with(
            category="spot",
            symbol="BTCUSDT",
            orderId="order123",
        )


@pytest.mark.asyncio
async def test_cancel_order_without_symbol_returns_false():
    """cancel_order ohne symbol → False (Bybit benötigt symbol)."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        MockHTTP.return_value = MagicMock()
        adapter = BybitAdapter(_make_cfg())
        result = await adapter.cancel_order("order123")  # symbol fehlt

        assert result is False


@pytest.mark.asyncio
async def test_rate_limit_retry():
    """Erster Call retCode=10006, zweiter retCode=0 → erfolgreiches OrderId nach Retry."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session

        # Erster Call: Rate-Limit; zweiter Call: Erfolg
        mock_session.place_order.side_effect = [
            {"retCode": 10006, "retMsg": "Too many requests", "result": {}},
            _mock_place_order(0, order_id="retry_ok"),
        ]

        adapter = BybitAdapter(_make_cfg())
        req = OrderRequest(symbol="BTCUSDT", side=OrderSide.BUY, qty=1)

        with patch("asyncio.sleep") as mock_sleep:
            order_id = await adapter.submit_order(req)
            mock_sleep.assert_awaited_once_with(0.5)

        assert order_id == "retry_ok"
        assert mock_session.place_order.call_count == 2


@pytest.mark.asyncio
async def test_rate_limit_no_second_retry():
    """Zweiter Call ebenfalls Rate-Limit → BrokerError (kein Endlos-Retry)."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.place_order.return_value = {
            "retCode": 10006,
            "retMsg": "Too many requests",
            "result": {},
        }

        adapter = BybitAdapter(_make_cfg())
        req = OrderRequest(symbol="BTCUSDT", side=OrderSide.BUY, qty=1)

        with patch("asyncio.sleep"):
            with pytest.raises(BrokerError):
                await adapter.submit_order(req)

        assert mock_session.place_order.call_count == 2  # Nur 1 Retry


@pytest.mark.asyncio
async def test_submit_limit_order_includes_price():
    """Limit-Order sendet 'price' an Bybit-API."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.place_order.return_value = _mock_place_order(0)

        adapter = BybitAdapter(_make_cfg())
        req = OrderRequest(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            qty=1,
            order_type="limit",
            limit_price=50000.0,
        )
        await adapter.submit_order(req)

        kwargs = mock_session.place_order.call_args.kwargs
        assert kwargs["orderType"] == "Limit"
        assert kwargs["price"] == "50000.0"


@pytest.mark.asyncio
async def test_submit_order_with_stop_loss():
    """stop_loss wird als stopLoss-String an Bybit übergeben."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.place_order.return_value = _mock_place_order(0)

        adapter = BybitAdapter(_make_cfg())
        req = OrderRequest(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            qty=1,
            stop_loss=48000.0,
        )
        await adapter.submit_order(req)

        kwargs = mock_session.place_order.call_args.kwargs
        assert kwargs["stopLoss"] == "48000.0"


@pytest.mark.asyncio
async def test_health_connected():
    """health() → connected=True bei retCode=0."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.get_wallet_balance.return_value = {
            "retCode": 0,
            "result": {"list": [{"totalEquity": "5000.0"}]},
        }

        adapter = BybitAdapter(_make_cfg())
        h = await adapter.health()

        assert h["connected"] is True
        assert h["session_healthy"] is True
        assert h["last_error_code"] is None


@pytest.mark.asyncio
async def test_health_disconnected():
    """health() → connected=False wenn Exception geworfen wird."""
    with patch("execution.bybit_adapter.HTTP") as MockHTTP:
        mock_session = MagicMock()
        MockHTTP.return_value = mock_session
        mock_session.get_wallet_balance.side_effect = ConnectionError("timeout")

        adapter = BybitAdapter(_make_cfg())
        h = await adapter.health()

        assert h["connected"] is False
        assert h["session_healthy"] is False
        assert "timeout" in h["last_error_msg"]


@pytest.mark.asyncio
async def test_paper_flag_follows_testnet():
    """adapter.paper == True wenn testnet=True."""
    with patch("execution.bybit_adapter.HTTP"):
        adapter = BybitAdapter(_make_cfg(testnet=True))
        assert adapter.paper is True

    with patch("execution.bybit_adapter.HTTP"):
        adapter = BybitAdapter(_make_cfg(testnet=False))
        assert adapter.paper is False

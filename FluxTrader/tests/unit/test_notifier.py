"""Tests fuer live/notifier.py ohne Netzwerk."""
from __future__ import annotations

import pytest

from live.notifier import TelegramNotifier


@pytest.mark.asyncio
async def test_trade_opened_message_includes_bot_reason_and_order_context(monkeypatch):
    notifier = TelegramNotifier(
        enabled=False,
        bot_name="Flux_OBB",
        strategy_name="obb",
        broker_name="ibkr-paper",
    )
    captured: dict[str, str] = {}

    async def fake_send(message: str, parse_mode: str = "Markdown") -> bool:
        captured["message"] = notifier._decorate_message(message)
        captured["parse_mode"] = parse_mode
        return True

    monkeypatch.setattr(notifier, "send", fake_send)

    await notifier.trade_opened(
        symbol="SOXL",
        side="long",
        qty=125,
        entry=45.2,
        stop=43.9,
        target=47.8,
        reason="OBB Long: Close 45.20 > 50-Bar-High 44.80",
        order_id="abc123",
        order_type="market",
        time_in_force="cls",
    )

    message = captured["message"]
    assert "Flux_OBB" in message
    assert "obb | ibkr-paper" in message
    assert "Qty: 125" in message
    assert "Order: MARKET / CLS" in message
    assert "Order ID: `abc123`" in message
    assert "Reason: OBB Long: Close 45.20 > 50-Bar-High 44.80" in message


def test_decorate_message_includes_bot_and_context():
    notifier = TelegramNotifier(
        enabled=False,
        bot_name="Flux_ORB",
        strategy_name="orb",
        broker_name="alpaca-paper",
    )

    rendered = notifier._decorate_message("*Daily Summary*\nPnL: $+10.00")

    assert rendered.startswith("*Bot:* `Flux_ORB`")
    assert "*Context:* `orb | alpaca-paper`" in rendered
    assert "*Daily Summary*" in rendered
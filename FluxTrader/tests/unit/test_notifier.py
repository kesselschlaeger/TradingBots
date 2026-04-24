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


# ── Rate-Limiter + Template-Alerts ───────────────────────────────────


from live.notifier import _RateLimiter  # noqa: E402


def test_rate_limiter_per_symbol_per_minute():
    lim = _RateLimiter(max_per_symbol_per_minute=1, max_per_hour=100)
    assert lim.is_allowed("AAPL") is True
    assert lim.is_allowed("AAPL") is False
    assert lim.is_allowed("TSLA") is True  # anderes Symbol darf


def test_rate_limiter_global_hour_limit():
    lim = _RateLimiter(max_per_symbol_per_minute=100, max_per_hour=3)
    assert lim.is_allowed("A") is True
    assert lim.is_allowed("B") is True
    assert lim.is_allowed("C") is True
    assert lim.is_allowed("D") is False  # Global voll


class _CapturingSend:
    def __init__(self):
        self.sent: list[str] = []

    async def __call__(self, message: str, parse_mode: str = "Markdown") -> bool:
        self.sent.append(message)
        return True


@pytest.mark.asyncio
async def test_alert_uses_template_and_rate_limiter(monkeypatch):
    from core.models import AlertLevel

    notifier = TelegramNotifier(enabled=False, bot_name="Flux")
    notifier.enabled = True
    cap = _CapturingSend()
    monkeypatch.setattr(notifier, "send", cap)

    ok1 = await notifier.alert(
        AlertLevel.WARNING, "drawdown_warn",
        rate_limit_key="dd", drawdown=-12.5, threshold=10.0,
    )
    ok2 = await notifier.alert(
        AlertLevel.WARNING, "drawdown_warn",
        rate_limit_key="dd", drawdown=-12.5, threshold=10.0,
    )
    assert ok1 is True
    assert ok2 is False  # Rate-Limiter blockt (1 Nachricht/Minute pro Key)
    assert len(cap.sent) == 1
    assert "Drawdown" in cap.sent[0]


@pytest.mark.asyncio
async def test_alert_missing_template_returns_false():
    from core.models import AlertLevel
    notifier = TelegramNotifier(enabled=False)
    notifier.enabled = True
    ok = await notifier.alert(AlertLevel.INFO, "nonexistent_event")
    assert ok is False


@pytest.mark.asyncio
async def test_send_readiness_uses_readiness_channel(monkeypatch):
    notifier = TelegramNotifier(
        enabled=False,
        bot_token="main_token",
        readiness_bot_token="readiness_token",
        chat_id="main_chat",
        readiness_chat_id="readiness_chat",
    )
    notifier.enabled = True

    captured: dict[str, str] = {}

    async def fake_send_to_target(bot_token: str, chat_id: str, message: str,
                                  parse_mode: str = "Markdown") -> bool:
        captured["bot_token"] = bot_token
        captured["chat_id"] = chat_id
        captured["message"] = message
        captured["parse_mode"] = parse_mode
        return True

    monkeypatch.setattr(notifier, "_send_to_target", fake_send_to_target)

    ok = await notifier.send_readiness("*Readiness Alert*")

    assert ok is True
    assert captured["bot_token"] == "readiness_token"
    assert captured["chat_id"] == "readiness_chat"
    assert captured["message"] == "*Readiness Alert*"


@pytest.mark.asyncio
async def test_send_health_falls_back_to_main_channel(monkeypatch):
    notifier = TelegramNotifier(
        enabled=False,
        bot_token="main_token",
        chat_id="main_chat",
    )
    notifier.enabled = True

    captured: dict[str, str] = {}

    async def fake_send_to_target(bot_token: str, chat_id: str, message: str,
                                  parse_mode: str = "Markdown") -> bool:
        captured["bot_token"] = bot_token
        captured["chat_id"] = chat_id
        captured["message"] = message
        captured["parse_mode"] = parse_mode
        return True

    monkeypatch.setattr(notifier, "_send_to_target", fake_send_to_target)

    ok = await notifier.send_health("*Health Alert*")

    # Health channel has no fallback to main channel — if health credentials
    # are not set, send_health must return False and nothing is sent.
    assert ok is False
    assert captured == {}
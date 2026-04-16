"""TelegramNotifier – Push-Alerts für Live-Bot.

Async via httpx. Bei fehlendem Bot-Token wird der Notifier zur No-Op-
Implementation, sodass Tests/Backtests ohne Netzwerk laufen.
"""
from __future__ import annotations

import os
from typing import Optional

from core.logging import get_logger

log = get_logger(__name__)

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


class TelegramNotifier:
    """Telegram Bot-API Wrapper mit graceful Degradation."""

    def __init__(self, bot_token: Optional[str] = None,
                 chat_id: Optional[str] = None,
                 enabled: bool = True,
                 bot_name: str = "",
                 strategy_name: str = "",
                 broker_name: str = ""):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self.bot_name = bot_name.strip() or "FluxTrader"
        self.strategy_name = strategy_name.strip()
        self.broker_name = broker_name.strip()
        self.enabled = enabled and bool(self.bot_token) and bool(self.chat_id) \
            and HTTPX_AVAILABLE
        if enabled and not self.enabled:
            log.warning("notifier.disabled",
                        has_token=bool(self.bot_token),
                        has_chat=bool(self.chat_id),
                        httpx=HTTPX_AVAILABLE)

    @property
    def _url(self) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def _decorate_message(self, message: str) -> str:
        header = [f"*Bot:* `{self.bot_name}`"]
        context_parts = [p for p in (self.strategy_name, self.broker_name) if p]
        if context_parts:
            header.append(f"*Context:* `{' | '.join(context_parts)}`")
        return "\n".join(header + ["", message])

    async def send(self, message: str, parse_mode: str = "Markdown") -> bool:
        if not self.enabled:
            return False
        rendered_message = self._decorate_message(message)
        payload = {
            "chat_id": self.chat_id,
            "text": rendered_message[:4000],
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(self._url, json=payload)
                ok = r.status_code == 200
                if not ok:
                    log.warning("notifier.failed",
                                status=r.status_code, body=r.text[:200])
                return ok
        except Exception as e:  # noqa: BLE001
            log.warning("notifier.exception", error=str(e))
            return False

    # ── Convenience-Wrapper ────────────────────────────────────────────

    async def trade_opened(self, symbol: str, side: str, qty: float,
                           entry: float, stop: float,
                           target: Optional[float] = None,
                           reason: str = "",
                           order_id: str = "",
                           order_type: str = "market",
                           time_in_force: str = "day") -> None:
        qty_text = f"{qty:g}"
        msg = (f"*OPEN* {side.upper()} `{symbol}`\n"
               f"Qty: {qty_text}\n"
               f"Entry: ${entry:.2f}\n"
               f"Stop: ${stop:.2f}")
        if target:
            msg += f"\nTarget: ${target:.2f}"
        msg += f"\nOrder: {order_type.upper()} / {time_in_force.upper()}"
        if order_id:
            msg += f"\nOrder ID: `{order_id}`"
        if reason:
            msg += f"\nReason: {reason}"
        await self.send(msg)

    async def trade_closed(self, symbol: str, side: str, exit_price: float,
                           pnl: float, reason: str,
                           qty: Optional[float] = None,
                           order_id: str = "") -> None:
        emoji = "✅" if pnl >= 0 else "❌"
        msg = (f"*CLOSE* {emoji} `{symbol}` ({side.upper()})\n"
               f"Exit: ${exit_price:.2f}\n")
        if qty is not None:
            msg += f"Qty: {qty:g}\n"
        msg += (f"PnL: ${pnl:+.2f}\n"
                f"Reason: {reason}")
        if order_id:
            msg += f"\nOrder ID: `{order_id}`"
        await self.send(msg)

    async def error(self, where: str, msg: str) -> None:
        await self.send(f"*ERROR* `{where}`\n{msg}")

    async def daily_summary(self, day: str, pnl: float, trades: int,
                            equity: float) -> None:
        emoji = "🟢" if pnl >= 0 else "🔴"
        await self.send(
            f"*Daily Summary* {emoji} `{day}`\n"
            f"PnL: ${pnl:+.2f}\n"
            f"Trades: {trades}\n"
            f"Equity: ${equity:,.2f}"
        )

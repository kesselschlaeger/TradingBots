"""TelegramNotifier – Push-Alerts fuer Live-Bot.

Async via httpx. Bei fehlendem Bot-Token wird der Notifier zur No-Op-
Implementation, sodass Tests/Backtests ohne Netzwerk laufen.

Erweiterungen (04/2026):
  - AlertLevel (INFO/WARNING/CRITICAL)
  - Template-basierte Messages (keine f-String-Streuung in der Logik)
  - Rate-Limiter (Token-Bucket pro Symbol + globales Stundenlimit)
  - ``alert()``/``send_daily_summary()``/``send_trade_alert()`` APIs
"""
from __future__ import annotations

import os
import time as _time
from collections import deque
from typing import Any, Optional

from core.logging import get_logger
from core.models import AlertLevel, DailySummary, Position, Trade

log = get_logger(__name__)

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


# ── Templates (zentral – keine f-Strings in der Logik) ───────────────

TEMPLATES: dict[str, str] = {
    "trade_opened": (
        "📈 *{strategy}* OPEN {side} {symbol}\n"
        "Entry: {entry:.2f} | Stop: {stop:.2f} | Size: {qty:g}"
    ),
    "trade_closed": (
        "📉 *{strategy}* CLOSE {symbol}\n"
        "PnL: {pnl:+.2f} ({pnl_pct:+.1f}%) | Grund: {reason}"
    ),
    "stop_loss": (
        "🛑 Stop-Loss {symbol} @ {price:.2f} | Verlust: {pnl:+.2f}"
    ),
    "drawdown_warn": (
        "⚠️ Drawdown *{drawdown:.1f}%* – Schwelle {threshold:.0f}% erreicht"
    ),
    "drawdown_critical": (
        "🚨 *CRITICAL* Drawdown *{drawdown:.1f}%* – Circuit-Breaker-Schwelle "
        "{threshold:.0f}%"
    ),
    "circuit_break": (
        "🚨 *CIRCUIT BREAKER AKTIV* – Kein neuer Trade bis Drawdown "
        "< {threshold:.0f}%"
    ),
    "disconnect": (
        "🔌 Broker-Verbindung verloren ({adapter}) – Reconnect läuft…"
    ),
    "reconnected": (
        "✅ Broker ({adapter}) wieder verbunden nach {seconds}s"
    ),
    "anomaly": (
        "{emoji} *Anomaly: {check}*\n{message}"
    ),
    "daily_summary": (
        "📊 *Tagesbericht {date}*\n"
        "Equity: ${equity:,.2f} ({equity_change:+.2f}%)\n"
        "PnL heute: ${pnl:+.2f} ({trades} Trades, {winners} Winner)\n"
        "Drawdown: {drawdown:.1f}%\n"
        "Benchmark: {benchmark:+.2f}% | Alpha: {alpha:+.2f}%\n"
        "Open: {open_positions} | Filtered: {filtered}\n"
        "Top Winner: {top_winner} | Top Loser: {top_loser}\n"
        "{cb_note}"
    ),
}

_LEVEL_EMOJI = {
    AlertLevel.INFO: "📊",
    AlertLevel.WARNING: "⚠️",
    AlertLevel.CRITICAL: "🚨",
}


# ── Rate-Limiter ─────────────────────────────────────────────────────

class _RateLimiter:
    """Token-Bucket pro Symbol + globales Stundenlimit."""

    def __init__(self, max_per_symbol_per_minute: int = 1,
                 max_per_hour: int = 20) -> None:
        self.max_per_symbol_per_minute = max(1, int(max_per_symbol_per_minute))
        self.max_per_hour = max(1, int(max_per_hour))
        self._per_symbol: dict[str, deque[float]] = {}
        self._global: deque[float] = deque()

    def is_allowed(self, key: str = "") -> bool:
        now = _time.time()
        # globales Fenster (1h)
        while self._global and now - self._global[0] > 3600.0:
            self._global.popleft()
        if len(self._global) >= self.max_per_hour:
            return False
        if key:
            bucket = self._per_symbol.setdefault(key, deque())
            while bucket and now - bucket[0] > 60.0:
                bucket.popleft()
            if len(bucket) >= self.max_per_symbol_per_minute:
                return False
            bucket.append(now)
        self._global.append(now)
        return True


class _HealthAlertTracker:
    """In-Memory Alert-State pro (bot, strategy, check)."""

    def __init__(self) -> None:
        self._state: dict[tuple[str, str, str], dict[str, float]] = {}

    def transition(self, *, key: tuple[str, str, str], is_firing: bool,
                   now_ts: float, reminder_seconds: float) -> tuple[str, Optional[float]]:
        cur = self._state.get(key)
        if not is_firing:
            if cur is None:
                return "NOOP", None
            since = cur.get("since")
            self._state.pop(key, None)
            return "RESOLVED", since

        if cur is None:
            self._state[key] = {"since": now_ts, "last_sent": now_ts}
            return "FIRING", now_ts

        last_sent = float(cur.get("last_sent", 0.0))
        if (now_ts - last_sent) >= max(1.0, reminder_seconds):
            cur["last_sent"] = now_ts
            return "FIRING_ONGOING", cur.get("since")
        return "NOOP", cur.get("since")


# ── Notifier ─────────────────────────────────────────────────────────

class TelegramNotifier:
    """Telegram Bot-API Wrapper mit graceful Degradation."""

    def __init__(self, bot_token: Optional[str] = None,
                 chat_id: Optional[str] = None,
                 health_bot_token: Optional[str] = None,
                 readiness_bot_token: Optional[str] = None,
                 health_chat_id: Optional[str] = None,
                 readiness_chat_id: Optional[str] = None,
                 enabled: bool = True,
                 bot_name: str = "",
                 strategy_name: str = "",
                 broker_name: str = "",
                 alerts_cfg: Any = None):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.health_bot_token = (
            health_bot_token
            or os.getenv("TELEGRAM_HEALTH_BOT_TOKEN", "")
            or os.getenv("TELEGRAM_HEALTH_TOKEN", "")
        )
        self.readiness_bot_token = (
            readiness_bot_token
            or os.getenv("TELEGRAM_READINESS_BOT_TOKEN", "")
            or os.getenv("TELEGRAM_READINESS_TOKEN", "")
        )
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self.health_chat_id = (
            health_chat_id
            or os.getenv("TELEGRAM_HEALTH_CHAT_ID", "")
        )
        self.readiness_chat_id = (
            readiness_chat_id
            or os.getenv("TELEGRAM_READINESS_CHAT_ID", "")
        )
        self.bot_name = bot_name.strip() or "FluxTrader"
        self.strategy_name = strategy_name.strip()
        self.broker_name = broker_name.strip()
        self.enabled = bool(enabled) and HTTPX_AVAILABLE
        if enabled and not self.enabled:
            log.warning("notifier.disabled",
                        httpx=HTTPX_AVAILABLE)

        self.alerts_cfg = alerts_cfg
        per_sym = int(getattr(alerts_cfg, "max_per_symbol_per_minute", 1) or 1)
        per_hour = int(getattr(alerts_cfg, "max_per_hour", 20) or 20)
        self._limiter = _RateLimiter(per_sym, per_hour)
        self._health_tracker = _HealthAlertTracker()
        self._dedup_window_s = int(getattr(alerts_cfg, "dedup_window_s", 300) or 300)
        self._dedup_cache: dict[str, float] = {}

    @staticmethod
    def _url_for(bot_token: str) -> str:
        return f"https://api.telegram.org/bot{bot_token}/sendMessage"

    def _decorate_message(self, message: str) -> str:
        header = [f"*Bot:* `{self.bot_name}`"]
        context_parts = [p for p in (self.strategy_name, self.broker_name) if p]
        if context_parts:
            header.append(f"*Context:* `{' | '.join(context_parts)}`")
        return "\n".join(header + ["", message])

    async def _send_to_target(self, bot_token: str, chat_id: str, message: str,
                            parse_mode: str = "Markdown") -> bool:
        if not self.enabled or not bot_token or not chat_id:
            return False
        rendered_message = self._decorate_message(message)
        payload = {
            "chat_id": chat_id,
            "text": rendered_message[:4000],
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(self._url_for(bot_token), json=payload)
                ok = r.status_code == 200
                if not ok:
                    log.warning("notifier.failed",
                                status=r.status_code, body=r.text[:200])
                return ok
        except Exception as e:  # noqa: BLE001
            log.warning("notifier.exception", error=str(e))
            return False

    async def send(self, message: str, parse_mode: str = "Markdown") -> bool:
        return await self._send_to_target(
            self.bot_token,
            self.chat_id,
            message,
            parse_mode,
        )

    async def send_health(self, message: str,
                          parse_mode: str = "Markdown") -> bool:
        if not self.health_bot_token or not self.health_chat_id:
            return False
        return await self._send_to_target(
            self.health_bot_token,
            self.health_chat_id,
            message,
            parse_mode,
        )

    async def send_readiness(self, message: str,
                             parse_mode: str = "Markdown") -> bool:
        return await self._send_to_target(
            self.readiness_bot_token,
            self.readiness_chat_id,
            message,
            parse_mode,
        )

    async def alert_health(self,
                           event: str,
                           *,
                           level: AlertLevel,
                           bot_name: str,
                           strategy: str,
                           check_name: str,
                           is_firing: bool,
                           details: str = "",
                           reminder_interval_min: int = 30) -> bool:
        """Einziger Einstieg für Health/Readiness-Events inkl. Dedup + Reminder."""
        if not self.enabled:
            return False

        key = (bot_name, strategy, check_name)
        now_ts = _time.time()
        phase, since_ts = self._health_tracker.transition(
            key=key,
            is_firing=is_firing,
            now_ts=now_ts,
            reminder_seconds=float(max(1, reminder_interval_min) * 60),
        )
        if phase == "NOOP":
            return False

        title = {
            "process_dead": "PROCESS_DEAD",
            "data_stale": "DATA_STALE",
            "circuit_break": "CIRCUIT_BREAK",
        }.get(event, event.upper())

        if phase == "RESOLVED":
            since_text = ""
            if since_ts:
                age_min = int(max(0, now_ts - since_ts) // 60)
                since_text = f"\nDuration: {age_min}m"
            msg = (
                f"✅ *Health Resolved* {title}\n"
                f"Bot: `{bot_name}`\n"
                f"Strategy: `{strategy}`\n"
                f"Check: `{check_name}`"
                f"{since_text}"
            )
        else:
            prefix = "🚨" if level == AlertLevel.CRITICAL else "⚠️"
            tag = "FIRING" if phase == "FIRING" else "FIRING_ONGOING"
            msg = (
                f"{prefix} *Health Alert* {tag} – {title}\n"
                f"Bot: `{bot_name}`\n"
                f"Strategy: `{strategy}`\n"
                f"Check: `{check_name}`"
            )
            if details:
                msg += f"\nDetails: {details}"

        if self._is_duplicate(msg):
            return False

        rate_key = f"health:{bot_name}:{strategy}:{check_name}"
        if not self._limiter.is_allowed(rate_key):
            return False

        channel = self._resolve_alert_channel(event)
        return await self._send_channel(channel, msg)

    def _resolve_alert_channel(self, event: str) -> str:
        routing = getattr(self.alerts_cfg, "routing", None)
        if routing is None:
            return "default"
        if event in set(getattr(routing, "channel_health", []) or []):
            return "health"
        if event in set(getattr(routing, "channel_readiness", []) or []):
            return "readiness"
        return "default"

    async def _send_channel(self, channel: str, message: str) -> bool:
        if channel == "health":
            return await self.send_health(message)
        if channel == "readiness":
            return await self.send_readiness(message)
        return await self.send(message)

    def _is_duplicate(self, message: str) -> bool:
        now = _time.time()
        # Alte Fingerprints aufraeumen.
        stale_before = now - float(max(1, self._dedup_window_s))
        self._dedup_cache = {
            k: ts for k, ts in self._dedup_cache.items() if ts >= stale_before
        }
        key = str(hash(message))
        last = self._dedup_cache.get(key)
        if last is not None and (now - last) <= float(self._dedup_window_s):
            return True
        self._dedup_cache[key] = now
        return False

    # ── High-Level API (neu) ─────────────────────────────────────────

    async def alert(self, level: AlertLevel, event: str,
                    rate_limit_key: str = "", **kwargs: Any) -> bool:
        """Sende einen Alert mit Rate-Limiter.

        ``event`` muss ein Schluessel in ``TEMPLATES`` sein.
        ``rate_limit_key`` = Symbol bzw. Event-ID; leer = nur globales Limit.
        """
        tpl = TEMPLATES.get(event)
        if tpl is None:
            log.warning("notifier.template_missing", event_name=event)
            return False
        if not self._limiter.is_allowed(rate_limit_key):
            log.info("notifier.rate_limited", event_name=event,
                     key=rate_limit_key, level=level.value)
            return False
        try:
            body = tpl.format(**kwargs)
        except KeyError as e:
            log.warning("notifier.template_key_missing",
                        event_name=event, missing=str(e))
            return False
        header = f"{_LEVEL_EMOJI.get(level, '')} "
        return await self.send(header + body)

    async def send_trade_alert(self, trade: Trade, action: str) -> bool:
        """Kompakter Trade-Event-Alert (action: "opened" | "closed")."""
        if action == "opened":
            return await self.alert(
                AlertLevel.INFO,
                "trade_opened",
                rate_limit_key=trade.symbol,
                strategy=trade.strategy_id or "?",
                side=trade.side,
                symbol=trade.symbol,
                entry=float(trade.price),
                stop=float(trade.metadata.get("stop", 0.0)),
                qty=float(trade.qty),
            )
        pnl_pct = 0.0
        if trade.price > 0 and trade.qty > 0:
            pnl_pct = (trade.pnl / (trade.price * trade.qty)) * 100.0
        return await self.alert(
            AlertLevel.INFO,
            "trade_closed",
            rate_limit_key=trade.symbol,
            strategy=trade.strategy_id or "?",
            symbol=trade.symbol,
            pnl=float(trade.pnl),
            pnl_pct=pnl_pct,
            reason=trade.reason or "",
        )

    async def send_position_update(self, positions: list[Position]) -> bool:
        if not positions:
            return False
        lines = ["*Open Positions*"]
        for p in positions[:20]:
            lines.append(
                f"`{p.symbol}` {p.side.upper()} {p.qty:g} @ {p.entry_price:.2f} "
                f"| PnL ${p.unrealized_pnl:+.2f}"
            )
        return await self.send("\n".join(lines))

    async def send_daily_summary(self, summary: DailySummary) -> bool:
        cb_note = "⚠️ Circuit Breaker aktiv" if summary.circuit_breaker else ""
        return await self.alert(
            AlertLevel.INFO,
            "daily_summary",
            rate_limit_key="daily_summary",
            date=summary.date.strftime("%Y-%m-%d"),
            equity=summary.equity,
            equity_change=summary.equity_change_pct,
            pnl=summary.pnl_today,
            trades=summary.trades_today,
            winners=summary.winners_today,
            drawdown=summary.drawdown_pct,
            benchmark=summary.benchmark_pct,
            alpha=summary.alpha_pct,
            open_positions=summary.open_positions,
            filtered=summary.signals_filtered,
            top_winner=summary.top_winner or "-",
            top_loser=summary.top_loser or "-",
            cb_note=cb_note,
        )

    # ── Convenience-Wrapper (Backwards-Compat) ───────────────────────

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

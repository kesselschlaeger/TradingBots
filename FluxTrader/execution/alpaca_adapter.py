"""AlpacaAdapter – BrokerPort-Implementation via alpaca-py.

Migriert aus ORB_Bot/orb_bot_alpaca.AlpacaClient:
  - Trading (Bracket-Orders Long/Short, cancel, close_all)
  - Account (equity/cash/buying_power)
  - Positionen (sync_positions)
  - Exponential Backoff bei 429 / HTTP-Errors
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.logging import get_logger
from core.models import CloseExecution, OrderRequest, OrderSide, Position
from execution.port import BrokerPort

log = get_logger(__name__)

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import (
        GetOrdersRequest,
        LimitOrderRequest,
        MarketOrderRequest,
        StopLossRequest,
        TakeProfitRequest,
    )
    from alpaca.trading.enums import (
        OrderClass,
        OrderSide as AlpacaSide,
        TimeInForce,
    )
    ALPACA_AVAILABLE = True
except ImportError:
    ALPACA_AVAILABLE = False


_MAX_RETRIES = 3
_BASE_BACKOFF = 1.0


def _map_tif(tif: str) -> TimeInForce:
    tif_norm = (tif or "day").strip().lower()
    if tif_norm == "opg":
        return getattr(TimeInForce, "OPG", TimeInForce.DAY)
    if tif_norm in {"cls", "moc"}:
        return getattr(TimeInForce, "CLS", TimeInForce.DAY)
    return TimeInForce.DAY


async def _with_backoff(func, *args, **kwargs):
    """Führe eine sync Alpaca-API in Executor aus, mit exponential backoff
    bei 429 / temporären HTTP-Fehlern. Gibt Exception weiter nach _MAX_RETRIES.
    """
    loop = asyncio.get_event_loop()
    last: Optional[Exception] = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
        except Exception as e:  # noqa: BLE001 – alpaca wirft generische APIError
            msg = str(e).lower()
            last = e
            if attempt == _MAX_RETRIES:
                break
            wait = _BASE_BACKOFF * (2 ** (attempt - 1))
            if "429" in msg or "rate" in msg:
                wait *= 2
            log.warning("alpaca.retry", attempt=attempt, wait=wait, error=str(e))
            await asyncio.sleep(wait)
    assert last is not None
    raise last


class AlpacaAdapter(BrokerPort):
    """Broker-Adapter für Alpaca Markets."""

    def __init__(self,
                 api_key: Optional[str] = None,
                 secret_key: Optional[str] = None,
                 paper: bool = True):
        if not ALPACA_AVAILABLE:
            raise RuntimeError("alpaca-py fehlt – pip install alpaca-py")

        self.paper = paper
        api_key = api_key or os.getenv("APCA_API_KEY_ID")
        secret_key = secret_key or os.getenv("APCA_API_SECRET_KEY")
        if not api_key or not secret_key:
            raise RuntimeError(
                "APCA_API_KEY_ID und APCA_API_SECRET_KEY müssen gesetzt sein"
            )
        self._client = TradingClient(api_key=api_key, secret_key=secret_key,
                                     paper=paper)
        log.info("alpaca.connected", mode="PAPER" if paper else "LIVE")

    # ── Order-Execution ─────────────────────────────────────────────

    async def submit_order(self, req: OrderRequest) -> str:
        tif = _map_tif(req.time_in_force)
        kwargs = dict(
            symbol=req.symbol,
            qty=req.qty,
            side=AlpacaSide.BUY if req.side == OrderSide.BUY else AlpacaSide.SELL,
            time_in_force=tif,
        )
        if req.stop_loss and req.take_profit:
            kwargs["order_class"] = OrderClass.BRACKET
            kwargs["stop_loss"] = StopLossRequest(stop_price=round(req.stop_loss, 2))
            kwargs["take_profit"] = TakeProfitRequest(
                limit_price=round(req.take_profit, 2),
            )
        if req.client_order_id:
            kwargs["client_order_id"] = req.client_order_id[:128]

        order_type = (req.order_type or "market").strip().lower()
        if order_type == "limit" and req.limit_price:
            kwargs["limit_price"] = round(req.limit_price, 2)
            order_req = LimitOrderRequest(**kwargs)
        else:
            order_req = MarketOrderRequest(**kwargs)

        resp = await _with_backoff(self._client.submit_order, order_req)
        log.info("alpaca.order.submitted", symbol=req.symbol,
                 side=req.side.value, qty=req.qty, id=str(resp.id),
                 order_type=order_type, tif=(req.time_in_force or "day"),
                 status=resp.status.value)
        return str(resp.id)

    async def cancel_order(self, order_id: str) -> bool:
        try:
            await _with_backoff(self._client.cancel_order_by_id, order_id)
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("alpaca.cancel_order_failed", order_id=order_id, error=str(e))
            return False

    async def cancel_all_orders(self) -> None:
        await _with_backoff(self._client.cancel_orders)

    # ── Positionen / Konto ──────────────────────────────────────────

    async def get_position(self, symbol: str) -> Optional[Position]:
        try:
            p = await _with_backoff(self._client.get_open_position, symbol)
        except Exception:  # noqa: BLE001
            return None
        return Position(
            symbol=p.symbol,
            qty=float(p.qty),
            side=p.side.value,
            entry_price=float(p.avg_entry_price),
            current_price=float(p.current_price),
            unrealized_pnl=float(p.unrealized_pl),
        )

    async def get_positions(self) -> dict[str, Position]:
        positions = await _with_backoff(self._client.get_all_positions)
        return {
            p.symbol: Position(
                symbol=p.symbol,
                qty=float(p.qty),
                side=p.side.value,
                entry_price=float(p.avg_entry_price),
                current_price=float(p.current_price),
                unrealized_pnl=float(p.unrealized_pl),
            )
            for p in positions
        }

    async def get_account(self) -> dict:
        acct = await _with_backoff(self._client.get_account)
        return {
            "equity": float(acct.equity),
            "cash": float(acct.cash),
            "buying_power": float(acct.buying_power),
            "paper": self.paper,
        }

    async def close_position(self, symbol: str) -> bool:
        try:
            await _with_backoff(self._client.close_position, symbol)
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("alpaca.close_position_failed", symbol=symbol,
                        error=str(e))
            return False

    async def close_all_positions(self) -> dict:
        current = await self.get_positions()
        attempted = list(current.keys())
        if not attempted:
            return {"attempted": [], "remaining": [], "ok": True}
        try:
            await _with_backoff(self._client.close_all_positions,
                                cancel_orders=True)
        except Exception as e:  # noqa: BLE001
            log.warning("alpaca.close_all_failed", error=str(e))

        await asyncio.sleep(2)
        remaining_positions = await self.get_positions()
        remaining = list(remaining_positions.keys())

        if remaining:
            for sym in list(remaining):
                await self.close_position(sym)
            await asyncio.sleep(2)
            still = await self.get_positions()
            remaining = list(still.keys())

        return {"attempted": attempted, "remaining": remaining,
                "ok": not remaining}

    async def is_shortable(self, symbol: str) -> bool:
        try:
            asset = await _with_backoff(self._client.get_asset, symbol)
            return bool(asset.shortable) and bool(asset.easy_to_borrow)
        except Exception:  # noqa: BLE001
            return False

    async def get_open_orders(self) -> list[dict]:
        req = GetOrdersRequest(status="open", limit=50)
        orders = await _with_backoff(self._client.get_orders, req)
        return [
            {
                "id": str(o.id),
                "symbol": o.symbol,
                "side": o.side.value,
                "qty": float(o.qty),
                "status": o.status.value,
            }
            for o in orders
        ]

    async def get_recent_closes(
        self,
        symbols: Optional[list[str]] = None,
    ) -> dict[str, CloseExecution]:
        symbol_set = {s.upper() for s in symbols} if symbols else None
        req = GetOrdersRequest(status="closed", limit=200)
        orders = await _with_backoff(self._client.get_orders, req)

        cutoff = datetime.now(timezone.utc) - timedelta(hours=12)
        out: dict[str, CloseExecution] = {}
        for o in orders:
            sym = str(getattr(o, "symbol", "")).upper()
            if not sym:
                continue
            if symbol_set and sym not in symbol_set:
                continue

            status = str(getattr(getattr(o, "status", None), "value", "")).lower()
            if status not in {"filled", "partially_filled", "done_for_day"}:
                continue

            filled_at = getattr(o, "filled_at", None) or getattr(o, "updated_at", None)
            if filled_at is not None:
                try:
                    if filled_at.tzinfo is None:
                        filled_at = filled_at.replace(tzinfo=timezone.utc)
                    if filled_at < cutoff:
                        continue
                except Exception:  # noqa: BLE001
                    pass

            fill_price_raw = (
                getattr(o, "filled_avg_price", None)
                or getattr(o, "avg_fill_price", None)
            )
            fill_qty_raw = getattr(o, "filled_qty", None) or getattr(o, "qty", None)
            if fill_price_raw in (None, "") or fill_qty_raw in (None, ""):
                continue

            if sym in out:
                continue

            side = str(getattr(getattr(o, "side", None), "value", "")).lower()
            out[sym] = CloseExecution(
                symbol=sym,
                qty=float(fill_qty_raw),
                fill_price=float(fill_price_raw),
                side=side,
                order_id=str(getattr(o, "id", "")),
                realized_pnl=None,
                fees=None,
            )

        return out

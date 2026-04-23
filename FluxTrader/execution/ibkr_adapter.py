"""IBKRAdapter – BrokerPort-Implementation via ib_insync.

Migriert aus ORB_Bot/orb_bot_ibkr.IBKRClient. Unterstützt Multi-Bot-Isolation
via clientId + bot_id-Präfix in orderRef.
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
    from ib_insync import IB, LimitOrder, MarketOrder, Stock, StopOrder, util
    IBKR_AVAILABLE = True
except ImportError:
    IBKR_AVAILABLE = False


def _build_order_ref(bot_id: str, symbol: str, side: str,
                     prefix: str = "") -> str:
    from datetime import datetime
    import pytz
    et = pytz.timezone("America/New_York")
    ts = datetime.now(et).strftime("%m%d-%H%M")
    base = f"{bot_id}|{symbol}|{side}|{ts}"
    if prefix:
        base = f"{prefix}|{base}"
    return base[:50]


class IBKRAdapter(BrokerPort):
    """IBKR-Adapter via ib_insync.

    Erwartet laufenden TWS/Gateway. clientId muss pro Bot-Instanz eindeutig
    sein – sonst Error 326.
    """

    def __init__(self,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 client_id: Optional[int] = None,
                 paper: Optional[bool] = None,
                 bot_id: str = "FLUX",
                 order_prefix: str = ""):
        if not IBKR_AVAILABLE:
            raise RuntimeError("ib_insync fehlt – pip install ib_insync")

        self.paper = paper if paper is not None else (
            os.getenv("IBKR_PAPER", "true").lower() != "false"
        )
        self._host = host or os.getenv("IBKR_HOST", "127.0.0.1")
        self._port = port or int(os.getenv("IBKR_PORT", "4002"))
        self._client_id = client_id or int(os.getenv("IBKR_CLIENT_ID", "1"))
        self._bot_id = bot_id.upper()[:8]
        self._order_prefix = order_prefix

        self.ib = IB()
        try:
            asyncio.get_running_loop()
            log.info("ibkr.connect_deferred", client_id=self._client_id)
        except RuntimeError:
            self._connect_sync()

    # ── Connection ──────────────────────────────────────────────────

    @staticmethod
    def _ensure_thread_event_loop() -> None:
        """ib_insync braucht in jedem aufrufenden Thread einen gesetzten Loop."""
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def _connect_sync(self) -> None:
        """Sync-Connect mit 3 Retries à 5s Pause (ib_insync ist thread-safe,
        aber connect() ist sync)."""
        import time as _time
        self._ensure_thread_event_loop()
        for attempt in range(1, 4):
            try:
                self.ib.connect(self._host, self._port,
                                clientId=self._client_id,
                                timeout=20, readonly=False)
                log.info("ibkr.connected", host=self._host, port=self._port,
                         client_id=self._client_id, bot_id=self._bot_id,
                         mode="PAPER" if self.paper else "LIVE")
                return
            except Exception as e:  # noqa: BLE001
                if "326" in str(e):
                    raise RuntimeError(
                        f"IBKR Client-ID {self._client_id} bereits belegt. "
                        f"Jede Bot-Instanz braucht eine eindeutige clientId."
                    ) from e
                if attempt == 3:
                    raise
                _time.sleep(attempt * 5)

    def _ensure_connected(self) -> None:
        if not self.ib.isConnected():
            log.warning("ibkr.reconnect")
            self._connect_sync()

    async def _run(self, func, *args, **kwargs):
        loop = asyncio.get_event_loop()

        def call():
            self._ensure_thread_event_loop()
            self._ensure_connected()
            return func(*args, **kwargs)

        return await loop.run_in_executor(None, call)

    # ── Order-Execution ─────────────────────────────────────────────

    @staticmethod
    def _normalize_tif(tif: str) -> str:
        tif_norm = (tif or "day").strip().lower()
        if tif_norm == "opg":
            return "OPG"
        return "DAY"

    async def submit_order(self, req: OrderRequest) -> str:
        contract = Stock(req.symbol, "SMART", "USD")
        action = "BUY" if req.side == OrderSide.BUY else "SELL"
        ref = _build_order_ref(self._bot_id, req.symbol, action,
                               self._order_prefix)
        order_type = (req.order_type or "market").strip().lower()
        tif = (req.time_in_force or "day").strip().lower()

        def place():
            self._ensure_connected()
            self.ib.qualifyContracts(contract)

            if order_type == "limit" and req.limit_price:
                parent = LimitOrder(action, req.qty, req.limit_price)
                parent.tif = self._normalize_tif(tif)
            else:
                parent = MarketOrder(action, req.qty)
                if tif in {"cls", "moc"}:
                    parent.orderType = "MOC"
                    parent.tif = "DAY"
                else:
                    parent.tif = self._normalize_tif(tif)

            parent.orderRef = ref
            parent.transmit = False if (req.stop_loss or req.take_profit) else True
            parent_trade = self.ib.placeOrder(contract, parent)

            opposite = "SELL" if action == "BUY" else "BUY"
            child_trades = []
            if req.stop_loss:
                sl = StopOrder(opposite, req.qty, req.stop_loss)
                sl.parentId = parent_trade.order.orderId
                sl.transmit = False if req.take_profit else True
                sl.orderRef = f"{ref}|SL"
                child_trades.append(self.ib.placeOrder(contract, sl))
            if req.take_profit:
                tp = LimitOrder(opposite, req.qty, req.take_profit)
                tp.parentId = parent_trade.order.orderId
                tp.transmit = True
                tp.orderRef = f"{ref}|TP"
                child_trades.append(self.ib.placeOrder(contract, tp))

            return parent_trade

        trade = await self._run(lambda: place())
        log.info("ibkr.order.submitted", symbol=req.symbol, side=action,
                 qty=req.qty, order_id=trade.order.orderId, ref=ref,
                 order_type=order_type, tif=tif)
        return str(trade.order.orderId)

    async def cancel_order(self, order_id: str) -> bool:
        def cancel():
            self._ensure_connected()
            for t in self.ib.trades():
                if str(t.order.orderId) == str(order_id):
                    self.ib.cancelOrder(t.order)
                    return True
            return False

        return await self._run(cancel)

    async def cancel_all_orders(self) -> None:
        def cancel_all():
            self._ensure_connected()
            for t in self.ib.openTrades():
                if (t.order.orderRef or "").startswith(self._bot_id) \
                        or (self._order_prefix
                            and t.order.orderRef.startswith(self._order_prefix)):
                    self.ib.cancelOrder(t.order)

        await self._run(cancel_all)

    # ── Positionen / Konto ──────────────────────────────────────────

    async def get_position(self, symbol: str) -> Optional[Position]:
        positions = await self.get_positions()
        return positions.get(symbol)

    async def get_positions(self) -> dict[str, Position]:
        def fetch():
            self._ensure_connected()
            out: dict[str, Position] = {}
            for p in self.ib.positions():
                if p.contract.secType != "STK":
                    continue
                sym = p.contract.symbol
                qty = float(p.position)
                if qty == 0:
                    continue
                side = "long" if qty > 0 else "short"
                # ib_insync Position hat je nach Version kein unrealizedPNL-Feld.
                unrealized = (
                    getattr(p, "unrealizedPNL", None)
                    or getattr(p, "unrealizedPnl", None)
                    or 0.0
                )
                entry_price = float(p.avgCost)
                abs_qty = abs(qty)
                if abs_qty > 0:
                    if side == "long":
                        current_price = entry_price + (float(unrealized) / abs_qty)
                    else:
                        current_price = entry_price - (float(unrealized) / abs_qty)
                else:
                    current_price = entry_price
                out[sym] = Position(
                    symbol=sym, qty=abs(qty), side=side,
                    entry_price=entry_price,
                    current_price=float(current_price),
                    unrealized_pnl=float(unrealized),
                )
            return out

        return await self._run(fetch)

    async def get_account(self) -> dict:
        def fetch():
            self._ensure_connected()
            summary = {a.tag: a.value for a in self.ib.accountSummary()}
            return {
                "equity": float(summary.get("NetLiquidation", 0.0)),
                "cash": float(summary.get("TotalCashValue", 0.0)),
                "buying_power": float(summary.get("BuyingPower", 0.0)),
                "paper": self.paper,
            }

        return await self._run(fetch)

    async def close_position(self, symbol: str) -> bool:
        pos = await self.get_position(symbol)
        if pos is None:
            return False
        action = "SELL" if pos.side == "long" else "BUY"
        req = OrderRequest(symbol=symbol,
                           side=OrderSide.SELL if action == "SELL" else OrderSide.BUY,
                           qty=max(1, int(round(pos.qty))), order_type="market")
        await self.submit_order(req)
        return True

    async def close_all_positions(self) -> dict:
        await self.cancel_all_orders()
        positions = await self.get_positions()
        attempted = list(positions.keys())
        for s in attempted:
            await self.close_position(s)
        await asyncio.sleep(3)
        remaining = list((await self.get_positions()).keys())
        return {"attempted": attempted, "remaining": remaining,
                "ok": not remaining}

    async def is_shortable(self, symbol: str) -> bool:
        # IBKR: short availability würde über reqShortableShares angefragt.
        # Konservativ True; tatsächliche Shortability prüft IBKR beim Order-Submit.
        return True

    async def close(self) -> None:
        if self.ib.isConnected():
            await asyncio.get_event_loop().run_in_executor(None, self.ib.disconnect)

    async def get_recent_closes(
        self,
        symbols: Optional[list[str]] = None,
    ) -> dict[str, CloseExecution]:
        symbol_set = {s.upper() for s in symbols} if symbols else None

        def fetch() -> dict[str, CloseExecution]:
            self._ensure_connected()
            fills = list(self.ib.fills())
            out: dict[str, CloseExecution] = {}
            cutoff = datetime.now(timezone.utc) - timedelta(hours=12)

            for fill in reversed(fills):
                contract = getattr(fill, "contract", None)
                execution = getattr(fill, "execution", None)
                if contract is None or execution is None:
                    continue
                if getattr(contract, "secType", "") != "STK":
                    continue

                sym = str(getattr(contract, "symbol", "")).upper()
                if not sym:
                    continue
                if symbol_set and sym not in symbol_set:
                    continue
                if sym in out:
                    continue

                exec_time = getattr(execution, "time", None)
                if isinstance(exec_time, datetime):
                    if exec_time.tzinfo is None:
                        exec_time = exec_time.replace(tzinfo=timezone.utc)
                    if exec_time < cutoff:
                        continue

                side_raw = str(getattr(execution, "side", "")).upper()
                side = "buy" if side_raw in {"BOT", "BUY"} else "sell"

                commission_report = getattr(fill, "commissionReport", None)
                fees = None
                realized_pnl = None
                if commission_report is not None:
                    comm = getattr(commission_report, "commission", None)
                    pnl = getattr(commission_report, "realizedPNL", None)
                    if comm not in (None, ""):
                        fees = float(comm)
                    if pnl not in (None, ""):
                        realized_pnl = float(pnl)

                out[sym] = CloseExecution(
                    symbol=sym,
                    qty=float(getattr(execution, "shares", 0.0)),
                    fill_price=float(getattr(execution, "price", 0.0)),
                    side=side,
                    order_id=str(getattr(execution, "orderId", "")),
                    realized_pnl=realized_pnl,
                    fees=fees,
                )

            return out

        return await self._run(fetch)

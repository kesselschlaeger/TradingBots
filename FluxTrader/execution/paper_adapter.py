"""PaperAdapter – vollständig in-memory, ohne Netzwerk.

Simuliert Market-Orders mit konfigurierbarem Slippage + Commission,
tracked virtuelle Positionen, erzeugt deterministische Order-IDs.
Für Unit-Tests und als Backtest-Executor nutzbar.
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from core.logging import get_logger
from core.models import CloseExecution, OrderRequest, OrderSide, Position, Trade
from execution.port import BrokerPort

log = get_logger(__name__)


@dataclass
class PaperOrder:
    id: str
    req: OrderRequest
    filled_price: float
    filled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "filled"


class PaperAdapter(BrokerPort):
    """In-Memory-Broker für Tests & Backtests.

    Markt-Orders werden sofort zum aktuellen Last-Price gefüllt
    (konfigurierbar per set_market_price), mit Slippage/Commission.
    """

    def __init__(self,
                 initial_cash: float = 30_000.0,
                 slippage_pct: float = 0.0002,
                 commission_pct: float = 0.00005,
                 paper: bool = True):
        self.paper = paper
        self._cash = initial_cash
        self._initial = initial_cash
        self._slippage = slippage_pct
        self._commission = commission_pct
        self._positions: dict[str, Position] = {}
        self._orders: dict[str, PaperOrder] = {}
        self._market_prices: dict[str, float] = {}
        self._trade_log: list[Trade] = []
        self._lock = asyncio.Lock()

    # ── Preis-Injection (Backtest ruft dies) ───────────────────────

    def set_market_price(self, symbol: str, price: float) -> None:
        self._market_prices[symbol] = float(price)
        # Unrealized-PnL aktualisieren
        pos = self._positions.get(symbol)
        if pos is not None:
            pos.current_price = price
            direction = 1 if pos.side == "long" else -1
            pos.unrealized_pnl = (price - pos.entry_price) * pos.qty * direction

    def set_market_prices(self, prices: dict[str, float]) -> None:
        for s, p in prices.items():
            self.set_market_price(s, p)

    def update_price(self, symbol: str, price: float) -> None:
        self.set_market_price(symbol, price)

    # ── Port-Implementation ────────────────────────────────────────

    async def submit_order(self, req: OrderRequest) -> str:
        async with self._lock:
            price = self._resolve_price(req)
            if req.side == OrderSide.BUY:
                filled = price * (1 + self._slippage)
            else:
                filled = price * (1 - self._slippage)
            commission = filled * req.qty * self._commission

            order_id = f"paper-{uuid.uuid4().hex[:12]}"
            self._orders[order_id] = PaperOrder(
                id=order_id, req=req, filled_price=filled,
            )
            self._apply_fill(req, filled, commission, order_id)
            log.info("paper.fill", symbol=req.symbol, side=req.side.value,
                     qty=req.qty, filled_price=filled, commission=commission,
                     order_id=order_id)
            return order_id

    async def cancel_order(self, order_id: str) -> bool:
        order = self._orders.get(order_id)
        if order is None:
            return False
        if order.status == "filled":
            return False
        order.status = "canceled"
        return True

    async def cancel_all_orders(self) -> None:
        for o in self._orders.values():
            if o.status == "pending":
                o.status = "canceled"

    async def get_position(self, symbol: str) -> Optional[Position]:
        return self._positions.get(symbol)

    async def get_positions(self) -> dict[str, Position]:
        return dict(self._positions)

    async def get_account(self) -> dict:
        equity = self._cash + sum(
            pos.qty * self._market_prices.get(s, pos.entry_price)
            * (1 if pos.side == "long" else -1)
            + (pos.entry_price * pos.qty if pos.side == "short" else 0.0)
            for s, pos in self._positions.items()
        )
        return {
            "equity": equity,
            "cash": self._cash,
            "buying_power": max(self._cash, 0.0) * 4,
            "paper": self.paper,
        }

    async def close_position(self, symbol: str) -> bool:
        pos = self._positions.get(symbol)
        if pos is None:
            return False
        price = self._market_prices.get(symbol, pos.entry_price)
        side = OrderSide.SELL if pos.side == "long" else OrderSide.BUY
        req = OrderRequest(symbol=symbol, side=side, qty=int(pos.qty),
                           order_type="market")
        await self.submit_order(req)
        return True

    async def close_all_positions(self) -> dict:
        attempted = list(self._positions.keys())
        for s in attempted:
            await self.close_position(s)
        remaining = list(self._positions.keys())
        return {"attempted": attempted, "remaining": remaining,
                "ok": not remaining}

    async def is_shortable(self, symbol: str) -> bool:
        return True

    async def get_recent_closes(
        self,
        symbols: Optional[list[str]] = None,
    ) -> dict[str, CloseExecution]:
        symbol_set = {s.upper() for s in symbols} if symbols else None
        out: dict[str, CloseExecution] = {}
        for trade in reversed(self._trade_log):
            sym = trade.symbol.upper()
            if symbol_set and sym not in symbol_set:
                continue
            if sym in out:
                continue
            out[sym] = CloseExecution(
                symbol=trade.symbol,
                qty=float(trade.qty),
                fill_price=float(trade.price),
                side="sell" if trade.side == "SELL" else "buy",
                order_id=trade.order_id,
                realized_pnl=float(trade.pnl),
                fees=float(trade.fees),
            )
        return out

    # ── Interne Matching-Engine ────────────────────────────────────

    def _resolve_price(self, req: OrderRequest) -> float:
        if req.order_type == "limit" and req.limit_price is not None:
            return float(req.limit_price)
        price = self._market_prices.get(req.symbol)
        if price is None:
            raise RuntimeError(
                f"PaperAdapter: kein Marktpreis für {req.symbol}. "
                f"set_market_price() vorher aufrufen."
            )
        return float(price)

    def _apply_fill(
        self,
        req: OrderRequest,
        price: float,
        commission: float,
        order_id: str,
    ) -> None:
        sym = req.symbol
        qty = int(req.qty)
        existing = self._positions.get(sym)

        if req.side == OrderSide.BUY:
            self._cash -= price * qty + commission
            if existing is None:
                self._positions[sym] = Position(
                    symbol=sym, qty=qty, side="long",
                    entry_price=price, current_price=price,
                    stop_loss=req.stop_loss, take_profit=req.take_profit,
                )
            elif existing.side == "long":
                new_qty = existing.qty + qty
                existing.entry_price = (
                    (existing.entry_price * existing.qty + price * qty) / new_qty
                )
                existing.qty = new_qty
            else:  # short -> cover (closing trade)
                close_qty = min(qty, existing.qty)
                pnl = (existing.entry_price - price) * close_qty - commission
                self._trade_log.append(Trade(
                    symbol=sym, side="COVER", qty=close_qty,
                    price=price, pnl=pnl,
                    order_id=order_id,
                    reason=req.client_order_id or "",
                    fees=commission,
                ))
                if qty >= existing.qty:
                    self._positions.pop(sym)
                else:
                    existing.qty -= qty
        else:  # SELL
            self._cash += price * qty - commission
            if existing is None:
                self._positions[sym] = Position(
                    symbol=sym, qty=qty, side="short",
                    entry_price=price, current_price=price,
                    stop_loss=req.stop_loss, take_profit=req.take_profit,
                )
            elif existing.side == "long":  # closing long trade
                close_qty = min(qty, existing.qty)
                pnl = (price - existing.entry_price) * close_qty - commission
                self._trade_log.append(Trade(
                    symbol=sym, side="SELL", qty=close_qty,
                    price=price, pnl=pnl,
                    order_id=order_id,
                    reason=req.client_order_id or "",
                    fees=commission,
                ))
                if qty >= existing.qty:
                    self._positions.pop(sym)
                else:
                    existing.qty -= qty
            else:  # grow short
                new_qty = existing.qty + qty
                existing.entry_price = (
                    (existing.entry_price * existing.qty + price * qty) / new_qty
                )
                existing.qty = new_qty

    # ── Sync-Shortcuts (Backtest-Hot-Loop, kein async-Overhead) ────

    def get_account_sync(self) -> dict:
        """Synchrone Variante von get_account – kein await, kein Lock."""
        equity = self._cash + sum(
            pos.qty * self._market_prices.get(s, pos.entry_price)
            * (1 if pos.side == "long" else -1)
            + (pos.entry_price * pos.qty if pos.side == "short" else 0.0)
            for s, pos in self._positions.items()
        )
        return {
            "equity": equity,
            "cash": self._cash,
            "buying_power": max(self._cash, 0.0) * 4,
            "paper": self.paper,
        }

    def get_position_sync(self, symbol: str) -> Optional[Position]:
        return self._positions.get(symbol)

    def get_positions_sync(self) -> dict[str, Position]:
        return dict(self._positions)

    def submit_order_sync(self, req: OrderRequest) -> str:
        """Synchrone Variante von submit_order – kein await, kein Lock."""
        price = self._resolve_price(req)
        if req.side == OrderSide.BUY:
            filled = price * (1 + self._slippage)
        else:
            filled = price * (1 - self._slippage)
        commission = filled * req.qty * self._commission

        order_id = f"paper-{uuid.uuid4().hex[:12]}"
        self._orders[order_id] = PaperOrder(
            id=order_id, req=req, filled_price=filled,
        )
        self._apply_fill(req, filled, commission, order_id)
        log.info("paper.fill", symbol=req.symbol, side=req.side.value,
                 qty=req.qty, filled_price=filled, commission=commission,
                 order_id=order_id)
        return order_id

    # ── Convenience für Tests ──────────────────────────────────────

    @property
    def cash(self) -> float:
        return self._cash

    @property
    def positions(self) -> dict[str, Position]:
        return dict(self._positions)

    @property
    def trade_log(self) -> list[Trade]:
        return list(self._trade_log)

    def reset(self) -> None:
        self._cash = self._initial
        self._positions.clear()
        self._orders.clear()
        self._market_prices.clear()
        self._trade_log.clear()

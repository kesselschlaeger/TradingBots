"""BybitAdapter – BrokerPort-Implementierung via pybit v5 (HTTP sync → async).

Unterstützte Kategorien:
  "spot"   → Spot-Handel (BTCUSDT, ETHUSDT, …)
  "linear" → USDT-Perpetuals (BTCUSDT Perp)

Sync-SDK wird via asyncio.get_running_loop().run_in_executor(None, ...) gewrapped.
Kein asyncio.run() innerhalb des laufenden Event-Loops.
"""
from __future__ import annotations

import asyncio
from typing import Optional

from core.config import AppConfig
from core.logging import get_logger
from core.models import CloseExecution, OrderRequest, OrderSide, Position
from execution.port import BrokerPort, OrderSubmitError

log = get_logger(__name__)

try:
    from pybit.unified_trading import HTTP
    PYBIT_AVAILABLE = True
except ImportError:
    PYBIT_AVAILABLE = False

    class HTTP:  # type: ignore[no-redef]
        """Platzhalter wenn pybit nicht installiert ist – nur für Tests patchbar."""
        def __init__(self, **kwargs):
            raise RuntimeError("pybit fehlt – pip install pybit")


class BrokerError(Exception):
    """Bybit hat eine Order mit retCode != 0 abgelehnt."""


_RATE_LIMIT_CODE = 10006   # Bybit: request rate limit exceeded
_QTY_PRECISION = 4          # Standard-Präzision; symbolabhängig überschreibbar


class BybitAdapter(BrokerPort):
    """Broker-Adapter für Bybit (Spot + USDT-Perpetuals) via pybit v5."""

    def __init__(self, cfg: AppConfig) -> None:
        params = cfg.broker_params or {}
        api_key = str(params.get("api_key", ""))
        api_secret = str(params.get("api_secret", ""))
        testnet = bool(params.get("testnet", True))
        self._category: str = str(params.get("category", "spot"))
        default_leverage = int(params.get("default_leverage", 1))

        self.paper: bool = testnet  # testnet entspricht Paper-Modus

        self._http = HTTP(testnet=testnet, api_key=api_key, api_secret=api_secret)
        log.info(
            "bybit.connected",
            mode="TESTNET" if testnet else "MAINNET",
            category=self._category,
        )

        if self._category == "linear" and default_leverage != 1:
            for sym in (cfg.strategy.symbols or []):
                self._set_leverage_sync(sym, default_leverage)

    def _set_leverage_sync(self, symbol: str, leverage: int) -> None:
        lev_str = str(leverage)
        try:
            self._http.set_leverage(
                category="linear",
                symbol=symbol,
                buyLeverage=lev_str,
                sellLeverage=lev_str,
            )
            log.info("bybit.leverage_set", symbol=symbol, leverage=leverage)
        except Exception as e:
            log.warning("bybit.set_leverage_failed", symbol=symbol, error=str(e))

    # ── Interne Hilfsmethode: run_in_executor ────────────────────────

    async def _run(self, fn):
        """Führt synchrone pybit-Funktion im Thread-Pool aus."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, fn)

    # ── Order-Execution ──────────────────────────────────────────────

    async def submit_order(self, req: OrderRequest) -> str:
        """Platziert Order bei Bybit; gibt Broker-Order-ID zurück.

        Bei retCode==10006 (Rate-Limit) wird einmal nach 0.5 s wiederholt.
        Bei anderen Fehler-Codes wird BrokerError geworfen.
        """
        return await self._submit_with_retry(req, _is_retry=False)

    async def _submit_with_retry(self, req: OrderRequest, *, _is_retry: bool) -> str:
        side = "Buy" if req.side == OrderSide.BUY else "Sell"
        order_type = "Market" if (req.order_type or "market").lower() == "market" else "Limit"
        qty_str = str(round(float(req.qty), _QTY_PRECISION))

        kwargs: dict = dict(
            category=self._category,
            symbol=req.symbol,
            side=side,
            orderType=order_type,
            qty=qty_str,
            positionIdx=0,            # One-Way-Mode (Standard für Retail)
            timeInForce="GTC",
        )
        if order_type == "Limit" and req.limit_price is not None:
            kwargs["price"] = str(round(req.limit_price, 2))
        if req.stop_loss is not None:
            kwargs["stopLoss"] = str(round(req.stop_loss, 2))
        if req.take_profit is not None:
            kwargs["takeProfit"] = str(round(req.take_profit, 2))
        if req.client_order_id:
            kwargs["orderLinkId"] = req.client_order_id[:64]

        try:
            result = await self._run(lambda: self._http.place_order(**kwargs))
        except Exception as e:
            log.error("bybit.order_exception", symbol=req.symbol, error=str(e))
            raise BrokerError(str(e)) from e

        ret_code = result.get("retCode", -1)

        if ret_code == _RATE_LIMIT_CODE:
            if _is_retry:
                raise BrokerError("bybit rate limit (retCode=10006) nach Retry nicht erholt")
            log.warning("bybit.rate_limit", symbol=req.symbol, retrying=True)
            await asyncio.sleep(0.5)
            return await self._submit_with_retry(req, _is_retry=True)

        if ret_code != 0:
            ret_msg = result.get("retMsg", "unknown")
            log.error("bybit.order_rejected", symbol=req.symbol,
                      retCode=ret_code, retMsg=ret_msg)
            raise BrokerError(ret_msg)

        order_id: str = result["result"]["orderId"]
        log.info("bybit.order.submitted", symbol=req.symbol, side=side,
                 qty=qty_str, order_type=order_type, order_id=order_id)
        return order_id

    async def cancel_order(self, order_id: str, symbol: str = "") -> bool:
        """Storniert eine offene Order. symbol wird für Bybit benötigt.

        Wenn symbol leer: Warning und False (Bybit API erfordert symbol).
        """
        if not symbol:
            log.warning("bybit.cancel_order.no_symbol", order_id=order_id)
            return False
        try:
            result = await self._run(lambda: self._http.cancel_order(
                category=self._category,
                symbol=symbol,
                orderId=order_id,
            ))
            if result.get("retCode") != 0:
                log.warning("bybit.cancel_order_rejected", order_id=order_id,
                            symbol=symbol, retMsg=result.get("retMsg"))
                return False
            log.info("bybit.order.cancelled", order_id=order_id, symbol=symbol)
            return True
        except Exception as e:
            log.warning("bybit.cancel_order_exception", order_id=order_id,
                        symbol=symbol, error=str(e))
            return False

    async def cancel_all_orders(self) -> None:
        open_orders = await self.get_open_orders(symbol="")
        for order in open_orders:
            await self.cancel_order(order["orderId"], symbol=order.get("symbol", ""))

    # ── Positionen & Konto ───────────────────────────────────────────

    async def get_position(self, symbol: str) -> Optional[Position]:
        positions = await self.get_positions()
        return positions.get(symbol)

    async def get_positions(self) -> dict[str, Position]:
        if self._category != "linear":
            return {}  # Spot hat keine separaten Positions-Einträge
        try:
            result = await self._run(lambda: self._http.get_positions(
                category="linear",
                settleCoin="USDT",
            ))
            if result.get("retCode") != 0:
                log.error("bybit.get_positions_failed", retMsg=result.get("retMsg"))
                return {}
            out: dict[str, Position] = {}
            for p in result["result"].get("list", []):
                qty = float(p.get("size", 0))
                if qty == 0:
                    continue
                sym = p["symbol"]
                out[sym] = Position(
                    symbol=sym,
                    qty=qty,
                    side="long" if p.get("side", "Buy") == "Buy" else "short",
                    entry_price=float(p.get("avgPrice", 0)),
                    current_price=float(p.get("markPrice", 0)),
                    unrealized_pnl=float(p.get("unrealisedPnl", 0)),
                )
            return out
        except Exception as e:
            log.error("bybit.get_positions_exception", error=str(e))
            return {}

    async def get_account(self) -> dict:
        equity = await self.get_equity()
        return {"equity": equity, "cash": equity, "buying_power": equity,
                "paper": self.paper}

    async def close_position(self, symbol: str) -> bool:
        pos = await self.get_position(symbol)
        if pos is None:
            return True
        close_side = OrderSide.SELL if pos.side == "long" else OrderSide.BUY
        close_req = OrderRequest(symbol=symbol, side=close_side, qty=int(pos.qty))
        try:
            await self.submit_order(close_req)
            return True
        except BrokerError as e:
            log.warning("bybit.close_position_failed", symbol=symbol, error=str(e))
            return False

    async def close_all_positions(self) -> dict:
        positions = await self.get_positions()
        attempted = list(positions.keys())
        ok = True
        for sym in attempted:
            if not await self.close_position(sym):
                ok = False
        remaining = list((await self.get_positions()).keys())
        return {"attempted": attempted, "remaining": remaining, "ok": ok}

    async def is_shortable(self, symbol: str) -> bool:
        # Spot: kein Leerverkauf möglich; linear: Short über Perp immer verfügbar
        return self._category == "linear"

    # ── Bybit-spezifische Methoden ───────────────────────────────────

    async def get_equity(self) -> float:
        """Gibt totalEquity des UNIFIED-Kontos zurück (in USD)."""
        try:
            result = await self._run(lambda: self._http.get_wallet_balance(
                accountType="UNIFIED",
            ))
            if result.get("retCode") != 0:
                log.error("bybit.get_equity_failed", retMsg=result.get("retMsg"))
                return 0.0
            return float(result["result"]["list"][0]["totalEquity"])
        except Exception as e:
            log.error("bybit.get_equity_exception", error=str(e))
            return 0.0

    async def get_open_orders(self, symbol: str = "") -> list[dict]:
        """Gibt offene Orders zurück. symbol="" → alle Symbole (Bybit-Limit: 50)."""
        kwargs: dict = dict(category=self._category)
        if symbol:
            kwargs["symbol"] = symbol
        try:
            result = await self._run(lambda: self._http.get_open_orders(**kwargs))
            if result.get("retCode") != 0:
                log.warning("bybit.get_open_orders_failed", retMsg=result.get("retMsg"))
                return []
            return result["result"].get("list", [])
        except Exception as e:
            log.error("bybit.get_open_orders_exception", symbol=symbol, error=str(e))
            return []

    # ── Health ───────────────────────────────────────────────────────

    async def health(self) -> dict:
        """Probe via get_wallet_balance – schlägt bei Auth-Fehler fehl."""
        try:
            result = await self._run(lambda: self._http.get_wallet_balance(
                accountType="UNIFIED",
            ))
            ok = result.get("retCode") == 0
            return {
                "connected": ok,
                "session_healthy": ok,
                "last_error_code": None if ok else result.get("retCode"),
                "last_error_msg": "" if ok else result.get("retMsg", ""),
                "managed_accounts": [],
            }
        except Exception as e:
            log.warning("bybit.health_failed", error=str(e))
            return {
                "connected": False,
                "session_healthy": False,
                "last_error_code": None,
                "last_error_msg": str(e),
                "managed_accounts": [],
            }

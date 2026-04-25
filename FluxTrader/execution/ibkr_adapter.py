"""IBKRAdapter – BrokerPort-Implementation via ib_insync.

Migriert aus ORB_Bot/orb_bot_ibkr.IBKRClient. Unterstützt Multi-Bot-Isolation
via clientId + bot_id-Präfix in orderRef.

Order-Lifecycle-Invariante (siehe CLAUDE.md → "Order-Lifecycle"):
    submit_order() garantiert, dass IBKR die Order akzeptiert hat
    (orderStatus ∈ {Submitted, PreSubmitted, Filled}) bevor eine Order-ID
    zurückgegeben wird. Ansonsten wird ``OrderSubmitError`` geworfen und
    lokal erstellte Parent-Legs werden wieder storniert. Dadurch darf der
    Runner aus einer erfolgreich zurückgegebenen Order-ID ableiten, dass
    ein ``ManagedTrade``/DB-Eintrag zulässig ist.
"""
from __future__ import annotations

import asyncio
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.logging import get_logger
from core.models import CloseExecution, OrderRequest, OrderSide, Position
from execution.contract_factory import build_contract, qualify_contract
from execution.port import BrokerPort, OrderSubmitError

log = get_logger(__name__)

try:
    from ib_insync import IB, LimitOrder, MarketOrder, Stock, StopOrder, util
    IBKR_AVAILABLE = True
except ImportError:
    IBKR_AVAILABLE = False


# Kritische IBKR-Fehlercodes, die die Session als ungesund markieren.
# 326 = clientId-Kollision; 502/504 = Keine Verbindung zu TWS; 1100/1102 =
# Verbindung verloren/wiederhergestellt; 2110 = Connectivity to server lost.
_CRITICAL_ERROR_CODES: frozenset[int] = frozenset({326, 502, 504, 1100, 1102, 2110})

# Terminal-negative Order-Status (Adapter meldet OrderSubmitError).
_TERMINAL_BAD_STATUSES: frozenset[str] = frozenset({
    "Cancelled", "ApiCancelled", "Inactive", "Rejected", "PendingCancel",
})

# Akzeptierte Order-Status – IBKR hat die Order übernommen.
_ACCEPTED_STATUSES: frozenset[str] = frozenset({
    "Submitted", "PreSubmitted", "Filled", "ApiPending",
})


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
                 order_prefix: str = "",
                 order_confirm_timeout_s: float = 10.0):
        if not IBKR_AVAILABLE:
            raise RuntimeError("ib_insync fehlt – pip install ib_insync")

        # Verbindungsparameter kommen ausschließlich aus AppConfig (_apply_env_overrides).
        # Kein eigener os.getenv-Fallback hier – das überschattet bewusst gesetzte Config-Werte.
        self.paper = paper if paper is not None else True
        self._host = host or "127.0.0.1"
        self._port = port or 4002
        self._client_id = client_id if client_id is not None else 1
        self._bot_id = bot_id.upper()[:8]
        self._order_prefix = order_prefix
        self._order_confirm_timeout_s = float(order_confirm_timeout_s)

        # Session-Health-Tracking: wird von errorEvent/disconnectedEvent
        # gepflegt. Ein Adapter gilt nur dann als „session_healthy", wenn
        # isConnected() UND keine kritische Fehlersequenz vorliegt.
        self._session_healthy: bool = False
        self._last_error_code: Optional[int] = None
        self._last_error_msg: str = ""
        self._last_error_ts: Optional[datetime] = None
        self._events_bound: bool = False

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

    def _bind_events(self) -> None:
        """Registriert errorEvent/disconnectedEvent/connectedEvent genau einmal."""
        if self._events_bound:
            return
        try:
            self.ib.errorEvent += self._on_ib_error
            self.ib.disconnectedEvent += self._on_ib_disconnected
            self.ib.connectedEvent += self._on_ib_connected
            self._events_bound = True
        except Exception as e:  # noqa: BLE001
            log.warning("ibkr.event_bind_failed", error=str(e))

    def _on_ib_connected(self) -> None:
        self._session_healthy = True
        log.info("ibkr.event.connected", client_id=self._client_id)

    def _on_ib_disconnected(self) -> None:
        self._session_healthy = False
        log.warning("ibkr.event.disconnected", client_id=self._client_id)

    def _on_ib_error(self, reqId: int, errorCode: int, errorString: str,
                     contract=None) -> None:  # noqa: ANN001
        """Callback für ib.errorEvent. Kritische Codes kippen die Session."""
        self._last_error_code = int(errorCode)
        self._last_error_msg = str(errorString)
        self._last_error_ts = datetime.now(timezone.utc)
        if int(errorCode) in _CRITICAL_ERROR_CODES:
            self._session_healthy = False
            log.error("ibkr.event.error_critical",
                      code=int(errorCode), msg=str(errorString), req_id=int(reqId))
        else:
            log.info("ibkr.event.error",
                     code=int(errorCode), msg=str(errorString), req_id=int(reqId))

    def _connect_sync(self) -> None:
        """Sync-Connect mit 3 Retries à 5s Pause (ib_insync ist thread-safe,
        aber connect() ist sync)."""
        self._ensure_thread_event_loop()
        for attempt in range(1, 4):
            try:
                self.ib.connect(self._host, self._port,
                                clientId=self._client_id,
                                timeout=20, readonly=False)
                self._bind_events()
                self._session_healthy = True
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
        if not self.ib.isConnected() or not self._session_healthy:
            log.warning("ibkr.reconnect",
                        connected=self.ib.isConnected(),
                        session_healthy=self._session_healthy)
            try:
                if self.ib.isConnected():
                    self.ib.disconnect()
            except Exception:  # noqa: BLE001
                pass
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

    def _wait_for_order_confirmation(self, parent_trade, transmit_trade) -> str:
        """Blockiert bis IBKR den Order-Status meldet oder Timeout.

        ``parent_trade``: Parent-Leg (kann == transmit_trade sein, wenn keine
        Bracket vorhanden).
        ``transmit_trade``: das Leg mit transmit=True (letzter Child bei
        Bracket-Order; Parent wenn keine Bracket existiert).

        Liefert den Status des Parent-Trade (maßgeblich für die Position),
        wirft ``OrderSubmitError`` bei Timeout oder terminalem Bad-Status.
        """
        deadline = _time.monotonic() + self._order_confirm_timeout_s
        parent_id = parent_trade.order.orderId
        last_status = ""
        last_transmit_status = ""

        while _time.monotonic() < deadline:
            # 1s-Schritte genügen – ib_insync pflegt orderStatus über Event-Loop
            try:
                self.ib.waitOnUpdate(timeout=0.5)
            except Exception:  # noqa: BLE001
                pass
            last_status = str(getattr(parent_trade.orderStatus, "status", "") or "")
            last_transmit_status = str(
                getattr(transmit_trade.orderStatus, "status", "") or ""
            )

            # Terminal negativ → sofort abbrechen
            if last_status in _TERMINAL_BAD_STATUSES:
                raise OrderSubmitError(
                    f"Parent-Order {parent_id} wurde terminal abgelehnt "
                    f"(status={last_status})",
                    status=last_status,
                    order_id=str(parent_id),
                    last_error_code=self._last_error_code,
                    last_error_msg=self._last_error_msg,
                )
            if last_transmit_status in _TERMINAL_BAD_STATUSES:
                raise OrderSubmitError(
                    f"Transmit-Leg {transmit_trade.order.orderId} abgelehnt "
                    f"(status={last_transmit_status})",
                    status=last_transmit_status,
                    order_id=str(parent_id),
                    last_error_code=self._last_error_code,
                    last_error_msg=self._last_error_msg,
                )

            if last_status in _ACCEPTED_STATUSES:
                return last_status

        # Timeout → Order hängt lokal / bei IBKR ohne Bestätigung
        raise OrderSubmitError(
            f"Parent-Order {parent_id} nicht innerhalb "
            f"{self._order_confirm_timeout_s}s bestätigt (status='{last_status}')",
            status=last_status or "Timeout",
            order_id=str(parent_id),
            last_error_code=self._last_error_code,
            last_error_msg=self._last_error_msg,
        )

    def _cancel_order_safe(self, trade) -> None:
        """Stille Cancel-Hilfe für Cleanup nach fehlgeschlagenem Submit."""
        try:
            self.ib.cancelOrder(trade.order)
        except Exception as e:  # noqa: BLE001
            log.warning("ibkr.cancel_on_error_failed",
                        order_id=getattr(trade.order, "orderId", None),
                        error=str(e))

    async def submit_order(self, req: OrderRequest) -> str:
        asset_class = str(req.metadata.get("asset_class", "equity"))
        contract = build_contract(req.symbol, asset_class, req.metadata)
        action = "BUY" if req.side == OrderSide.BUY else "SELL"
        ref = _build_order_ref(self._bot_id, req.symbol, action,
                               self._order_prefix)
        order_type = (req.order_type or "market").strip().lower()
        tif = (req.time_in_force or "day").strip().lower()

        def place():
            self._ensure_connected()
            resolved_contract = qualify_contract(self.ib, contract, asset_class)
            if resolved_contract is None:
                raise OrderSubmitError(
                    f"Kein qualifizierbarer IBKR-Contract für "
                    f"{req.symbol} ({asset_class})",
                    status="NoContract",
                )

            has_bracket = bool(req.stop_loss or req.take_profit)
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
            parent.transmit = not has_bracket
            parent_trade = self.ib.placeOrder(resolved_contract, parent)
            transmit_trade = parent_trade

            opposite = "SELL" if action == "BUY" else "BUY"
            child_trades = []
            try:
                if req.stop_loss:
                    sl = StopOrder(opposite, req.qty, req.stop_loss)
                    sl.parentId = parent_trade.order.orderId
                    sl.transmit = not bool(req.take_profit)
                    sl.orderRef = f"{ref}|SL"
                    sl_trade = self.ib.placeOrder(resolved_contract, sl)
                    child_trades.append(sl_trade)
                    if sl.transmit:
                        transmit_trade = sl_trade
                if req.take_profit:
                    tp = LimitOrder(opposite, req.qty, req.take_profit)
                    tp.parentId = parent_trade.order.orderId
                    tp.transmit = True
                    tp.orderRef = f"{ref}|TP"
                    tp_trade = self.ib.placeOrder(resolved_contract, tp)
                    child_trades.append(tp_trade)
                    transmit_trade = tp_trade
            except Exception as e:  # noqa: BLE001
                # Parent wurde ggf. mit transmit=False gesendet – bleibt
                # sonst als „Floating"-Order bei IBKR stehen.
                log.error("ibkr.bracket_leg_failed",
                          symbol=req.symbol, error=str(e))
                self._cancel_order_safe(parent_trade)
                for ct in child_trades:
                    self._cancel_order_safe(ct)
                raise OrderSubmitError(
                    f"Bracket-Leg konnte nicht platziert werden: {e}",
                    status="BracketSetupFailed",
                    order_id=str(parent_trade.order.orderId),
                    last_error_code=self._last_error_code,
                    last_error_msg=self._last_error_msg,
                ) from e

            # Bestätigungsphase: Parent muss ACCEPTED sein
            try:
                status = self._wait_for_order_confirmation(parent_trade, transmit_trade)
            except OrderSubmitError:
                self._cancel_order_safe(parent_trade)
                for ct in child_trades:
                    self._cancel_order_safe(ct)
                raise

            return parent_trade, status

        trade, status = await self._run(lambda: place())
        log.info("ibkr.order.submitted", symbol=req.symbol, side=action,
                 qty=req.qty, order_id=trade.order.orderId, ref=ref,
                 order_type=order_type, tif=tif, status=status)
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

    def _owns_ref(self, order_ref: str) -> bool:
        """True wenn der orderRef-String zu diesem Bot gehört.

        Nutzt bot_id- und optionalen order_prefix-Präfix. Wenn beide leer
        sind, gilt der Ref als „eigen" – so bleibt das Verhalten für Legacy-
        Setups unverändert.
        """
        if not order_ref:
            return False
        if self._order_prefix and order_ref.startswith(self._order_prefix):
            return True
        if self._bot_id and order_ref.startswith(self._bot_id):
            return True
        return False

    async def get_position(self, symbol: str) -> Optional[Position]:
        positions = await self.get_positions()
        return positions.get(symbol)

    async def get_positions(self) -> dict[str, Position]:
        def fetch():
            self._ensure_connected()
            out: dict[str, Position] = {}
            for p in self.ib.positions():
                if p.contract.secType not in ("STK", "FUT", "CRYPTO"):
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
        # Asset-class aus IB-Position ableiten (STK/FUT/CRYPTO), damit der
        # Gegen-Trade den passenden Contract-Typ benutzt.
        meta: dict = {}
        for p in self.ib.positions():
            if getattr(p.contract, "symbol", "").upper() == symbol.upper():
                sec_type = getattr(p.contract, "secType", "STK")
                meta["asset_class"] = {
                    "STK": "equity",
                    "FUT": "futures",
                    "CRYPTO": "crypto",
                }.get(sec_type, "equity")
                if sec_type == "FUT":
                    meta["futures_exchange"] = getattr(
                        p.contract, "exchange", "CME",
                    )
                if sec_type == "CRYPTO":
                    meta["crypto_quote_currency"] = getattr(
                        p.contract, "currency", "USD",
                    )
                break
        req = OrderRequest(
            symbol=symbol,
            side=OrderSide.SELL if action == "SELL" else OrderSide.BUY,
            qty=max(1, int(round(pos.qty))),
            order_type="market",
            metadata=meta,
        )
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

    # ── Session-Health (BrokerPort-Hook) ────────────────────────────

    async def health(self) -> dict:
        def fetch():
            connected = bool(self.ib.isConnected())
            accounts: list[str] = []
            try:
                accounts = list(self.ib.managedAccounts() or [])
            except Exception:  # noqa: BLE001
                pass
            return {
                "connected": connected,
                "session_healthy": bool(connected and self._session_healthy),
                "last_error_code": self._last_error_code,
                "last_error_msg": self._last_error_msg,
                "managed_accounts": accounts,
            }

        try:
            return await asyncio.get_event_loop().run_in_executor(None, fetch)
        except Exception as e:  # noqa: BLE001
            log.warning("ibkr.health_failed", error=str(e))
            return {
                "connected": False,
                "session_healthy": False,
                "last_error_code": self._last_error_code,
                "last_error_msg": self._last_error_msg or str(e),
                "managed_accounts": [],
            }

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
                if getattr(contract, "secType", "") not in ("STK", "FUT", "CRYPTO"):
                    continue

                sym = str(getattr(contract, "symbol", "")).upper()
                if not sym:
                    continue
                if symbol_set and sym not in symbol_set:
                    continue
                if sym in out:
                    continue

                # Multi-Bot-Isolation: nur Fills mit passendem orderRef-Prefix
                # akzeptieren; so werden Fremd-Bot-Fills nie als eigene
                # verbucht, wenn beide auf demselben IBKR-Paper-Account laufen.
                order_ref = str(getattr(execution, "orderRef", "") or "")
                if not order_ref:
                    order_ref = str(
                        getattr(getattr(fill, "order", None), "orderRef", "")
                        or ""
                    )
                if order_ref and not self._owns_ref(order_ref):
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

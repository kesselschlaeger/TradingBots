"""BrokerPort – abstraktes Broker-Interface + Standard execute_signal."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from core.logging import get_logger
from core.models import (
    CloseExecution,
    ExecutionResult,
    OrderRequest,
    OrderSide,
    PairSignal,
    Position,
    Signal,
)
from core.risk import fixed_fraction_size, position_size

log = get_logger(__name__)


class BrokerPort(ABC):
    """Minimaler Vertrag für jeden Broker-Adapter (Alpaca, IBKR, Paper)."""

    paper: bool

    # ── Order-Execution ──────────────────────────────────────────────

    @abstractmethod
    async def submit_order(self, req: OrderRequest) -> str:
        """Liefert Broker-Order-ID zurück."""

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        ...

    @abstractmethod
    async def cancel_all_orders(self) -> None:
        ...

    # ── Positionen & Konto ──────────────────────────────────────────

    @abstractmethod
    async def get_position(self, symbol: str) -> Optional[Position]:
        ...

    @abstractmethod
    async def get_positions(self) -> dict[str, Position]:
        ...

    @abstractmethod
    async def get_account(self) -> dict:
        """{'equity', 'cash', 'buying_power', 'paper'}"""

    @abstractmethod
    async def close_position(self, symbol: str) -> bool:
        ...

    @abstractmethod
    async def close_all_positions(self) -> dict:
        """{'attempted': [...], 'remaining': [...], 'ok': bool}"""

    @abstractmethod
    async def is_shortable(self, symbol: str) -> bool:
        ...

    async def get_recent_closes(
        self,
        symbols: Optional[list[str]] = None,
    ) -> dict[str, CloseExecution]:
        """Optionale Hook-Methode fuer exakte Exit-Fills.

        Default: keine Daten. Adapter koennen dies fuer praezise
        Telegram-CLOSE-Meldungen mit echten Fill-Preisen implementieren.
        """
        return {}

    # ── Preis-Update Hook (für Paper-/Backtest-Adapter) ─────────────

    def update_price(self, symbol: str, price: float) -> None:
        """Aktueller Marktpreis für ein Symbol (nur PaperAdapter implementiert dies)."""

    # ── Standard-Implementierung ────────────────────────────────────

    async def execute_signal(
        self,
        signal: Signal,
        account_equity: float,
        risk_pct: float = 0.01,
        max_equity_at_risk: float = 0.05,
        max_position_value_pct: float = 0.25,
    ) -> Optional[ExecutionResult]:
        """Signal -> OrderRequest -> submit_order.

        Subklassen überschreiben i.d.R. nur die einzelnen API-Methoden
        (submit_order, get_account, ...), nicht diese.
        """
        if signal.direction == 0:
            return None

        entry = float(signal.metadata.get("entry_price", signal.stop_price))

        # Sizing: bevorzugt qty_hint aus metadata (OBB), sonst R-basiert.
        qty_hint = signal.metadata.get("qty_hint")
        if qty_hint is not None:
            qty = int(qty_hint)
        elif signal.stop_price and signal.stop_price > 0:
            qty = position_size(
                equity=account_equity,
                risk_pct=risk_pct * max(signal.strength, 0.0),
                entry=entry,
                stop=signal.stop_price,
                max_equity_at_risk=max_equity_at_risk,
                max_position_value_pct=max_position_value_pct,
            )
        else:
            # Kein Stop, kein Hint -> fallback auf fixen Prozent-Anteil
            qty = fixed_fraction_size(account_equity, entry,
                                      max_position_value_pct)

        qty_factor = float(signal.metadata.get("qty_factor", 1.0))
        qty = max(0, int(qty * max(qty_factor, 0.0)))
        if qty < 1:
            log.info("broker.execute_signal.zero_qty",
                     symbol=signal.symbol, reason="qty<1")
            return None

        side = OrderSide.BUY if signal.direction > 0 else OrderSide.SELL
        # Asset-class-spezifische Kontext-Keys werden an den Broker-Adapter
        # weitergereicht (→ build_contract in execution.contract_factory).
        order_meta = {
            "asset_class": signal.metadata.get("asset_class", "equity"),
            "futures_exchange": signal.metadata.get("futures_exchange", "CME"),
            "crypto_quote_currency": signal.metadata.get(
                "crypto_quote_currency", "USD",
            ),
        }
        ibkr_crypto_symbol = signal.metadata.get("ibkr_crypto_symbol")
        if ibkr_crypto_symbol:
            order_meta["ibkr_crypto_symbol"] = ibkr_crypto_symbol
        req = OrderRequest(
            symbol=signal.symbol,
            side=side,
            qty=qty,
            order_type=signal.metadata.get("order_type", "market"),
            limit_price=signal.metadata.get("limit_price"),
            stop_loss=signal.stop_price if signal.stop_price > 0 else None,
            take_profit=signal.target_price,
            time_in_force=signal.metadata.get("time_in_force", "day"),
            client_order_id=signal.metadata.get("client_order_id"),
            metadata=order_meta,
        )
        order_id = await self.submit_order(req)
        return ExecutionResult(
            order_id=order_id,
            qty=qty,
            order_type=req.order_type,
            time_in_force=req.time_in_force,
        )

    # ── Pair-Signal Execution ──────────────��────────────────────────────

    async def execute_pair_signal(
        self,
        signal: PairSignal,
        equity: float,
    ) -> tuple[Optional[dict], Optional[dict]]:
        """Führt Long- und Short-Leg atomar aus.

        Gibt (long_result, short_result) zurück.
        Bei Failure eines Legs: anderen sofort stornieren (kein Leg-Mismatch).
        Default-Implementierung in BrokerPort – Adapter erben sie.
        """
        atr_pct = signal.features.atr_pct
        if atr_pct <= 0:
            atr_pct = 0.01
        qty = max(1, int(equity * signal.qty_pct / atr_pct))

        long_req = OrderRequest(
            symbol=signal.long_symbol,
            side=OrderSide.BUY,
            qty=qty,
        )
        short_req = OrderRequest(
            symbol=signal.short_symbol,
            side=OrderSide.SELL,
            qty=qty,
        )

        long_id = await self.submit_order(long_req)
        if long_id is None:
            log.warning("pair.long_leg_failed", symbol=signal.long_symbol)
            return None, None

        short_id = await self.submit_order(short_req)
        if short_id is None:
            await self.cancel_order(long_id)
            log.warning("pair.short_leg_failed_rollback",
                        long_symbol=signal.long_symbol,
                        short_symbol=signal.short_symbol)
            return None, None

        long_r = {"id": long_id, "symbol": signal.long_symbol,
                  "side": "long", "qty": qty}
        short_r = {"id": short_id, "symbol": signal.short_symbol,
                   "side": "short", "qty": qty}
        return long_r, short_r

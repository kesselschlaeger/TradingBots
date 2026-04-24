"""Shared IBKR contract factory.

Wird von ``execution/ibkr_adapter.py`` (Order-Submission) und
``data/providers/ibkr_provider.py`` (Bar-Abruf) genutzt. Die eigentliche
Asset-Class-Logik lebt in den Strategien – hier wird nur das
ib_insync-Objekt erzeugt.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from core.logging import get_logger

log = get_logger(__name__)

try:
    from ib_insync import Stock, Future
    IBKR_AVAILABLE = True
except ImportError:
    IBKR_AVAILABLE = False
    Stock = None  # type: ignore
    Future = None  # type: ignore

try:
    from ib_insync import Crypto  # type: ignore
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    Crypto = None  # type: ignore


def build_contract(
    symbol: str,
    asset_class: str = "equity",
    cfg: Optional[dict] = None,
) -> Any:
    """Liefert das passende ib_insync-Contract-Objekt.

    - equity:  Stock(symbol, 'SMART', 'USD')
    - futures: Future(symbol, exchange, 'USD'), Front-Month via IBKR
    - crypto:  Crypto(symbol, 'PAXOS', quote) mit Fallback auf Stock

    Der Fallback-Stock-Contract für Crypto wird benötigt, weil ältere
    ib_insync-Versionen keinen Crypto-Typ haben.
    """
    if not IBKR_AVAILABLE:
        raise RuntimeError("ib_insync fehlt – pip install ib_insync")

    cfg = cfg or {}
    ac = (asset_class or "equity").lower()
    sym = symbol.upper()

    if ac == "futures":
        exchange = cfg.get("futures_exchange", "CME")
        return Future(
            symbol=sym,
            exchange=exchange,
            currency="USD",
            lastTradeDateOrContractMonth=(
                cfg.get("futures_contract_month")
                or cfg.get("lastTradeDateOrContractMonth")
                or ""
            ),
            localSymbol=cfg.get("futures_local_symbol", ""),
            tradingClass=cfg.get("futures_trading_class", ""),
            multiplier=(
                str(cfg.get("futures_multiplier"))
                if cfg.get("futures_multiplier") is not None else ""
            ),
        )

    if ac == "crypto":
        quote = cfg.get("crypto_quote_currency", "USD")
        ib_sym = cfg.get("ibkr_crypto_symbol", sym)
        if CRYPTO_AVAILABLE:
            return Crypto(symbol=ib_sym, exchange="PAXOS", currency=quote)
            #return Crypto(symbol=ib_sym, exchange="SMART", currency=quote)
        log.warning(
            "contract_factory.crypto_fallback_stock",
            symbol=ib_sym,
            reason="ib_insync ohne Crypto-Klasse, nutze Stock auf PAXOS",
        )
        return Stock(ib_sym, "PAXOS", quote)

    # equity (default)
    return Stock(sym, "SMART", "USD")


def qualify_contract(
    ib: Any,
    contract: Any,
    asset_class: str = "equity",
) -> Any:
    """Qualifiziere einen Contract; Futures ohne Expiry werden auf Front-Month aufgelöst."""
    ac = (asset_class or "equity").lower()
    if ac != "futures":
        qualified = ib.qualifyContracts(contract)
        return qualified[0] if qualified else None

    expiry = str(getattr(contract, "lastTradeDateOrContractMonth", "") or "").strip()
    local_symbol = str(getattr(contract, "localSymbol", "") or "").strip()
    if expiry or local_symbol:
        qualified = ib.qualifyContracts(contract)
        return qualified[0] if qualified else None

    details = ib.reqContractDetails(contract)
    if not details:
        return None

    now = datetime.now(timezone.utc).strftime("%Y%m%d")

    def _sort_key(detail: Any) -> tuple[int, str, int]:
        candidate = detail.contract
        raw_expiry = str(
            getattr(candidate, "lastTradeDateOrContractMonth", "") or ""
        ).strip()
        normalized = raw_expiry[:8] if len(raw_expiry) >= 8 else (raw_expiry + "01")[:8]
        is_past = 0 if normalized >= now else 1
        con_id = int(getattr(candidate, "conId", 0) or 0)
        return (is_past, normalized, con_id)

    chosen = sorted(details, key=_sort_key)[0].contract
    log.info(
        "contract_factory.futures_resolved",
        symbol=getattr(chosen, "symbol", ""),
        exchange=getattr(chosen, "exchange", ""),
        expiry=getattr(chosen, "lastTradeDateOrContractMonth", ""),
        local_symbol=getattr(chosen, "localSymbol", ""),
        trading_class=getattr(chosen, "tradingClass", ""),
    )
    qualified = ib.qualifyContracts(chosen)
    return qualified[0] if qualified else chosen

"""Shared IBKR contract factory.

Wird von ``execution/ibkr_adapter.py`` (Order-Submission) und
``data/providers/ibkr_provider.py`` (Bar-Abruf) genutzt. Die eigentliche
Asset-Class-Logik lebt in den Strategien – hier wird nur das
ib_insync-Objekt erzeugt.
"""
from __future__ import annotations

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
        return Future(symbol=sym, exchange=exchange, currency="USD")

    if ac == "crypto":
        quote = cfg.get("crypto_quote_currency", "USD")
        ib_sym = cfg.get("ibkr_crypto_symbol", sym)
        if CRYPTO_AVAILABLE:
            return Crypto(symbol=ib_sym, exchange="PAXOS", currency=quote)
        log.warning(
            "contract_factory.crypto_fallback_stock",
            symbol=ib_sym,
            reason="ib_insync ohne Crypto-Klasse, nutze Stock auf PAXOS",
        )
        return Stock(ib_sym, "PAXOS", quote)

    # equity (default)
    return Stock(sym, "SMART", "USD")

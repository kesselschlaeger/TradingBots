"""
obb_broker_base.py – Abstraktes Broker-Interface für den One-Bar-Breakout Bot.

Definiert den Vertrag, den AlpacaClientDaily und IBKRClientDaily erfüllen müssen.
Unterschied zu orb_broker_base: Daily-Bars statt 5m-Bars, einfache Market-Orders
statt Bracket-Orders, close_position() für Einzel-Symbols.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import pandas as pd


class OBBBrokerBase(ABC):
    """Abstrakte Basisklasse für OBB-Broker-Implementierungen (Daily-Bar-basiert)."""

    # Subklassen MÜSSEN self.paper im __init__ setzen (bool: Paper-/Simulations-Modus)
    paper: bool

    # ── Marktdaten ──────────────────────────────────────────────────────────

    @abstractmethod
    def fetch_daily_bars(self, symbol: str, days: int = 80) -> pd.DataFrame:
        """Tägliche OHLCV-Bars für ein Symbol.
        Spalten: Open, High, Low, Close, Volume. DatetimeIndex."""

    @abstractmethod
    def fetch_daily_bars_bulk(self, symbols: List[str], start: str,
                              end: str) -> Dict[str, pd.DataFrame]:
        """Historische Daily-Bars für mehrere Symbole (für Backtest-Datenabruf)."""

    # ── Kontoinformationen ──────────────────────────────────────────────────

    @abstractmethod
    def get_equity(self) -> float:
        """Aktueller Konto-Equity-Wert."""

    @abstractmethod
    def get_cash(self) -> float:
        """Verfügbares Cash im Konto."""

    @abstractmethod
    def get_buying_power(self) -> float:
        """Kaufkraft (Buying Power) des Kontos."""

    # ── Positionen & Orders ─────────────────────────────────────────────────

    @abstractmethod
    def sync_positions(self) -> Dict[str, dict]:
        """
        Aktuell gehaltene Positionen vom Broker.
        Rückgabe: {symbol: {"qty": float, "side": "long"|"short",
                             "entry": float, "current_price": float,
                             "unrealized_pnl": float, "market_value": float}}
        """

    @abstractmethod
    def is_shortable(self, symbol: str) -> bool:
        """True wenn Symbol für Short-Verkauf verfügbar."""

    @abstractmethod
    def get_open_orders(self) -> List[dict]:
        """Offene Orders. Jedes dict: {"id", "symbol", "side", "qty", "status"}"""

    # ── Order-Execution ─────────────────────────────────────────────────────

    @abstractmethod
    def place_market_order(self, symbol: str, qty: int, side: str,
                           time_in_force: str = "day",
                           client_order_id: str = "") -> dict:
        """
        Einfache Market-Order.
        side: "buy" | "sell"
        time_in_force: "day" | "opg" | "cls" | "gtc"
        Rückgabe: {"ok": True, "id": str, ...} oder {"ok": False, "error": str}
        """

    # ── Positionsmanagement ─────────────────────────────────────────────────

    @abstractmethod
    def cancel_all_orders(self) -> None:
        """Alle offenen Orders dieser Instanz stornieren."""

    @abstractmethod
    def close_position(self, symbol: str) -> dict:
        """Einzelne Position schließen. Rückgabe: {"ok": bool, ...}"""

    @abstractmethod
    def close_all_positions(self) -> dict:
        """Alle Positionen schließen. Rückgabe: {"ok": bool, "closed": [...]}"""

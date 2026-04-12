"""
orb_broker_base.py – Abstraktes Broker-Interface für den ORB Bot.

Definiert den Vertrag, den AlpacaClient und IBKRClient erfüllen müssen,
damit ORB_Bot broker-agnostisch arbeiten kann.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import pandas as pd


class BrokerBase(ABC):
    """Abstrakte Basisklasse für alle Broker-Implementierungen."""

    # Subklassen MÜSSEN self.paper im __init__ setzen (bool: Paper-/Simulations-Modus)
    paper: bool

    # ── Marktdaten ──────────────────────────────────────────────────────────

    @abstractmethod
    def fetch_bars(self, symbol: str, days: int = 2) -> pd.DataFrame:
        """5m-OHLCV-Bars für ein Symbol.
        Spalten: Open, High, Low, Close, Volume. DatetimeIndex UTC."""

    @abstractmethod
    def check_bar_freshness(self, df: pd.DataFrame,
                            max_delay_minutes: int = 20) -> bool:
        """True wenn letzter Bar frisch genug, False wenn stale."""

    @abstractmethod
    def fetch_bars_bulk(self, symbols: List[str], start: str,
                        end: str) -> Dict[str, pd.DataFrame]:
        """Historische 5m-Bars für mehrere Symbole (für Backtest-Datenabruf)."""

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
    def place_long_bracket(self, symbol: str, qty: int, stop_loss: float,
                           take_profit: float,
                           client_order_id: str = None) -> dict:
        """
        Long-Entry als Bracket-Order (Market-Entry + Stop-Loss + Take-Profit).
        Rückgabe: {"ok": True, "id": str, ...} oder {"ok": False, "error": str}
        """

    @abstractmethod
    def place_short_bracket(self, symbol: str, qty: int, stop_loss: float,
                            take_profit: float,
                            client_order_id: str = None) -> dict:
        """
        Short-Entry als Bracket-Order (Market-Entry + Stop-Loss + Take-Profit).
        stop_loss liegt ÜBER dem Entry-Preis, take_profit DARUNTER.
        Rückgabe: {"ok": True, "id": str, ...} oder {"ok": False, "error": str}
        """

    # ── Order-Management ────────────────────────────────────────────────────

    @abstractmethod
    def cancel_all_orders(self) -> None:
        """Alle offenen Orders dieser Instanz stornieren."""

    @abstractmethod
    def close_all_positions(self, verify: bool = True) -> dict:
        """
        EOD: alle Positionen dieser Instanz schließen + offene Orders stornieren.
        Rückgabe: {"attempted": [syms], "remaining": [syms], "ok": bool}
        """

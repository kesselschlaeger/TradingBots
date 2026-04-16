"""Execution: BrokerPort-Interface + Adapter (Paper, Alpaca, IBKR)."""
from execution.port import BrokerPort  # noqa: F401
from execution.paper_adapter import PaperAdapter  # noqa: F401

__all__ = ["BrokerPort", "PaperAdapter", "AlpacaAdapter", "IBKRAdapter"]


def __getattr__(name: str):
    # Lazy-Import, damit optionale Broker-Pakete nicht bei jedem
    # `from execution import ...` benötigt werden.
    if name == "AlpacaAdapter":
        from execution.alpaca_adapter import AlpacaAdapter
        return AlpacaAdapter
    if name == "IBKRAdapter":
        from execution.ibkr_adapter import IBKRAdapter
        return IBKRAdapter
    raise AttributeError(name)

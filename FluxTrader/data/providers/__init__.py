"""Data Providers: AlpacaDataProvider, YFinanceDataProvider, IBKRDataProvider."""
from data.providers.base import DataProvider  # noqa: F401

__all__ = [
    "DataProvider",
    "AlpacaDataProvider",
    "YFinanceDataProvider",
    "IBKRDataProvider",
]


def __getattr__(name: str):
    if name == "AlpacaDataProvider":
        from data.providers.alpaca_provider import AlpacaDataProvider
        return AlpacaDataProvider
    if name == "YFinanceDataProvider":
        from data.providers.yfinance_provider import YFinanceDataProvider
        return YFinanceDataProvider
    if name == "IBKRDataProvider":
        from data.providers.ibkr_provider import IBKRDataProvider
        return IBKRDataProvider
    raise AttributeError(name)

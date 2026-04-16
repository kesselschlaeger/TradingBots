"""Strategy-Registry via @register-Decorator."""
from __future__ import annotations

from typing import Optional, Type

from core.context import MarketContextService
from strategy.base import BaseStrategy

_REGISTRY: dict[str, Type[BaseStrategy]] = {}


def register(name: str):
    """Class-Decorator: registriert die Strategy unter `name`.

    Beispiel:
        @register("orb")
        class ORBStrategy(BaseStrategy): ...
    """
    def decorator(cls: Type[BaseStrategy]) -> Type[BaseStrategy]:
        if not issubclass(cls, BaseStrategy):
            raise TypeError(f"{cls.__name__} muss BaseStrategy erben")
        if name in _REGISTRY and _REGISTRY[name] is not cls:
            raise ValueError(f"Strategy '{name}' doppelt registriert")
        _REGISTRY[name] = cls
        return cls
    return decorator


class StrategyRegistry:
    @staticmethod
    def get(name: str, config: dict,
            context: Optional[MarketContextService] = None) -> BaseStrategy:
        if name not in _REGISTRY:
            raise KeyError(
                f"Strategy '{name}' nicht registriert. "
                f"Verfügbar: {list(_REGISTRY)}"
            )
        return _REGISTRY[name](config, context=context)

    @staticmethod
    def available() -> list[str]:
        return list(_REGISTRY.keys())

    @staticmethod
    def classes() -> dict[str, Type[BaseStrategy]]:
        return dict(_REGISTRY)

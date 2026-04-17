"""Strategy-Registry via @register-Decorator."""
from __future__ import annotations

from typing import Optional, Type, Union

from core.context import MarketContextService
from strategy.base import BaseStrategy, PairStrategy

_REGISTRY: dict[str, Type[Union[BaseStrategy, PairStrategy]]] = {}


def register(name: str):
    """Class-Decorator: registriert die Strategy unter `name`.

    Beispiel:
        @register("orb")
        class ORBStrategy(BaseStrategy): ...

        @register("botti_pair")
        class BottiPairStrategy(PairStrategy): ...
    """
    def decorator(cls):
        if not (issubclass(cls, BaseStrategy) or issubclass(cls, PairStrategy)):
            raise TypeError(
                f"{cls.__name__} muss BaseStrategy oder PairStrategy erben"
            )
        if name in _REGISTRY and _REGISTRY[name] is not cls:
            raise ValueError(f"Strategy '{name}' doppelt registriert")
        _REGISTRY[name] = cls
        return cls
    return decorator


class StrategyRegistry:
    @staticmethod
    def get(name: str, config: dict,
            context: Optional[MarketContextService] = None
            ) -> Union[BaseStrategy, PairStrategy]:
        if name not in _REGISTRY:
            raise KeyError(
                f"Strategy '{name}' nicht registriert. "
                f"Verfügbar: {list(_REGISTRY)}"
            )
        return _REGISTRY[name](config, context=context)

    @staticmethod
    def get_pair(name: str, config: dict,
                 context: Optional[MarketContextService] = None
                 ) -> PairStrategy:
        """Wie get(), aber gibt garantiert eine PairStrategy zurück."""
        strat = StrategyRegistry.get(name, config, context=context)
        if not isinstance(strat, PairStrategy):
            raise TypeError(f"'{name}' ist keine PairStrategy")
        return strat

    @staticmethod
    def available() -> list[str]:
        return list(_REGISTRY.keys())

    @staticmethod
    def classes() -> dict[str, Type[Union[BaseStrategy, PairStrategy]]]:
        return dict(_REGISTRY)

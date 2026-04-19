"""Strategy-Package.

Import löst @register-Seiteneffekte aus, damit StrategyRegistry.get()
alle Strategien sofort findet. Neue Strategien hier hinzufügen.
"""
from strategy.base import BaseStrategy, PairStrategy  # noqa: F401
from strategy.registry import StrategyRegistry, register  # noqa: F401

# Registrierungen
from strategy.orb import ORBStrategy  # noqa: F401
from strategy.obb import OBBStrategy  # noqa: F401
from strategy.botti import BottiStrategy  # noqa: F401
from strategy.botti_pair import BottiPairStrategy  # noqa: F401
from strategy.ict_ob import IctOrderBlockStrategy  # noqa: F401

__all__ = ["BaseStrategy", "PairStrategy", "StrategyRegistry", "register",
           "ORBStrategy", "OBBStrategy", "BottiStrategy", "BottiPairStrategy",
           "IctOrderBlockStrategy"]

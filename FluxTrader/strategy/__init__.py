"""Strategy-Package.

Import löst @register-Seiteneffekte aus, damit StrategyRegistry.get()
alle Strategien sofort findet. Neue Strategien hier hinzufügen.
"""
from strategy.base import BaseStrategy  # noqa: F401
from strategy.registry import StrategyRegistry, register  # noqa: F401

# Registrierungen
from strategy.orb import ORBStrategy  # noqa: F401
from strategy.obb import OBBStrategy  # noqa: F401

__all__ = ["BaseStrategy", "StrategyRegistry", "register",
           "ORBStrategy", "OBBStrategy"]

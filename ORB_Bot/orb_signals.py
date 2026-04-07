#!/usr/bin/env python3
"""
orb_signals.py – REDIRECT STUB.

Dieses Modul wurde durch orb_strategy.py ersetzt.
Alle Funktionen werden von dort re-exportiert für Abwärtskompatibilität.

Migration:
    from orb_strategy import calculate_orb_levels, compute_orb_signals, ...
"""

# Re-Export aller öffentlichen Funktionen aus orb_strategy
from orb_strategy import (  # noqa: F401
    ET,
    calculate_atr,
    calculate_orb_levels,
    compute_indicators,
    compute_orb_signals,
    prepare_orb_day,
)

"""Bot-Status per Strategie (Live vs. HealthState-Cache).

Endpunkt: /api/strategies/status – Aggregierter Broker + Circuit-Breaker
Status aus HealthState (wenn Live-Runner angebunden), sonst read-only
aus PersistentState.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

router = APIRouter(tags=["strategies"])


@router.get("/strategies/status")
async def strategies_status(request: Request) -> dict[str, Any]:
    """Broker-Status + Circuit-Breaker aus HealthState (Live-Mode).

    Falls kein Live-Runner angebunden (Standalone-Dashboard):
    Leere Liste, aber DB-Daten sind trotzdem verfügbar via
    /api/portfolio, /api/positions, /api/equity, /api/trades.
    """
    hs = request.app.state.health_state
    if hs is None:
        return {
            "strategies": [],
            "broker": None,
            "circuit_breaker": False,
            "message": (
                "HealthState nicht angebunden (Standalone-Dashboard). "
                "Live-Daten lesen Sie über /api/portfolio, /api/positions, "
                "/api/equity, /api/trades, /api/strategies/list."
            ),
            "timestamp": None,
        }

    snap = hs.snapshot()
    return {
        "strategies": snap.get("strategies", []),
        "broker": snap.get("broker", {}),
        "circuit_breaker": snap.get("circuit_breaker_active", False),
        "timestamp": snap.get("timestamp"),
    }


@router.get("/strategies/health")
async def strategies_health(request: Request) -> dict[str, Any]:
    """Detaillierter Health-Check pro Strategie.

    Versucht zuerst den lokalen HealthState (Live-Mode in selben Prozess),
    fallback ist SQLite.
    """
    hs = request.app.state.health_state

    # Fallback 1: Lokaler HealthState (selber Prozess)
    if hs is not None:
        snap = hs.snapshot()
        return {
            "available": True,
            "source": "local",
            "portfolio": snap.get("portfolio", {}),
            "strategies": snap.get("strategies", []),
            "broker": snap.get("broker", {}),
            "circuit_breaker": snap.get("circuit_breaker_active", False),
            "health_score": snap.get("health_score", 0),
            "timestamp": snap.get("timestamp"),
        }

    # Fallback 2: SQLite
    try:
        state = request.app.state.persistent_state
        instances = await state.get_bot_instances()
        items = []
        circuit_breaker_active = False
        for inst in instances:
            snap = await state.get_health_snapshot(inst["bot_name"], inst["strategy"])
            circuit_breaker_active = (
                circuit_breaker_active
                or bool(snap.get("circuit_breaker", False))
            )
            items.append({
                "name": inst["strategy"],
                "bot_name": inst["bot_name"],
                "last_bar_ts": snap.get("last_bar_ts"),
                "last_bar_lag_ms": snap.get("last_bar_lag_ms"),
                "signals_today": snap.get("signals_today", 0),
                "signals_filtered_today": snap.get("signals_filtered_today", 0),
                "symbol_status": snap.get("symbol_status", {}),
            })

        return {
            "available": bool(items),
            "source": "db",
            "portfolio": {},
            "strategies": items,
            "broker": {},
            "circuit_breaker": circuit_breaker_active,
            "health_score": 0,
            "timestamp": None,
        }
    except Exception:
        pass

    # Fallback 3: Nichts verfügbar
    return {
        "available": False,
        "source": "none",
        "message": "Live-Bot nicht verfügbar. Starten Sie: python main.py paper --config configs/orb_paper.yaml",
        "strategies": [],
        "portfolio": {},
        "broker": {},
    }

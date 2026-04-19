"""Bot-Status per Strategie (Live vs. HealthState-Cache).

Endpunkt: /api/strategies/status – Aggregierter Broker + Circuit-Breaker
Status aus HealthState (wenn Live-Runner angebunden), sonst read-only
aus PersistentState.
"""
from __future__ import annotations

from typing import Any, Optional

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
    falls nicht vorhanden, holt Daten vom Health-Server (Live-Bot auf
    localhost:8090).
    """
    import httpx

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

    # Fallback 2: Remote Health-Server (Live-Bot in anderem Prozess)
    try:
        health_url = request.app.state.health_server_url
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(f"{health_url}/status")
            if resp.status_code == 200:
                snap = resp.json()
                return {
                    "available": True,
                    "source": "remote",
                    "portfolio": snap.get("portfolio", {}),
                    "strategies": snap.get("strategies", []),
                    "broker": snap.get("broker", {}),
                    "circuit_breaker": snap.get("circuit_breaker_active", False),
                    "health_score": snap.get("health_score", 0),
                    "timestamp": snap.get("timestamp"),
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

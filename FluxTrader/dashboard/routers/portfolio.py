"""Portfolio-Status: Live-Positionen + Equity-Kurven pro Strategie.

Endpunkte:
- /api/portfolio – Gesamtstatus (Peak-Equity, HealthState wenn vorhanden)
- /api/positions – Offene Positionen mit Live-Unrealized-PnL
- /api/equity – Equity-Zeitreihe pro Strategie (für Chart)
- /api/strategies – Liste aller aktiven Strategien + ihre Bot-Status
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Query, Request

router = APIRouter(tags=["portfolio"])


async def _get_active_strategy_names(request: Request) -> set[str]:
    """Bestimmt aktive Strategien aus Live-Health (lokal oder remote)."""
    hs = request.app.state.health_state
    if hs is not None:
        snap = hs.snapshot()
        return {
            str(s.get("name", "")).strip()
            for s in snap.get("strategies", [])
            if str(s.get("name", "")).strip()
        }

    # Standalone-Dashboard: versuche Health-Server des Live-Bots
    try:
        import httpx

        health_url = request.app.state.health_server_url
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(f"{health_url}/status")
            if resp.status_code != 200:
                return set()
            snap = resp.json()
            return {
                str(s.get("name", "")).strip()
                for s in snap.get("strategies", [])
                if str(s.get("name", "")).strip()
            }
    except Exception:
        return set()


@router.get("/portfolio")
async def get_portfolio(request: Request) -> dict[str, Any]:
    """Gesamt-Portfolio aus PersistentState + optional HealthState.

    Zeigt:
    - peak_equity: globales Allzeit-High
    - open_positions: aktuelle Anzahl
    - equity: aktuelle Equity aus letztem Snapshot
    - drawdown_pct: aktueller Drawdown
    - health: HealthState-Snapshot (falls vorhanden)
    """
    hs = request.app.state.health_state
    state = request.app.state.persistent_state
    peak = await state.get_peak_equity()

    # Letzter Equity-Snapshot über alle Strategien
    latest_equity = {
        "equity": 0.0,
        "cash": 0.0,
        "drawdown_pct": 0.0,
        "open_positions": 0,
    }
    strats = await state.get_strategies()
    if strats:
        # Aggregier alle offenen Positionen
        all_positions = []
        for s in strats:
            all_positions.extend(await state.get_open_positions(s))
        latest_equity["open_positions"] = len(all_positions)

        # Letzter Snapshot (von beliebiger Strategie)
        for s in strats:
            curve = await state.get_latest_equity_curve(s, limit=1)
            if curve:
                latest_equity.update({
                    "equity": float(curve[-1].get("equity", 0.0)),
                    "cash": float(curve[-1].get("cash", 0.0)),
                    "drawdown_pct": float(curve[-1].get("drawdown_pct", 0.0)),
                })
                break

    snap = hs.snapshot() if hs is not None else {}
    return {
        "peak_equity": peak,
        "latest_equity": latest_equity.get("equity", 0.0),
        "open_positions": latest_equity.get("open_positions", 0),
        "drawdown_pct": latest_equity.get("drawdown_pct", 0.0),
        "cash": latest_equity.get("cash", 0.0),
        "health": snap,
    }


@router.get("/positions")
async def get_positions(
    request: Request,
    strategy: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Offene Positionen mit Live-PnL aus der zentralen DB.

    Fields:
    - strategy, symbol, side, entry_price, qty
    - stop_price, current_price, unrealized_pnl, unrealized_pnl_pct
    - held_minutes (wie lange die Position offen ist)
    """
    state = request.app.state.persistent_state

    if strategy:
        positions = await state.get_open_positions(strategy)
    else:
        # Alle offenen Positionen über alle Strategien
        strats = await state.get_strategies()
        positions = []
        for s in strats:
            positions.extend(await state.get_open_positions(s))

    result = []
    for p in positions:
        result.append({
            "strategy": p.get("strategy"),
            "symbol": p.get("symbol"),
            "side": p.get("side"),
            "entry_price": float(p.get("entry_price") or 0.0),
            "qty": float(p.get("qty") or 0.0),
            "stop_price": float(p.get("stop_price") or 0.0),
            "current_price": float(p.get("current_price") or 0.0),
            "unrealized_pnl": float(p.get("unrealized_pnl") or 0.0),
            "unrealized_pnl_pct": float(p.get("unrealized_pnl_pct") or 0.0),
            "held_minutes": p.get("held_minutes") or 0,
            "last_update_ts": p.get("last_update_ts"),
        })

    return result


@router.get("/equity")
async def get_equity(
    request: Request,
    strategy: Optional[str] = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Equity-Zeitreihe (hochfrequent aus equity_snapshots).

    Fields: ts, equity, cash, drawdown_pct, peak_equity, unrealized_pnl_total
    """
    state = request.app.state.persistent_state

    curve = await state.get_latest_equity_curve(strategy, limit=limit)
    result = []
    for snap in curve:
        result.append({
            "ts": snap.get("ts"),
            "equity": float(snap.get("equity") or 0.0),
            "cash": float(snap.get("cash") or 0.0),
            "drawdown_pct": float(snap.get("drawdown_pct") or 0.0),
            "peak_equity": float(snap.get("peak_equity") or 0.0),
            "unrealized_pnl_total": float(snap.get("unrealized_pnl_total") or 0.0),
        })

    return result


@router.get("/strategies/list")
async def list_strategies(
    request: Request,
    active_only: bool = Query(True),
) -> dict[str, Any]:
    """Alle aktiven Strategien + ihr aktueller Status.

    Pro Strategie:
    - equity, drawdown_pct, peak_equity
    - open_positions, trades_today, pnl_today
    - last_equity_ts (wann der letzte Snapshot war)
    - symbol_status (HealthState): {SYMBOL: {code, reason, ts}}
    """
    state = request.app.state.persistent_state

    active_names = await _get_active_strategy_names(request)
    symbol_status_by_strat = await _get_symbol_status_map(request)
    strategies = await state.get_strategies()
    result = []
    for strat in strategies:
        status = await state.get_strategy_status(strat)
        status["running"] = strat in active_names
        status["symbol_status"] = symbol_status_by_strat.get(strat, {})
        result.append(status)

    # Im Dashboard ist "Active Bots" gewünscht: nur wirklich laufende Bots.
    if active_only:
        result = [r for r in result if r.get("running", False)]

    return {
        "total_strategies": len(strategies),
        "active_strategies": len(result),
        "active_source": "health",
        "strategies": result,
    }


async def _get_symbol_status_map(request: Request) -> dict[str, dict]:
    """Sammle pro-Symbol-Status pro Strategie aus HealthState.

    Lokal bevorzugt, sonst Remote-Health-Server.
    """
    hs = request.app.state.health_state
    snapshots: list[dict[str, Any]] = []
    if hs is not None:
        snapshots = list(hs.snapshot().get("strategies", []))
    else:
        try:
            import httpx

            health_url = request.app.state.health_server_url
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(f"{health_url}/status")
                if resp.status_code == 200:
                    snapshots = list(resp.json().get("strategies", []))
        except Exception:
            snapshots = []

    out: dict[str, dict] = {}
    for s in snapshots:
        name = str(s.get("name", "")).strip()
        if name:
            out[name] = s.get("symbol_status", {}) or {}
    return out

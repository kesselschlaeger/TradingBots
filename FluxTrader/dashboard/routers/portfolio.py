"""Portfolio-Status: Live-Positionen + Equity-Kurven pro Strategie.

Endpunkte:
- /api/portfolio – Gesamtstatus (Peak-Equity, HealthState wenn vorhanden)
- /api/positions – Offene Positionen mit Live-Unrealized-PnL
- /api/equity – Equity-Zeitreihe pro Strategie (für Chart)
- /api/strategies – Liste aller aktiven Strategien + ihre Bot-Status
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Query, Request
from core.logging import get_logger

router = APIRouter(tags=["portfolio"])


async def _get_active_strategy_names(request: Request) -> set[str]:
    """Aktive Strategien via DB-Recency (kein HTTP)."""
    hs = request.app.state.health_state
    if hs is not None:
        snap = hs.snapshot()
        return {
            str(s.get("name", "")).strip()
            for s in snap.get("strategies", [])
            if str(s.get("name", "")).strip()
        }

    state = request.app.state.persistent_state
    heartbeats = await state.get_bot_heartbeats(active_only=True)
    if heartbeats:
        return {str(h.get("strategy", "")).strip() for h in heartbeats if str(h.get("strategy", "")).strip()}

    strategies = await state.get_strategies()
    active: set[str] = set()
    for strat in strategies:
        curve = await state.get_latest_equity_curve(strat, limit=1)
        if not curve:
            continue
        try:
            last_ts = datetime.fromisoformat(
                str(curve[-1]["ts"]).replace("Z", "+00:00")
            )
            age = (datetime.now(timezone.utc) - last_ts).total_seconds()
            if age < 300:
                active.add(strat)
        except (ValueError, KeyError, TypeError):
            continue
    return active


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
    - strategy/bot, symbol, side, entry_price, qty
    - stop_price, current_price, unrealized_pnl, unrealized_pnl_pct
    - order_ts, held_minutes (wie lange die Position offen ist)
    - entry_signal, entry_reason, order_reference, broker_order_id
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
        entry_ts = p.get("entry_ts")
        held_minutes = p.get("held_minutes") or 0
        if entry_ts:
            try:
                parsed = datetime.fromisoformat(str(entry_ts).replace("Z", "+00:00"))
                held_minutes = max(
                    0,
                    int((datetime.now(timezone.utc) - parsed).total_seconds() // 60),
                )
            except (TypeError, ValueError):
                pass
        result.append({
            "strategy": p.get("strategy"),
            "bot": p.get("strategy"),
            "symbol": p.get("symbol"),
            "side": p.get("side"),
            "order_ts": entry_ts,
            "entry_price": float(p.get("entry_price") or 0.0),
            "qty": float(p.get("qty") or 0.0),
            "stop_price": float(p.get("stop_price") or 0.0),
            "current_price": float(p.get("current_price") or 0.0),
            "unrealized_pnl": float(p.get("unrealized_pnl") or 0.0),
            "unrealized_pnl_pct": float(p.get("unrealized_pnl_pct") or 0.0),
            "held_minutes": held_minutes,
            "entry_signal": p.get("entry_signal"),
            "entry_reason": p.get("entry_reason"),
            "broker_order_id": p.get("broker_order_id"),
            "order_reference": p.get("order_reference") or p.get("broker_order_id"),
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


@router.get("/anomalies")
async def get_anomalies(
    request: Request,
    strategy: Optional[str] = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Letzte Anomalien aus ``anomaly_events`` fuer das Dashboard."""
    state = request.app.state.persistent_state
    anomalies = await state.get_anomalies(
        strategy=strategy,
        limit=max(1, min(int(limit), 100)),
    )

    result = []
    for item in anomalies:
        result.append({
            "id": item.get("id"),
            "strategy": item.get("strategy"),
            "ts": item.get("ts"),
            "check_name": item.get("check_name"),
            "severity": item.get("severity"),
            "symbol": item.get("symbol"),
            "message": item.get("message"),
            "context_json": item.get("context_json"),
        })
    return result


@router.get("/signals")
async def get_signals(
    request: Request,
    strategy: Optional[str] = None,
    symbol: Optional[str] = None,
    since: Optional[str] = Query(None, description="ISO-8601 timestamp"),
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Letzte Signale aus ``signals`` fuer das Dashboard."""
    state = request.app.state.persistent_state
    since_dt: Optional[datetime] = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            since_dt = None
    signals = await state.get_signals(
        strategy=strategy,
        symbol=symbol,
        since=since_dt,
        limit=max(1, min(int(limit), 200)),
    )

    result = []
    for item in signals:
        filtered_by = item.get("filtered_by")
        result.append({
            "id": item.get("id"),
            "strategy": item.get("strategy"),
            "symbol": item.get("symbol"),
            "ts": item.get("ts"),
            "action": item.get("action"),
            "strength": float(item.get("strength") or 0.0),
            "filtered_by": filtered_by,
            "filtered": bool(filtered_by),
            "mit_passed": bool(item.get("mit_passed")) if item.get("mit_passed") is not None else None,
            "ev_value": float(item.get("ev_value") or 0.0),
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
    - last_bar_ts, last_bar_lag_ms, signals_today, signals_filtered_today
    """
    state = request.app.state.persistent_state

    active_names = await _get_active_strategy_names(request)
    health_by_strat = await _get_strategy_health_map(request)
    strategies = await state.get_strategies()
    result = []
    for strat in strategies:
        status = await state.get_strategy_status(strat)
        health = health_by_strat.get(strat, {})
        status["running"] = strat in active_names
        status["symbol_status"] = health.get("symbol_status", {})
        status["last_bar_ts"] = health.get("last_bar_ts")
        status["last_bar_lag_ms"] = health.get("last_bar_lag_ms")
        status["signals_today"] = int(health.get("signals_today", 0) or 0)
        status["signals_filtered_today"] = int(
            health.get("signals_filtered_today", 0) or 0
        )
        result.append(status)

    # Im Dashboard ist "Active Bots" gewünscht: nur wirklich laufende Bots.
    if active_only:
        result = [r for r in result if r.get("running", False)]

    return {
        "total_strategies": len(strategies),
        "active_strategies": len(result),
        "active_source": "in_process" if request.app.state.health_state else "db_recency",
        "strategies": result,
    }


async def _get_strategy_health_map(request: Request) -> dict[str, dict[str, Any]]:
    """Health-Telemetrie aus SQLite (kein HTTP)."""
    log = get_logger(__name__)
    hs = request.app.state.health_state
    if hs is not None:
        snapshots = list(hs.snapshot().get("strategies", []))
        out: dict[str, dict[str, Any]] = {}
        for s in snapshots:
            name = str(s.get("name", "")).strip()
            if name:
                out[name] = {
                    "symbol_status": s.get("symbol_status", {}) or {},
                    "last_bar_ts": s.get("last_bar_ts"),
                    "last_bar_lag_ms": s.get("last_bar_lag_ms"),
                    "signals_today": s.get("signals_today", 0),
                    "signals_filtered_today": s.get("signals_filtered_today", 0),
                }
        return out

    state = request.app.state.persistent_state
    strategies = await state.get_strategies()
    out: dict[str, dict[str, Any]] = {}
    for strat in strategies:
        try:
            snap = await state.get_health_snapshot(strat)
            out[strat] = {
                "symbol_status": snap.get("symbol_status", {}),
                "last_bar_ts": snap.get("last_bar_ts"),
                "last_bar_lag_ms": snap.get("last_bar_lag_ms"),
                "signals_today": snap.get("signals_today", 0),
                "signals_filtered_today": snap.get("signals_filtered_today", 0),
                "broker_connected": snap.get("broker_connected", False),
                "circuit_breaker": snap.get("circuit_breaker", False),
            }
        except Exception as e:
            log.warning("health_map.db_failed", strategy=strat, error=str(e))
    return out

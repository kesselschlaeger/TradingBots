"""Portfolio-Status: Live-Positionen + Equity-Kurven pro Bot.

Endpunkte:
- /api/portfolio – Gesamtstatus (Peak-Equity, HealthState wenn vorhanden)
- /api/positions – Offene Positionen mit Live-Unrealized-PnL
- /api/equity – Equity-Zeitreihe pro Bot (für Chart)
- /api/strategies – Liste aller aktiven Bots + ihr Status
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Query, Request
from core.logging import get_logger

router = APIRouter(tags=["portfolio"])


async def _get_bot_instances(state: Any) -> list[dict[str, str]]:
    try:
        return await state.get_bot_instances()
    except Exception:
        return []


async def _get_active_bot_keys(request: Request) -> set[tuple[str, str]]:
    """Aktive (bot_name, strategy)-Paare via DB-Recency."""
    hs = request.app.state.health_state
    if hs is not None:
        snap = hs.snapshot()
        return {
            (str(s.get("bot_name", s.get("name", ""))).strip(),
             str(s.get("strategy", s.get("name", ""))).strip())
            for s in snap.get("strategies", [])
            if str(s.get("name", "")).strip()
        }

    state = request.app.state.persistent_state
    heartbeats = await state.get_bot_heartbeats(active_only=True)
    return {
        (str(h.get("bot_name", "")).strip(), str(h.get("strategy", "")).strip())
        for h in heartbeats
        if str(h.get("bot_name", "")).strip()
    }


@router.get("/portfolio")
async def get_portfolio(request: Request) -> dict[str, Any]:
    """Gesamt-Portfolio aus PersistentState + optional HealthState."""
    hs = request.app.state.health_state
    state = request.app.state.persistent_state
    instances = await _get_bot_instances(state)

    latest_equity = {
        "equity": 0.0,
        "cash": 0.0,
        "drawdown_pct": 0.0,
        "open_positions": 0,
        "peak_equity": 0.0,
    }

    for inst in instances:
        bn, strat = inst["bot_name"], inst["strategy"]
        positions = await state.get_open_positions(bn, strat)
        latest_equity["open_positions"] += len(positions)

        peak = await state.get_peak_equity(bn, strat)
        if peak > latest_equity["peak_equity"]:
            latest_equity["peak_equity"] = peak

        curve = await state.get_latest_equity_curve(bn, strat, limit=1)
        if curve:
            latest_equity.update({
                "equity": latest_equity["equity"] + float(curve[-1].get("equity", 0.0)),
                "cash": latest_equity["cash"] + float(curve[-1].get("cash", 0.0)),
            })

    snap = hs.snapshot() if hs is not None else {}
    return {
        "peak_equity": latest_equity["peak_equity"],
        "latest_equity": latest_equity.get("equity", 0.0),
        "open_positions": latest_equity.get("open_positions", 0),
        "drawdown_pct": latest_equity.get("drawdown_pct", 0.0),
        "cash": latest_equity.get("cash", 0.0),
        "health": snap,
    }


@router.get("/positions")
async def get_positions(
    request: Request,
    bot_name: Optional[str] = None,
    strategy: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Offene Positionen mit Live-PnL aus der zentralen DB."""
    state = request.app.state.persistent_state

    if bot_name and strategy:
        raw_positions = await state.get_open_positions(bot_name, strategy)
    else:
        instances = await _get_bot_instances(state)
        if strategy:
            instances = [i for i in instances if i["strategy"] == strategy]
        if bot_name:
            instances = [i for i in instances if i["bot_name"] == bot_name]
        raw_positions = []
        for inst in instances:
            raw_positions.extend(
                await state.get_open_positions(inst["bot_name"], inst["strategy"])
            )

    result = []
    for p in raw_positions:
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
            "bot_name": p.get("bot_name"),
            "strategy": p.get("strategy"),
            "bot": p.get("bot_name") or p.get("strategy"),
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
    bot_name: Optional[str] = None,
    strategy: Optional[str] = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Equity-Zeitreihe (hochfrequent aus equity_snapshots).

    Fields: ts, equity, cash, drawdown_pct, peak_equity, unrealized_pnl_total
    """
    state = request.app.state.persistent_state

    if bot_name and strategy:
        curve = await state.get_latest_equity_curve(bot_name, strategy, limit=limit)
    else:
        instances = await _get_bot_instances(state)
        if strategy:
            instances = [i for i in instances if i["strategy"] == strategy]
        if bot_name:
            instances = [i for i in instances if i["bot_name"] == bot_name]
        if instances:
            first = instances[0]
            curve = await state.get_latest_equity_curve(
                first["bot_name"], first["strategy"], limit=limit
            )
        else:
            curve = []

    return [
        {
            "ts": snap.get("ts"),
            "equity": float(snap.get("equity") or 0.0),
            "cash": float(snap.get("cash") or 0.0),
            "drawdown_pct": float(snap.get("drawdown_pct") or 0.0),
            "peak_equity": float(snap.get("peak_equity") or 0.0),
            "unrealized_pnl_total": float(snap.get("unrealized_pnl_total") or 0.0),
        }
        for snap in curve
    ]


@router.get("/anomalies")
async def get_anomalies(
    request: Request,
    bot_name: Optional[str] = None,
    strategy: Optional[str] = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Letzte Anomalien aus ``anomaly_events`` fuer das Dashboard."""
    state = request.app.state.persistent_state
    cap = max(1, min(int(limit), 100))

    if bot_name and strategy:
        anomalies = await state.get_anomalies(
            bot_name=bot_name, strategy=strategy, limit=cap,
        )
    else:
        instances = await _get_bot_instances(state)
        if strategy:
            instances = [i for i in instances if i["strategy"] == strategy]
        if bot_name:
            instances = [i for i in instances if i["bot_name"] == bot_name]
        anomalies = []
        for inst in instances:
            anomalies.extend(
                await state.get_anomalies(
                    bot_name=inst["bot_name"], strategy=inst["strategy"],
                    limit=cap,
                )
            )
        anomalies = sorted(anomalies, key=lambda x: x.get("ts", ""), reverse=True)[:cap]

    return [
        {
            "id": item.get("id"),
            "bot_name": item.get("bot_name"),
            "strategy": item.get("strategy"),
            "ts": item.get("ts"),
            "check_name": item.get("check_name"),
            "severity": item.get("severity"),
            "symbol": item.get("symbol"),
            "message": item.get("message"),
            "context_json": item.get("context_json"),
        }
        for item in anomalies
    ]


@router.get("/signals")
async def get_signals(
    request: Request,
    bot_name: Optional[str] = None,
    strategy: Optional[str] = None,
    symbol: Optional[str] = None,
    since: Optional[str] = Query(None, description="ISO-8601 timestamp"),
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Letzte Signale aus ``signals`` fuer das Dashboard."""
    state = request.app.state.persistent_state
    cap = max(1, min(int(limit), 200))
    since_dt: Optional[datetime] = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            since_dt = None

    if bot_name and strategy:
        signals = await state.get_signals(
            bot_name=bot_name, strategy=strategy,
            symbol=symbol, since=since_dt, limit=cap,
        )
    else:
        instances = await _get_bot_instances(state)
        if strategy:
            instances = [i for i in instances if i["strategy"] == strategy]
        if bot_name:
            instances = [i for i in instances if i["bot_name"] == bot_name]
        signals = []
        for inst in instances:
            signals.extend(
                await state.get_signals(
                    bot_name=inst["bot_name"], strategy=inst["strategy"],
                    symbol=symbol, since=since_dt, limit=cap,
                )
            )
        signals = sorted(signals, key=lambda x: x.get("ts", ""), reverse=True)[:cap]

    result = []
    for item in signals:
        filtered_by = item.get("filtered_by")
        result.append({
            "id": item.get("id"),
            "bot_name": item.get("bot_name"),
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
    """Alle aktiven Bots + ihr aktueller Status.

    Pro Bot: equity, drawdown_pct, open_positions, trades_today, pnl_today,
    last_equity_ts, symbol_status, last_bar_ts, last_bar_lag_ms, signals_today.
    """
    state = request.app.state.persistent_state
    instances = await _get_bot_instances(state)
    active_keys = await _get_active_bot_keys(request)
    health_map = await _get_bot_health_map(request)

    result = []
    for inst in instances:
        bn, strat = inst["bot_name"], inst["strategy"]
        status = await state.get_strategy_status(bn, strat)
        health = health_map.get((bn, strat), health_map.get(strat, {}))
        status["running"] = (bn, strat) in active_keys or strat in active_keys
        status["symbol_status"] = health.get("symbol_status", {})
        status["last_bar_ts"] = health.get("last_bar_ts")
        status["last_bar_lag_ms"] = health.get("last_bar_lag_ms")
        status["signals_today"] = int(health.get("signals_today", 0) or 0)
        status["signals_filtered_today"] = int(
            health.get("signals_filtered_today", 0) or 0
        )
        result.append(status)

    if active_only:
        result = [r for r in result if r.get("running", False)]

    return {
        "total_bots": len(instances),
        "active_bots": len(result),
        "active_source": "in_process" if request.app.state.health_state else "db_recency",
        "strategies": result,
    }


async def _get_bot_health_map(request: Request) -> dict[Any, dict[str, Any]]:
    """Health-Telemetrie aus SQLite, indexiert nach (bot_name, strategy)."""
    log = get_logger(__name__)
    hs = request.app.state.health_state
    if hs is not None:
        snapshots = list(hs.snapshot().get("strategies", []))
        out: dict[Any, dict[str, Any]] = {}
        for s in snapshots:
            name = str(s.get("name", "")).strip()
            if name:
                data = {
                    "symbol_status": s.get("symbol_status", {}) or {},
                    "last_bar_ts": s.get("last_bar_ts"),
                    "last_bar_lag_ms": s.get("last_bar_lag_ms"),
                    "signals_today": s.get("signals_today", 0),
                    "signals_filtered_today": s.get("signals_filtered_today", 0),
                }
                out[name] = data
                bn = str(s.get("bot_name", "")).strip()
                if bn:
                    out[(bn, name)] = data
        return out

    state = request.app.state.persistent_state
    instances = await _get_bot_instances(state)
    out: dict[Any, dict[str, Any]] = {}
    for inst in instances:
        bn, strat = inst["bot_name"], inst["strategy"]
        try:
            snap = await state.get_health_snapshot(bn, strat)
            data = {
                "symbol_status": snap.get("symbol_status", {}),
                "last_bar_ts": snap.get("last_bar_ts"),
                "last_bar_lag_ms": snap.get("last_bar_lag_ms"),
                "signals_today": snap.get("signals_today", 0),
                "signals_filtered_today": snap.get("signals_filtered_today", 0),
                "broker_connected": snap.get("broker_connected", False),
                "circuit_breaker": snap.get("circuit_breaker", False),
            }
            out[(bn, strat)] = data
            out[strat] = data
        except Exception as e:
            log.warning("health_map.db_failed", bot_name=bn, strategy=strat, error=str(e))
    return out

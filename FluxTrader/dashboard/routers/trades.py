"""Trade-History mit MIT-Qty-Factor + EV-Estimates.

Endpunkt: /api/trades – komplette Trade-Tabelle mit allen Feldern aus
der zentralen DB. Für Single-Bot oder aggregiert über mehrere Strategien.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Query, Request

router = APIRouter(tags=["trades"])


@router.get("/trades")
async def list_trades(
    request: Request,
    strategy: Optional[str] = None,
    bot_name: Optional[str] = None,
    symbol: Optional[str] = None,
    since: Optional[str] = Query(None, description="YYYY-MM-DD"),
    until: Optional[str] = Query(None, description="YYYY-MM-DD"),
    only_closed: bool = False,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Komplette Trade-Historie aus der zentralen DB mit:
    - mit_qty_factor (MIT-Overlay, 0.25–1.0)
    - ev_estimate (Expected-Value-Schätzung)
    - signal_strength (0.0–1.0)
    - group_name (für Trade-Independence)
    - features_json (Feature-Vector als JSON-String für ML-Analyse)

    Optional gefiltert nach:
    - strategy (z.B. "orb", "obb", "botti")
    - symbol (z.B. "AAPL")
    - Datum-Range [since, until]
    - only_closed: nur exits, noch keine offenen
    """
    state = request.app.state.persistent_state

    # Datum-Parsing
    start_date = None
    end_date = None
    if since:
        try:
            start_date = datetime.fromisoformat(since).replace(
                hour=0, minute=0, second=0
            )
        except ValueError:
            start_date = None
    if until:
        try:
            end_date = datetime.fromisoformat(until).replace(
                hour=23, minute=59, second=59
            )
        except ValueError:
            end_date = None

    trades = await state.get_trades(
        strategy=strategy if strategy else None,
        bot_name=bot_name if bot_name else None,
        symbol=symbol if symbol else None,
        since=start_date,
        until=end_date,
        only_closed=only_closed,
        limit=limit,
    )

    # Format für Frontend
    result = []
    for t in trades:
        result.append({
            "id": t.get("id"),
            "strategy": t.get("strategy"),
            "bot_name": t.get("bot_name") or t.get("strategy"),
            "symbol": t.get("symbol"),
            "side": t.get("side"),
            "entry_ts": t.get("entry_ts"),
            "exit_ts": t.get("exit_ts"),
            "entry_price": float(t.get("entry_price") or 0.0),
            "exit_price": float(t.get("exit_price") or 0.0),
            "qty": float(t.get("qty") or 0.0),
            "pnl": float(t.get("pnl") or 0.0),
            "pnl_pct": float(t.get("pnl_pct") or 0.0),
            "reason": t.get("reason"),
            "stop_price": float(t.get("stop_price") or 0.0),
            "signal_strength": float(t.get("signal_strength") or 0.0),
            "mit_qty_factor": float(t.get("mit_qty_factor") or 0.0),
            "ev_estimate": float(t.get("ev_estimate") or 0.0),
            "group_name": t.get("group_name"),
            "features_json": t.get("features_json"),
        })

    return result


@router.get("/trades/summary")
async def trades_summary(
    request: Request,
    strategy: Optional[str] = None,
    days: int = 30,
) -> dict[str, Any]:
    """Tägliche Trade-Statistik (Aggregat): count, pnl pro Tag/Strategie."""
    state = request.app.state.persistent_state
    today = date.today()

    summary = []
    for i in range(days, -1, -1):
        d = today - timedelta(days=i)
        pnl = await state.daily_pnl(d, strategy=strategy if strategy else None)
        by_sym = await state.trades_today(d, strategy=strategy if strategy else None)
        summary.append({
            "date": d.isoformat(),
            "pnl": float(pnl),
            "trades_count": sum(by_sym.values()),
            "by_symbol": by_sym,
        })

    return {
        "strategy": strategy or "all",
        "days": days,
        "data": summary,
    }

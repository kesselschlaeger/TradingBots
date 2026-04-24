"""Health-Overview fuer Dashboard-Ampel und aktive Alerts."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request

from live.health_eval import evaluate_liveness

router = APIRouter(tags=["health"])


@router.get("/health/overview")
async def health_overview(request: Request) -> dict[str, Any]:
    state = request.app.state.persistent_state
    cfg = request.app.state.app_config
    now = datetime.now(timezone.utc)

    rows = await state.get_liveness_view()
    items: list[dict[str, Any]] = []

    for row in rows:
        strategy_cfg = dict(cfg.strategy.params or {})
        eval_row = evaluate_liveness(
            row=row,
            strategy_cfg=strategy_cfg,
            monitoring_cfg=cfg.monitoring,
            now=now,
        )
        overall = eval_row["overall_state"]

        active_alerts: list[dict[str, Any]] = []
        if overall == "DATA_STALE":
            active_alerts.append({
                "check": "data_stale",
                "since": eval_row.get("next_expected_bar_at"),
                "level": "WARNING",
            })
        if overall == "PROCESS_DEAD":
            active_alerts.append({
                "check": "process_dead",
                "since": row.get("last_watchdog_ts") or row.get("bot_last_seen"),
                "level": "CRITICAL",
            })
        if overall == "CIRCUIT_BREAK":
            active_alerts.append({
                "check": "circuit_break",
                "since": row.get("bot_last_seen"),
                "level": "CRITICAL",
            })

        items.append({
            "bot_name": row.get("bot_name", ""),
            "strategy": row.get("strategy", ""),
            "overall_state": overall,
            "process_alive": eval_row["process_alive"],
            "data_flowing": eval_row["data_flowing"],
            "in_trade_window": eval_row["in_trade_window"],
            "last_watchdog_ts": eval_row.get("last_watchdog_ts"),
            "last_bar_ts": eval_row.get("last_bar_ts"),
            "next_expected_bar_at": eval_row.get("next_expected_bar_at"),
            "seconds_to_next_bar": eval_row.get("seconds_to_next_bar"),
            "trade_window": eval_row.get("trade_window"),
            "active_alerts": active_alerts,
        })

    return {
        "timestamp": now.isoformat(),
        "items": items,
    }

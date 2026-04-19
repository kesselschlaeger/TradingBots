"""WFO-Results-Endpunkt: /api/wfo/results."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request

router = APIRouter(tags=["wfo"])


@router.get("/wfo/results")
async def wfo_results(request: Request) -> dict[str, Any]:
    """Liefert WFO-Ergebnisse, wenn eine ``wfo_results.json`` im
    ``persistence.data_dir`` liegt."""
    cfg = request.app.state.app_config
    path = Path(cfg.persistence.data_dir) / "wfo_results.json"
    if not path.exists():
        return {"available": False, "path": str(path)}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return {"available": True, "data": json.load(f)}
    except Exception as e:  # noqa: BLE001
        return {"available": False, "error": str(e)}

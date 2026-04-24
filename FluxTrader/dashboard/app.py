"""FastAPI-App fuer das FluxTrader Web-Dashboard.

Einziger Einstiegspunkt. Laeuft per

    python main.py dashboard --config configs/botti.yaml --port 8080

Liest ausschliesslich aus ``PersistentState`` (aiosqlite) und optional
einer ``HealthState``-Instanz im gleichen Prozess. Keine Broker-Calls,
kein Import aus ``live/runner.py``.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

try:
    import fastapi  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    FASTAPI_AVAILABLE = False

from core.config import AppConfig
from core.logging import get_logger
from live.health import HealthState

log = get_logger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"


def create_app(cfg: AppConfig,
               health_state: Optional[HealthState] = None):
    """Baut die FastAPI-App. Wird von ``main.py`` im dashboard-Kommando
    aufgerufen."""
    if not FASTAPI_AVAILABLE:
        raise RuntimeError("fastapi nicht installiert – pip install fastapi uvicorn")

    # Lazy imports, damit das Modul auch ohne fastapi importierbar bleibt.
    from fastapi import FastAPI
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles

    from dashboard.routers import health, portfolio, strategies, trades, wfo
    from live.state import PersistentState

    state = PersistentState(
        Path(cfg.persistence.data_dir) / cfg.persistence.state_db
    )

    app = FastAPI(title="FluxTrader Dashboard", version="1.0.0")

    # Dependency-Injection-Stubs an die Router-Module haengen, damit
    # keine globalen Singletons noetig sind.
    app.state.persistent_state = state
    app.state.health_state = health_state  # None im Standalone-Modus
    app.state.app_config = cfg

    app.include_router(portfolio.router, prefix="/api")
    app.include_router(trades.router, prefix="/api")
    app.include_router(strategies.router, prefix="/api")
    app.include_router(health.router, prefix="/api")
    app.include_router(wfo.router, prefix="/api")

    @app.on_event("startup")
    async def _init() -> None:
        await state.init()
        log.info("dashboard.started", db=str(state.db_path))

    @app.get("/")
    async def _root() -> Any:
        idx = _STATIC_DIR / "index.html"
        if idx.exists():
            return FileResponse(idx)
        return {"message": "FluxTrader Dashboard running – static/index.html missing"}

    if _STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    return app

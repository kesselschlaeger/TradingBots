"""Pydantic-v2 Config + YAML-Loader mit Merge von base.yaml."""
from __future__ import annotations

from datetime import time
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ─────────────────────────── Nested Config Models ──────────────────────────

class BrokerConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: Literal["paper", "alpaca", "ibkr"] = "paper"
    paper: bool = True
    # Alpaca
    alpaca_data_feed: Literal["iex", "sip"] = "iex"
    # IBKR
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 4002
    ibkr_client_id: int = 1
    ibkr_bot_id: str = "FLUX"


class DataConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    provider: Literal["alpaca", "yfinance", "ibkr"] = "alpaca"
    timeframe: str = "5Min"
    lookback_days: int = 5


class NotificationConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: bool = False
    bot_name: str = ""
    telegram_token: str = ""
    telegram_chat_id: str = ""
    health_telegram_token: str = ""
    readiness_telegram_token: str = ""
    health_telegram_chat_id: str = ""
    readiness_telegram_chat_id: str = ""


class StrategyConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    symbols: list[str] = Field(default_factory=list)
    risk_pct: float = 0.01
    params: dict[str, Any] = Field(default_factory=dict)


class PersistenceConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    data_dir: str = "fluxtrader_data"
    state_db: str = "state.db"


class TradeWindowPhaseConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    premarket_alert: bool = False
    after_cutoff_alert: bool = False
    after_eod_alert: bool = False


class MonitoringConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    health_port: int = 8090                 # HTTP-Port für Health/Metrics-Endpoint
    prometheus_enabled: bool = True         # Prometheus-Metriken aktivieren (0 = aus)
    dashboard_port: int = 8080              # Port für FastAPI-Dashboard
    watchdog_interval_s: int = 15           # Heartbeat-Takt des LiveRunners in die DB (Sekunden)
    bar_timeframe_seconds: int = 0          # 0 = aus strategy.timeframe abgeleitet; >0 = expliziter Override
    provider_poll_interval_s: int = 30      # Polling-Intervall Datenprovider (Sekunden)
    stale_tolerance_s: int = 0              # 0 = aus timeframe abgeleitet (max 60s, 25% der TF); >0 = Override
    grace_period_s: int = 90                # Anlaufpuffer nach Session-Start, bevor Stale-Alerts feuern
    reminder_interval_min: int = 30         # Erinnerungs-Intervall für aktive Health-Alerts (Minuten)
    trade_window_phases: TradeWindowPhaseConfig = Field(
        default_factory=TradeWindowPhaseConfig,
    )


class AlertsRoutingConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    channel_default: list[str] = Field(
        default_factory=lambda: ["trade_opened", "trade_closed", "daily_summary"],
    )
    channel_health: list[str] = Field(
        default_factory=lambda: ["process_dead", "data_stale", "circuit_break"],
    )
    channel_readiness: list[str] = Field(default_factory=list)


class AlertsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    trade_opened: bool = True
    trade_closed: bool = True
    stop_loss_hit: bool = True
    drawdown_warning_pct: float = -10.0
    drawdown_critical_pct: float = -15.0
    circuit_breaker: bool = True
    broker_disconnect: bool = True
    daily_summary_time_et: str = "15:55"
    daily_summary_enabled: bool = True
    large_order_confirm_above: float = 0.0
    max_per_symbol_per_minute: int = 1
    max_per_hour: int = 20
    dedup_window_s: int = 300
    routing: AlertsRoutingConfig = Field(default_factory=AlertsRoutingConfig)


class AnomalyConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    duplicate_window_minutes: int = 5           # Fensterbreite des Duplicate-Signal-Guards in Minuten
    duplicate_hard_block: bool = False          # True = identisches Signal innerhalb des Fensters wird blockiert
    max_single_order_pct: float = 0.25          # Maximalanteil einer Einzelorder am Equity (0.25 = 25 %)
    max_volume_pct: float = 0.01                # Maximalanteil der Order am Tagesvolumen des Symbols
    pnl_spike_sigma: float = 3.0                # σ-Schwelle für PnL-Spike-Erkennung
    pnl_lookback_trades: int = 50               # Rollendes Fenster der letzten N realisierten PnLs
    bar_gap_minutes: int = 10                   # Schwellwert für bar_stream_gap-Anomalie (Minuten)
    max_signals_per_hour: int = 20              # Signal-Flood-Limit pro Strategie und Stunde
    enabled_checks: dict[str, bool] = Field(
        default_factory=lambda: {
            "duplicate_trade": True,
            "oversized_order": True,
            "pnl_spike": True,
            "connectivity": True,
            "signal_flood": True,
        }
    )


class BacktestExportConfig(BaseModel):
    """Trade-Export-Optionen für den Backtest-Modus."""
    model_config = ConfigDict(extra="allow")

    export_trades: str = "none"          # "none" | "csv" | "excel" | "both"
    export_dir: Path = Path("trading_data/exports")
    show_exit_stats: bool = True         # Exit-Reason-Tabelle in Konsole


class ExecutionConfig(BaseModel):
    """Order- und Reconcile-Parameter für Live/Paper-Broker."""
    model_config = ConfigDict(extra="allow")

    order_confirm_timeout_s: float = 10.0               # Timeout für IBKR orderStatus-Bestätigung nach placeOrder (Sekunden)
    close_verification_timeout_s: float = 120.0         # Timeout bis fehlender Fill als orphan_close eskaliert (Sekunden)
    reconcile_require_healthy_session: bool = True      # Reconcile (Fantasie-Close-Block) nur bei gesunder Broker-Session
    scanner_provider: Literal["auto", "alpaca", "ibkr", "yfinance"] = "auto"  # Datenquelle für Premarket-Gap-Scan
    scanner_premarket_hours: int = 4                    # Zeitfenster (Stunden vor Open), das der Scanner als Premarket fetcht


class AppConfig(BaseModel):
    """Wurzel-Konfiguration. extra='allow' -> unbekannte Keys werden durchgereicht."""
    model_config = ConfigDict(extra="allow")

    strategy: StrategyConfig
    broker: BrokerConfig = Field(default_factory=BrokerConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)
    persistence: PersistenceConfig = Field(default_factory=PersistenceConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    anomaly: AnomalyConfig = Field(default_factory=AnomalyConfig)
    backtest_export: BacktestExportConfig = Field(
        default_factory=BacktestExportConfig,
    )
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    bot_name: str = ""              # YAML-Instanzname, z.B. "botti_nq_live"; leer → aus strategy+broker abgeleitet
    initial_capital: float = 10_000.0
    currency: str = "USD"
    benchmark: str = "SPY"
    mode: Literal["live", "paper", "backtest"] = "paper"


# ─────────────────────────── Env-Settings ───────────────────────────────────

class EnvSettings(BaseSettings):
    """Keys und Infra-Parameter aus der Umgebung/`.env`."""
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8",
                                      extra="ignore")

    APCA_API_KEY_ID: Optional[str] = None
    APCA_API_SECRET_KEY: Optional[str] = None
    APCA_PAPER: Optional[str] = None
    APCA_DATA_FEED: Optional[str] = None

    IBKR_HOST: Optional[str] = None
    IBKR_PORT: Optional[int] = None
    IBKR_CLIENT_ID: Optional[int] = None
    IBKR_PAPER: Optional[str] = None

    TELEGRAM_TOKEN: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("TELEGRAM_TOKEN", "TELEGRAM_BOT_TOKEN"),
    )
    TELEGRAM_CHAT_ID: Optional[str] = None
    TELEGRAM_HEALTH_TOKEN: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "TELEGRAM_HEALTH_TOKEN",
            "TELEGRAM_HEALTH_BOT_TOKEN",
        ),
    )
    TELEGRAM_READINESS_TOKEN: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "TELEGRAM_READINESS_TOKEN",
            "TELEGRAM_READINESS_BOT_TOKEN",
        ),
    )
    TELEGRAM_HEALTH_CHAT_ID: Optional[str] = None
    TELEGRAM_READINESS_CHAT_ID: Optional[str] = None


# ─────────────────────────── YAML-Loader ────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> dict:
    """Rekursives dict-Merge (override gewinnt)."""
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _coerce_time_fields(d: dict) -> dict:
    """Wandle bekannte Zeitfelder (HH:MM-Strings) in datetime.time um."""
    time_keys = {"market_open", "market_close", "orb_end_time",
                 "eod_close_time", "buy_cutoff_time_et", "entry_cutoff_time",
                 "premarket_time", "market_open_time", "post_market_time",
                 "obb_entry_cutoff_time", "obb_close_time",
                 "obb_exit_open_time"}
    out = dict(d)
    for k, v in list(out.items()):
        if k in time_keys and isinstance(v, str):
            hh, mm = v.split(":")
            out[k] = time(int(hh), int(mm))
        elif isinstance(v, dict):
            out[k] = _coerce_time_fields(v)
    return out


def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _apply_env_overrides(raw: dict, env: EnvSettings) -> dict:
    out = dict(raw)
    broker = dict(out.get("broker", {}))

    if env.IBKR_HOST:
        broker["ibkr_host"] = env.IBKR_HOST
    if env.IBKR_PORT is not None:
        broker["ibkr_port"] = env.IBKR_PORT

    if broker:
        out["broker"] = broker

    return out


def load_config(config_path: str | Path) -> AppConfig:
    """Lade YAML-Config.

    Wenn `base.yaml` im gleichen Ordner existiert, werden die Keys dieser
    Datei unterhalb des expliziten Configs gemerged (expliziter Config
    überschreibt).
    """
    path = Path(config_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    raw = _load_yaml(path)

    base_path = path.parent / "base.yaml"
    if base_path.exists() and base_path != path:
        base = _load_yaml(base_path)
        raw = _deep_merge(base, raw)

    raw = _apply_env_overrides(raw, load_env())
    raw = _coerce_time_fields(raw)
    return AppConfig.model_validate(raw)


def load_env() -> EnvSettings:
    return EnvSettings()

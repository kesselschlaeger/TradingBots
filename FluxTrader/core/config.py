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


class AppConfig(BaseModel):
    """Wurzel-Konfiguration. extra='allow' -> unbekannte Keys werden durchgereicht."""
    model_config = ConfigDict(extra="allow")

    strategy: StrategyConfig
    broker: BrokerConfig = Field(default_factory=BrokerConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)
    persistence: PersistenceConfig = Field(default_factory=PersistenceConfig)
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

    raw = _coerce_time_fields(raw)
    return AppConfig.model_validate(raw)


def load_env() -> EnvSettings:
    return EnvSettings()

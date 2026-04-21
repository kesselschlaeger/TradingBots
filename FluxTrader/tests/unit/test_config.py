"""Tests fuer core/config.py."""
from __future__ import annotations

from core.config import load_config, load_env


def test_load_env_supports_telegram_bot_token_alias(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TELEGRAM_BOT_TOKEN=test-bot-token\n"
        "TELEGRAM_CHAT_ID=test-chat-id\n"
        "TELEGRAM_HEALTH_BOT_TOKEN=test-health-token\n"
        "TELEGRAM_READINESS_BOT_TOKEN=test-readiness-token\n"
        "TELEGRAM_HEALTH_CHAT_ID=test-health-chat\n"
        "TELEGRAM_READINESS_CHAT_ID=test-readiness-chat\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_env()

    assert settings.TELEGRAM_TOKEN == "test-bot-token"
    assert settings.TELEGRAM_CHAT_ID == "test-chat-id"
    assert settings.TELEGRAM_HEALTH_TOKEN == "test-health-token"
    assert settings.TELEGRAM_READINESS_TOKEN == "test-readiness-token"
    assert settings.TELEGRAM_HEALTH_CHAT_ID == "test-health-chat"
    assert settings.TELEGRAM_READINESS_CHAT_ID == "test-readiness-chat"


def test_load_config_overrides_ibkr_connection_from_env(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "IBKR_HOST=10.0.0.5\n"
        "IBKR_PORT=7497\n",
        encoding="utf-8",
    )
    config_file = tmp_path / "test.yaml"
    config_file.write_text(
        "strategy:\n"
        "  name: orb\n"
        "broker:\n"
        "  type: ibkr\n"
        "  ibkr_host: 127.0.0.1\n"
        "  ibkr_port: 4002\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    config = load_config(config_file)

    assert config.broker.ibkr_host == "10.0.0.5"
    assert config.broker.ibkr_port == 7497
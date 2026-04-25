"""Tests fuer core/config.py."""
from __future__ import annotations

import pytest
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


def test_ibkr_client_id_from_env_overrides_yaml(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("IBKR_CLIENT_ID=42\n", encoding="utf-8")
    (tmp_path / "test.yaml").write_text(
        "strategy:\n  name: orb\n"
        "broker:\n  type: ibkr\n  ibkr_client_id: 8\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    cfg = load_config(tmp_path / "test.yaml")

    assert cfg.broker.ibkr_client_id == 42


def test_ibkr_paper_from_env_true(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("IBKR_PAPER=false\n", encoding="utf-8")
    (tmp_path / "test.yaml").write_text(
        "strategy:\n  name: orb\n"
        "broker:\n  type: ibkr\n  paper: true\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    cfg = load_config(tmp_path / "test.yaml")

    assert cfg.broker.paper is False


def test_ibkr_paper_env_various_values(tmp_path, monkeypatch):
    for value, expected in [("1", True), ("yes", True), ("0", False), ("no", False)]:
        (tmp_path / ".env").write_text(f"IBKR_PAPER={value}\n", encoding="utf-8")
        (tmp_path / "test.yaml").write_text(
            "strategy:\n  name: orb\nbroker:\n  type: ibkr\n", encoding="utf-8"
        )
        monkeypatch.chdir(tmp_path)
        cfg = load_config(tmp_path / "test.yaml")
        assert cfg.broker.paper is expected, f"IBKR_PAPER={value!r} → expected {expected}"


def test_empty_ibkr_host_yaml_falls_back_to_env(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("IBKR_HOST=10.1.2.3\n", encoding="utf-8")
    (tmp_path / "test.yaml").write_text(
        "strategy:\n  name: orb\n"
        "broker:\n  type: ibkr\n  ibkr_host: \"\"\n",  # leerer String
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    cfg = load_config(tmp_path / "test.yaml")

    assert cfg.broker.ibkr_host == "10.1.2.3"


def test_ibkr_data_client_id_from_env(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("IBKR_DATA_CLIENT_ID=201\n", encoding="utf-8")
    (tmp_path / "test.yaml").write_text(
        "strategy:\n  name: orb\nbroker:\n  type: ibkr\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)

    cfg = load_config(tmp_path / "test.yaml")

    assert cfg.data.ibkr_data_client_id == 201


def test_duplicate_hard_block_default_true():
    from core.config import AnomalyConfig
    cfg = AnomalyConfig()
    assert cfg.duplicate_hard_block is True


def test_execution_allow_scale_in_default_false():
    from core.config import ExecutionConfig
    cfg = ExecutionConfig()
    assert cfg.allow_scale_in is False
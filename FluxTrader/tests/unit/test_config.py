"""Tests fuer core/config.py."""
from __future__ import annotations

from core.config import load_env


def test_load_env_supports_telegram_bot_token_alias(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TELEGRAM_BOT_TOKEN=test-bot-token\n"
        "TELEGRAM_CHAT_ID=test-chat-id\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_env()

    assert settings.TELEGRAM_TOKEN == "test-bot-token"
    assert settings.TELEGRAM_CHAT_ID == "test-chat-id"
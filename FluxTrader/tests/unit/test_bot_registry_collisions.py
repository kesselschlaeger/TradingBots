"""Bot-Registry-Kollisions-Test.

Liest alle YAMLs in configs/ und stellt sicher, dass keine zwei Live-Configs
die gleiche (ibkr_client_id, mode) Kombination haben.
Kein Netzwerk, kein Broker.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml


_CONFIGS_DIR = Path(__file__).parent.parent.parent / "FluxTrader" / "configs"
if not _CONFIGS_DIR.exists():
    _CONFIGS_DIR = Path(__file__).parent.parent.parent / "configs"


def _load_live_configs():
    """Lädt alle YAML-Configs die broker.type=ibkr und mode != backtest haben."""
    configs = []
    for p in sorted(_CONFIGS_DIR.glob("*.yaml")):
        if "backtest" in p.name or p.name == "base.yaml":
            continue
        try:
            raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        broker = raw.get("broker", {})
        mode = raw.get("mode", "paper")
        if broker.get("type") != "ibkr":
            continue
        client_id = broker.get("ibkr_client_id")
        bot_id = broker.get("ibkr_bot_id", "FLUX")
        if client_id is not None:
            configs.append({
                "file": p.name,
                "mode": mode,
                "ibkr_client_id": client_id,
                "ibkr_bot_id": bot_id,
            })
    return configs


class TestBotRegistryCollisions:
    def test_no_duplicate_ibkr_client_ids(self):
        """Gleiche ibkr_client_id ist erlaubt, wenn ibkr_bot_id identisch ist
        (= zwei Configs für denselben logischen Bot, z.B. orb_live + orb_live_ibkr)."""
        configs = _load_live_configs()
        seen: dict[tuple, str] = {}
        for cfg in configs:
            # Schlüssel: (client_id, mode, bot_id) – gleicher Bot darf gleiche ID haben
            key = (cfg["ibkr_client_id"], cfg["mode"])
            bot_key = (cfg["ibkr_client_id"], cfg["mode"], cfg["ibkr_bot_id"].upper())
            if key in seen:
                existing_bot_key = seen[key]
                if existing_bot_key != bot_key:
                    pytest.fail(
                        f"ibkr_client_id {cfg['ibkr_client_id']} (mode={cfg['mode']}) "
                        f"doppelt vergeben von VERSCHIEDENEN Bots: {existing_bot_key[2]!r} und "
                        f"{cfg['ibkr_bot_id']!r} – Configs: {seen.get(key + ('file',))} und {cfg['file']}"
                    )
            seen[key] = bot_key

    def test_no_duplicate_ibkr_bot_ids(self):
        """Gleiche ibkr_bot_id ist erlaubt, wenn sie denselben logischen Bot repräsentiert
        (mehrere Config-Varianten desselben Bots, z.B. orb_live + orb_live_ibkr)."""
        configs = _load_live_configs()
        # Gruppiere nach bot_id → alle client_ids, die diese bot_id nutzen
        bot_id_to_client_ids: dict[str, set[int]] = {}
        for cfg in configs:
            bot_id = cfg["ibkr_bot_id"].upper()
            if bot_id == "FLUX":
                continue
            bot_id_to_client_ids.setdefault(bot_id, set()).add(cfg["ibkr_client_id"])

        for bot_id, client_ids in bot_id_to_client_ids.items():
            if len(client_ids) > 1:
                pytest.fail(
                    f"ibkr_bot_id {bot_id!r} wird mit VERSCHIEDENEN client_ids "
                    f"verwendet: {sorted(client_ids)} – das ist ein Fill-Routing-Problem"
                )

    def test_all_ibkr_configs_have_explicit_bot_id(self):
        configs = _load_live_configs()
        for cfg in configs:
            if cfg["ibkr_bot_id"] == "FLUX":
                pytest.fail(
                    f"{cfg['file']}: ibkr_bot_id ist noch Default 'FLUX' – "
                    f"bitte expliziten Wert in _bot_registry.md eintragen"
                )

    def test_all_ibkr_configs_have_explicit_client_id_not_one(self):
        configs = _load_live_configs()
        for cfg in configs:
            if cfg["ibkr_client_id"] == 1:
                pytest.fail(
                    f"{cfg['file']}: ibkr_client_id ist noch Default 1 – "
                    f"bitte eindeutige ID aus docs/bot_registry.md vergeben"
                )

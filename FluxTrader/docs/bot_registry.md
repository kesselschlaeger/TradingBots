# FluxTrader – Bot-Registry

Verbindliche Zuweisung von `ibkr_client_id` und `ibkr_bot_id` für alle Live-Bots.
Mehrere Bots auf demselben IBKR-Paper-Account → Client-ID-Kollision wirft **Error 326**.
`ibkr_bot_id` trennt Fills via `orderRef`-Präfix (mehrere Bots auf einem Account).

## Aktive Bots (Stand 2026-04-25)

| Bot              | Strategy      | ibkr_client_id | Data-Client-ID | ibkr_bot_id       | Config-Datei                    |
|------------------|---------------|----------------|----------------|-------------------|---------------------------------|
| Botti            | botti         | 8              | 108            | Flux_BOTTI        | configs/botti.yaml              |
| ORB              | orb           | 11             | 111            | Flux_ORB          | configs/orb_live.yaml           |
| OBB              | obb           | 12             | 112            | Flux_OBB          | configs/obb_live.yaml           |
| ICT-Equity       | ict_ob_mtf    | 18             | 118            | Flux_ICT_EQ       | configs/ict_ob_live.yaml        |
| ICT-Futures      | ict_ob_mtf    | 19             | 119            | Flux_ICT_FUT      | configs/ict_ob_futures_live.yaml|
| ICT-Crypto       | ict_ob_mtf    | 20             | 120            | Flux_ICT_CRY      | configs/ict_ob_crypto_live.yaml |
| Quick Flip       | quick_flip    | 22             | 122            | Flux_QUICKFLIP    | configs/quick_flip.yaml         |
| Botti-Pair       | botti_pair    | 23             | 123            | Flux_BOTTI_PAIR   | configs/botti_pair.yaml         |

**Data-Client-ID** = `ibkr_client_id + 100` (berechnet in `main.py._build_data_provider`).
Kann via `data.ibkr_data_client_id` in YAML oder `IBKR_DATA_CLIENT_ID` in `.env` überschrieben werden.

## Freie IDs

Client-IDs **13–17**, **21**, **24+** sind noch nicht vergeben.

## Regeln für neue Bots

1. Freie `ibkr_client_id` aus der Tabelle oben wählen.
2. `ibkr_bot_id` nach Schema `Flux_<NAME>` vergeben (max. 8 Zeichen nach Strip).
3. Tabelle hier aktualisieren, Config-YAML anlegen, `memory/project_bot_isolation.md` aktualisieren.
4. Runtime-Validator in `main.py` prüft beim Start auf Kollisionen und wirft `RuntimeError`.

## Historische Korrekturen (2026-04-25)

| Config                        | Vorher               | Nachher              | Grund                                       |
|-------------------------------|----------------------|----------------------|---------------------------------------------|
| `ict_ob_live.yaml`            | `Flux_BOTTI`         | `Flux_ICT_EQ`        | Copy-Paste-Bug, kollidierte mit Botti-Bot   |
| `obb_live.yaml`               | `ibkr_client_id: 19` | `ibkr_client_id: 12` | Kollision mit ICT-Futures (beide hatten 19) |
| `orb_live_ibkr.yaml`          | `ibkr_client_id: 8`  | `ibkr_client_id: 11` | Kollision mit Botti (beide hatten 8)        |
| `botti_pair.yaml`             | (kein Eintrag, FLUX) | `23 / Flux_BOTTI_PAIR` | Neu vergeben                              |

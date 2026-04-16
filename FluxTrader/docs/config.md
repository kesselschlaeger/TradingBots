# Konfiguration

FluxTrader nutzt YAML-Konfiguration mit automatischem Merge aus `base.yaml`.
Pydantic v2 validiert alle Werte typsicher.

## Lade-Reihenfolge

```
1. configs/base.yaml          ← Shared Defaults
2. configs/<deine>.yaml       ← Überschreibt base.yaml (deep merge)
3. .env / Umgebungsvariablen  ← API-Keys, Infra-Parameter
```

```bash
python main.py paper --config configs/orb_paper.yaml
```

---

## Vollständige Config-Referenz

### Wurzel-Keys

| Key | Typ | Default | Beschreibung |
|---|---|---|---|
| `mode` | `live` \| `paper` \| `backtest` | `paper` | Betriebsmodus |
| `initial_capital` | float | `10000.0` | Startkapital in USD |
| `currency` | str | `USD` | Handelswährung |
| `benchmark` | str | `SPY` | Benchmark-Symbol für Trendfilter |

---

### `broker`

| Key | Typ | Default | Beschreibung |
|---|---|---|---|
| `type` | `paper` \| `alpaca` \| `ibkr` | `paper` | Broker-Adapter |
| `paper` | bool | `true` | Paper-Endpunkt nutzen |
| `alpaca_data_feed` | `iex` \| `sip` | `iex` | IEX = kostenfrei, SIP = kostenpflichtig |
| `ibkr_host` | str | `127.0.0.1` | TWS/Gateway Host |
| `ibkr_port` | int | `4002` | 4001 = Live, 4002 = Paper |
| `ibkr_client_id` | int | `1` | Muss pro Bot-Instanz eindeutig sein |
| `ibkr_bot_id` | str | `FLUX` | Prefix in `orderRef` (max 8 Zeichen) |

---

### `data`

| Key | Typ | Default | Beschreibung |
|---|---|---|---|
| `provider` | `alpaca` \| `yfinance` | `alpaca` | Datenquelle |
| `timeframe` | str | `5Min` | Bar-Größe (siehe unten) |
| `lookback_days` | int | `5` | Historischer Puffer für Indicators |

**Gültige Timeframe-Strings:**

| Eingabe | Bedeutung |
|---|---|
| `1Min`, `1m` | 1 Minute |
| `5Min`, `5m` | 5 Minuten (ORB-Default) |
| `15Min`, `15m` | 15 Minuten |
| `1Hour`, `1h` | 1 Stunde |
| `1Day`, `1d` | 1 Tag (OBB-Default) |

---

### `strategy`

| Key | Typ | Beschreibung |
|---|---|---|
| `name` | `orb` \| `obb` | Strategie-Name (muss registriert sein) |
| `symbols` | `list[str]` | Handelssymbole |
| `risk_pct` | float | Risk pro Trade als Anteil des Eigenkapitals (z.B. `0.01` = 1%) |
| `params` | dict | Strategie-spezifische Parameter (siehe Strategie-Docs) |

---

### `notifications`

| Key | Typ | Default | Beschreibung |
|---|---|---|---|
| `enabled` | bool | `false` | Telegram-Alerts aktivieren |
| `bot_name` | str | `""` | Anzeigename des Bots im Telegram-Chat |
| `telegram_token` | str | `""` | Bot-Token (besser via `.env`) |
| `telegram_chat_id` | str | `""` | Chat-ID (besser via `.env`) |

---

### `persistence`

| Key | Typ | Default | Beschreibung |
|---|---|---|---|
| `data_dir` | str | `fluxtrader_data` | Verzeichnis für SQLite-DB |
| `state_db` | str | `state.db` | Dateiname der State-Datenbank |

---

## Zeitfelder

Zeitangaben in `params` können als `HH:MM`-String angegeben werden:

```yaml
strategy:
  params:
    market_open: "09:30"
    eod_close_time: "15:27"
    orb_end_time: "10:00"
```

Der Config-Loader konvertiert diese automatisch in `datetime.time`-Objekte.

---

## Umgebungsvariablen

Alle Credentials kommen idealerweise aus `.env` (nie in YAML committen):

| Variable | Beschreibung |
|---|---|
| `APCA_API_KEY_ID` | Alpaca API Key |
| `APCA_API_SECRET_KEY` | Alpaca Secret |
| `IBKR_HOST` | TWS/Gateway Host (überschreibt YAML) |
| `IBKR_PORT` | TWS/Gateway Port |
| `IBKR_CLIENT_ID` | Client-ID |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot-Token |
| `TELEGRAM_CHAT_ID` | Telegram Chat-ID |

---

## Beispiel: Minimale Live-Config

```yaml
# configs/mein_live.yaml
mode: live

strategy:
  name: orb
  symbols: [NVDA, AMD, TSLA]
  risk_pct: 0.01
  params:
    opening_range_minutes: 15
    allow_shorts: false
    use_mit_probabilistic_overlay: true
    eod_close_time: "15:27"

broker:
  type: alpaca
  paper: false          # ACHTUNG: Echtgeld!

notifications:
  enabled: true
```

!!! warning "Echtgeld"
    `paper: false` schaltet auf echte Order-Ausführung um. Sicherstellen dass
    der API-Key ein Live-Key ist (nicht Paper).

---

## Config programmatisch laden

```python
from core.config import load_config, load_env

cfg = load_config("configs/orb_paper.yaml")
env = load_env()

print(cfg.strategy.name)       # "orb"
print(cfg.broker.type)         # "alpaca"
print(env.APCA_API_KEY_ID)     # aus .env
```

# Monitoring & Dashboard

Das Monitoring-Subsystem besteht aus fünf Komponenten mit klarer Trennung:

| Komponente | Datei | Zweck |
|---|---|---|
| PersistentState | `live/state.py` | Zentrale SQLite DB (eine Datei für alle Bots) mit Trades, Equity, Positionen, Signals, Anomalies |
| Health-Server | `live/health.py` | `/health`, `/ready`, `/status`, `/metrics/text` (aiohttp) |
| Metrics | `live/metrics.py` | Prometheus-kompatible Counter/Gauge/Histogramme (Null-Object-Fallback) |
| Dashboard | `dashboard/app.py` | FastAPI + Vanilla-JS SPA, liest nur aus `PersistentState` + `HealthState` |
| Alerts | `live/notifier.py` | Templates + Rate-Limiter (Token-Bucket pro Symbol + global/h) |
| Anomaly-Detector | `live/anomaly.py` | Duplicate / Oversized / PnL-Spike / Connectivity / Signal-Flood (persistiert zu DB) |

---

## Zentrale SQLite-Datenbank (Single-Source-of-Truth)

Eine `state.db` für **alle gleichzeitig laufenden Strategien** (ORB, OBB, Botti, BottiPair, ...).

### Schema

Jede Tabelle trägt `strategy TEXT NOT NULL` zur Bot-Trennung:

| Tabelle | Zweck | Key Columns |
|---|---|---|
| **trades** | Komplette Trade-Historie | id, strategy, symbol, entry_ts, exit_ts |
| **equity_snapshots** | Hochfrequente Equity-Kurve | (ts, strategy) |
| **positions** | Offene Positionen (Live-Spiegel) | (strategy, symbol) |
| **signals** | Eingegangene Signale (optional) | id, strategy, ts |
| **anomaly_events** | Erkannte Anomalien (Alerts, Errors) | id, strategy, ts |
| **daily** | Tägliche PnL + Trade-Count | (day, strategy) |
| **account** | Peak-Equity, sonstige Meter | key |
| **cooldowns** | Symbol-Cooldowns | symbol |
| **reserved_groups** | MIT-Independence pro Strategie | (group, day, strategy) |

### trades Tabelle (Beispiel)

```sql
SELECT * FROM trades WHERE strategy='orb' AND symbol='AAPL' LIMIT 1;
```

Liefert:

```
id | strategy | symbol | side  | entry_ts           | exit_ts            | entry_price | exit_price | qty  | pnl   | pnl_pct | reason | stop_price | signal_strength | mit_qty_factor | ev_estimate | group_name | features_json
---|----------|--------|-------|--------------------|--------------------|-------------|------------|------|-------|---------|--------|------------|-----------------|----------------|-------------|------------|----------------------
1  | orb      | AAPL   | long  | 2026-04-18T14:31.. | 2026-04-18T14:45.. | 150.25      | 152.10     | 10.0 | 18.50 | 1.23    | target | 149.00     | 0.85            | 0.75           | 0.42        | TECH_1     | {"rsi":65,"sma_diff":0.02}
```

**Wichtige Felder für Probabilistische Auswertungen:**
- `mit_qty_factor` (0.25–1.0) – aus MIT-Overlay (Quantity-Adjustment für Independence)
- `ev_estimate` – Expected-Value-Schätzung aus Signal
- `signal_strength` – 0.0–1.0 Konfidenz
- `features_json` – kompletter FeatureVector als JSON (für ML-Retraining)

---

## Start

### 1. Abhängigkeiten installieren

```bash
pip install fastapi uvicorn aiosqlite
```

### 2. Live-Bot starten

```bash
cd FluxTrader
python main.py live --config configs/orb_paper.yaml
```

Der Bot **erstellt automatisch** alle Tabellen beim Start (idempotent via `ensure_schema()`). Jeden Bar-Tick schreibt er:
- `equity_snapshots` (aktueller Stand)
- `positions` (Live-Update + Unrealized-PnL)
- Bei Order: `trades` (open) + `signals`
- Bei Close: `trades` (set exit_ts/exit_price/pnl) + `daily` (inkrementiert)

### 3. Dashboard als separater Prozess

```bash
python main.py dashboard --config configs/orb_paper.yaml --port 8080
```

Öffne [http://localhost:8080](http://localhost:8080) im Browser.

### 4. Mehrere Bots gleichzeitig

Alle schreiben in **eine** `fluxtrader_data/state.db`, getrennt über `strategy`-Feld:

```bash
# Terminal 1
python main.py paper --config configs/orb_paper.yaml

# Terminal 2
python main.py paper --config configs/obb_live.yaml

# Terminal 3
python main.py paper --config configs/botti.yaml

# Terminal 4 (Dashboard für alle)
python main.py dashboard --config configs/orb_paper.yaml --port 8080
```

Das Dashboard zeigt alle Bots aggregiert oder einzeln.

---

## Dashboard-API

Alle Endpunkte lesen **read-only** aus der zentralen DB (keine Broker-Calls).

### Trade-Historie

```bash
# Alle Trades (mit MIT-Qty + EV)
curl "http://localhost:8080/api/trades?strategy=orb&limit=100"

# Nur geschlossene Trades
curl "http://localhost:8080/api/trades?strategy=orb&only_closed=true"

# Nach Datum filtern
curl "http://localhost:8080/api/trades?strategy=orb&since=2026-04-15&until=2026-04-18"

# Tägliche Statistik
curl "http://localhost:8080/api/trades/summary?strategy=orb&days=30"
```

**Response** (`/api/trades`):

```json
[
  {
    "id": 1,
    "strategy": "orb",
    "symbol": "AAPL",
    "side": "long",
    "entry_ts": "2026-04-18T14:31:00+00:00",
    "exit_ts": "2026-04-18T14:45:30+00:00",
    "entry_price": 150.25,
    "exit_price": 152.10,
    "qty": 10.0,
    "pnl": 18.50,
    "pnl_pct": 1.23,
    "reason": "target",
    "stop_price": 149.00,
    "signal_strength": 0.85,
    "mit_qty_factor": 0.75,
    "ev_estimate": 0.42,
    "group_name": "TECH_1",
    "features_json": "{\"rsi\":65,\"sma_diff\":0.02}"
  }
]
```

### Positionen & Equity

```bash
# Offene Positionen
curl "http://localhost:8080/api/positions?strategy=orb"

# Equity-Kurve (letzten 500 Snapshots)
curl "http://localhost:8080/api/equity?strategy=orb&limit=500"

# Portfolio-Gesamt
curl "http://localhost:8080/api/portfolio"

# Alle aktiven Strategien + Status
curl "http://localhost:8080/api/strategies/list"
```

**Response** (`/api/positions`):

```json
[
  {
    "strategy": "orb",
    "symbol": "AAPL",
    "side": "long",
    "entry_price": 150.25,
    "qty": 10.0,
    "stop_price": 149.00,
    "current_price": 151.50,
    "unrealized_pnl": 12.50,
    "unrealized_pnl_pct": 0.83,
    "held_minutes": 14,
    "last_update_ts": "2026-04-18T14:45:00+00:00"
  }
]
```

**Response** (`/api/equity`):

```json
[
  {
    "ts": "2026-04-18T14:30:00+00:00",
    "equity": 101250.0,
    "cash": 95000.0,
    "drawdown_pct": -1.23,
    "peak_equity": 102500.0,
    "unrealized_pnl_total": 1250.0
  }
]
```

**Response** (`/api/strategies/list`):

```json
{
  "total_strategies": 3,
  "strategies": [
    {
      "strategy": "orb",
      "equity": 101250.0,
      "drawdown_pct": -1.23,
      "peak_equity": 102500.0,
      "open_positions": 2,
      "trades_today": 5,
      "pnl_today": 1250.0,
      "last_equity_ts": "2026-04-18T14:45:00+00:00"
    }
  ]
}
```

### Health & Status (nur Live-Mode)

```bash
# Broker + Circuit-Breaker Status
curl "http://localhost:8090/status"

# Prometheus-Metriken
curl "http://localhost:8090/metrics/text"
```

---

## Config-Snippets

```yaml
monitoring:
  health_port: 8090
  prometheus_enabled: true
  dashboard_port: 8080

alerts:
  trade_opened: true
  trade_closed: true
  stop_loss_hit: true
  drawdown_warning_pct: -10.0
  drawdown_critical_pct: -15.0
  circuit_breaker: true
  broker_disconnect: true
  daily_summary_time_et: "15:55"
  max_per_symbol_per_minute: 1
  max_per_hour: 20

anomaly:
  duplicate_window_minutes: 5
  duplicate_hard_block: false           # true → blockiert Duplicates
  max_single_order_pct: 0.25            # max Ordervolumen / Equity
  max_volume_pct: 0.05                  # max Order / Tagesvolumen
  pnl_spike_sigma: 3.0                  # σ-Schwelle für PnL-Ausreißer
  pnl_lookback_trades: 50               # Fenster für Spike-Detection
  bar_gap_minutes: 10                   # Connectivity-Schwelle
  max_signals_per_hour: 20              # Signal-Flood-Limit
  enabled_checks:
    duplicate_trade: true
    oversized_order: true
    pnl_spike: true
    connectivity: true
    signal_flood: true
```

---

## Datenfluss

```
LiveRunner._process_bar(bar)
├─ strategy.on_bar(bar) → Signal
├─ broker.execute_signal(sig) → ExecutionResult
├─ TradeManager.register_and_persist(trade, sig)
│  └─ state.save_trade(...)  ← DB: trades (open)
│  └─ state.update_or_create_position(...)  ← DB: positions
├─ state.save_signal(sig)  ← DB: signals (optional, für ML-Retraining)
└─ state.save_equity_snapshot(...)  ← DB: equity_snapshots

LiveRunner._on_fill() / close_trade()
├─ AnomalyDetector._emit(event)
│  └─ state.log_anomaly(event)  ← DB: anomaly_events
├─ state.close_trade(...)  ← DB: trades (set exit_ts, exit_price, pnl)
├─ state.remove_position(...)  ← DB: positions
└─ state.update_daily_record(...)  ← DB: daily (inkrementiert pnl, trades_count)

Dashboard (Standalone oder mit Live-Runner)
├─ state.get_trades(...)  ← read: trades
├─ state.get_open_positions(...)  ← read: positions
├─ state.get_latest_equity_curve(...)  ← read: equity_snapshots
├─ state.get_strategy_status(...)  ← read: aggregiert (equity, positions, daily)
└─ health.snapshot()  ← read: HealthState (nur Live-Mode)
```

---

## Konkrete Auswertungen (für Probabilistische Modelle)

### Expected-Value-Tracking

```sql
-- Durchschnittliches EV-Estimate vs. realisiertes PnL pro Strategie
SELECT
  strategy,
  AVG(ev_estimate) AS avg_ev_forecast,
  AVG(pnl_pct) AS realized_pnl_pct,
  COUNT(*) AS num_trades
FROM trades
WHERE exit_ts IS NOT NULL
  AND ev_estimate IS NOT NULL
GROUP BY strategy;
```

### MIT-Qty-Factor-Analyse

```sql
-- Trade-Größe vs. Erfolgsrate (nach MIT-Qty-Factor)
SELECT
  strategy,
  ROUND(mit_qty_factor, 2) AS qty_factor_bucket,
  COUNT(*) AS num_trades,
  ROUND(AVG(pnl_pct), 2) AS avg_pnl_pct,
  ROUND(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate
FROM trades
WHERE exit_ts IS NOT NULL
GROUP BY strategy, ROUND(mit_qty_factor, 2)
ORDER BY strategy, qty_factor_bucket;
```

### Drawdown-Rekonvaleszenz

```sql
-- Wie lange hat ein Bot nach DD-Peak gebraucht, wieder auf High zu kommen?
SELECT
  strategy,
  MIN(ts) AS min_equity_ts,
  equity AS min_equity,
  MAX(peak_equity) AS peak_at_min,
  (MAX(peak_equity) - equity) AS drawdown_amount
FROM equity_snapshots
WHERE strategy = 'orb'
  AND equity < peak_equity
ORDER BY ts DESC
LIMIT 1;
```

---

## Prometheus-Scrape (optional)

```yaml
# prometheus.yml
scrape_configs:
  - job_name: fluxtrader
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8090']
    metrics_path: /metrics/text
```

**Verfügbare Metriken:**
- `fluxtrader_trades_total` – Akkumulierte Trade-Count
- `fluxtrader_equity_current` – Aktuelle Equity
- `fluxtrader_drawdown_pct` – Aktueller Drawdown
- `fluxtrader_positions_open` – Anzahl offener Positionen
- `fluxtrader_order_latency_ms` – Broker-Latenz (Percentile)
- `fluxtrader_bar_processing_lag_ms` – Data-Lag
- `fluxtrader_signals_total` – Eingegangene Signale
- `fluxtrader_circuit_breaker_active` – Circuit-Breaker Status
- `fluxtrader_wfo_oos_sharpe` – WFO Out-of-Sample Sharpe (wenn aktiv)

---

## Troubleshooting

### Dashboard zeigt keine Daten

1. Schema wurde nicht initialisiert:
   ```bash
   python -c "
   import asyncio
   from pathlib import Path
   from live.state import PersistentState
   asyncio.run(PersistentState(Path('fluxtrader_data/state.db')).ensure_schema())
   "
   ```

2. Kein Bot schreibt aktuell – starten Sie den Runner:
   ```bash
   python main.py paper --config configs/orb_paper.yaml
   ```

3. Port 8080 belegt – `--port 8081` verwenden oder `monitoring.dashboard_port` in YAML ändern.

### DB ist gesperrt (SQLITE_BUSY)

WAL-Modus ist aktiv (optimal für Concurrency). Falls trotzdem Timeout:
- Zu viele parallele Schreiber? Max. 3–5 Bots gleichzeitig empfohlen.
- Langsame Disk? NVMe > HDD.

### Tests schlagen fehl

`state.ensure_schema()` wird automatisch beim `init()` (oder explizit `ensure_schema()`) aufgerufen. Falls Tests `state.db` mit altem Schema haben:
```bash
rm fluxtrader_data/state.db
python -m pytest tests/unit/
```

---

## Nächste Schritte

- **HTML-Dashboard erweitern:** `dashboard/static/` mit echten Charts (Chart.js, Plotly)
- **Grafana-Integration:** Prometheus-Metriken in Grafana visualisieren
- **Trade-Replay:** Export → Jupyter-Notebooks für Post-Trade-Analyse
- **Backtester-Integration:** WFO-Ergebnisse in die DB speichern

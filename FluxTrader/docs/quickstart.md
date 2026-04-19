# Quickstart

## Voraussetzungen

- Python 3.11+
- Alpaca-Konto (Paper reicht für den Einstieg)
- Optional: TWS/IB Gateway für IBKR-Betrieb

---

## Installation

```bash
# Repository klonen / Ordner öffnen
cd FluxTrader

# Abhängigkeiten installieren (Extras je nach Bedarf)
pip install -e ".[alpaca,live,backtest]"

# Nur Paper/Backtest ohne Live-Infrastruktur:
pip install -e ".[backtest]"
```

### PowerShell (empfohlen unter Windows)

!!! warning "Execution Policy – einmalig pro Session"
    Windows blockiert PS-Skripte standardmaessig. Vor dem ersten Skript-Aufruf:
    ```powershell
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
    ```
    `-Scope Process` gilt **nur fuer die aktuelle Shell**, kein Admin-Recht noetig,
    nichts systemweit veraendert.

```powershell
# 0. Execution Policy entsperren (jede neue PS-Session einmalig)
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned

Set-Location FluxTrader

# 1. Einmalig: dediziertes FluxTrader-venv erstellen + Basis-Extras installieren
.\tools\Setup-FluxEnv.ps1 -PythonVersion 3.11 -Recreate

# Optional: ML-Test-Abhaengigkeiten fuer test_ml_filter.py installieren
.\tools\Setup-FluxEnv.ps1 -PythonVersion 3.11 -InstallMlTestDeps

# 2. In jeder neuen Shell aktivieren
.\tools\Enter-FluxEnv.ps1
```

Damit verwendest du in VS Code Terminal und PowerShell immer dasselbe `.venv`
im FluxTrader-Ordner statt zufaellig wechselnder globaler Interpreter.

### Extras im Überblick

| Extra | Pakete | Wann nötig |
|---|---|---|
| `alpaca` | alpaca-py | Alpaca Live/Paper-Trading + Daten |
| `ibkr` | ib_insync | Interactive Brokers |
| `live` | aiosqlite, httpx, apscheduler | Live-Runner, State, Telegram |
| `backtest` | yfinance, exchange-calendars | Historische Daten |
| `dev` | pytest, ruff | Entwicklung & Tests |
| `all` | Alles außer dev | Vollinstallation |

---

## Umgebungsvariablen

```bash
cp .env.example .env
```

`.env` bearbeiten:

```dotenv
# Alpaca (Paper oder Live)
APCA_API_KEY_ID=PKxxxxxxxxxxxxxxx
APCA_API_SECRET_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# Telegram (optional)
TELEGRAM_BOT_TOKEN=123456789:ABC-xxx
TELEGRAM_CHAT_ID=-100123456789
```

!!! tip "Paper vs. Live"
    Alpaca unterscheidet Paper- und Live-Keys. Der `paper: true`-Schalter in der
    YAML-Config steuert welcher Endpunkt angesprochen wird – aber nur der passende
    API-Key funktioniert.

---

## Erster Lauf: Paper-Trading

```bash
python main.py paper --config configs/orb_paper.yaml
```

Die Ausgabe sieht etwa so aus:

```
2025-03-12 09:28:00 [info] runner.started strategy=orb broker=AlpacaAdapter symbols=['SPY','QQQ','AAPL','NVDA','TSLA']
2025-03-12 09:30:00 [info] scanner.results count=3 symbols=['NVDA','TSLA','META']
2025-03-12 10:05:32 [info] runner.signal_executed symbol=NVDA side=long order_id=abc123
```

---

## Erster Lauf: Backtest

```bash
python main.py backtest --config configs/orb_backtest.yaml
```

Beispiel-Output:

```
==================================================
BACKTEST RESULT
==================================================
Initial:        $100,000.00
Final:          $112,340.50
Total Return:   +12.34%
CAGR:           +18.21%
Max Drawdown:   6.82%
Sharpe:         1.43
Sortino:        2.11
Trades:         47
Win Rate:       57.4%
Avg Win:        $312.40
Avg Loss:       $-198.60
Profit Factor:  1.91
Expectancy:     $66.80/Trade
```

---

## Tests ausführen

```bash
# Alle Unit- und Integrationstests (kein Netzwerk nötig)
pytest

# Nur Unit-Tests
pytest tests/unit/

# Mit Verbose-Output
pytest -v
```

!!! tip "Warum manchmal Tests geskippt werden"
    Einige Tests sind absichtlich optional aufgebaut (z.B. ML/Prometheus).
    Fehlen die Pakete, werden einzelne Tests mit `importorskip` uebersprungen
    statt als Fehler zu brechen. Fuer vollstaendige lokale Testlaeufe installiere
    diese Pakete zusaetzlich, z.B. via `tools/Setup-FluxEnv.ps1 -InstallMlTestDeps`.

!!! info "Netzwerkfreie Tests"
    Alle Tests in `tests/unit/` und `tests/integration/` laufen vollständig
    ohne Netzwerkzugriff – Broker und Daten werden durch `PaperAdapter` und
    deterministische OHLCV-Fixtures ersetzt.

---

## Logs & Diagnose

```bash
# JSON-Logs (für Log-Aggregation)
python main.py paper --config configs/orb_paper.yaml --log-json

# Debug-Level
python main.py paper --config configs/orb_paper.yaml --log-level DEBUG
```

State-Datenbank (SQLite) liegt in `fluxtrader_data/state.db` – einsehbar mit
jedem SQLite-Viewer (z.B. DB Browser for SQLite).

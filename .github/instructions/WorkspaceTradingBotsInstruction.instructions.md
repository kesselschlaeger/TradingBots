Du bist ein Senior Quant Trading Engineer und arbeitest an einem Projekt, das sich mit der Entwicklung von Trading-Bots beschäftigt. Deine Aufgabe ist es, Anweisungen für die Implementierung und Wartung dieser Bots zu erstellen. Diese Anweisungen sollen sicherstellen, dass alle Teammitglieder die gleichen Standards und Best Practices befolgen, um die Qualität und Effizienz der Bots zu gewährleisten.

# FluxTrader – Claude Session Guide

Modulares Trading-Bot-Framework. Python 3.11+, asyncio, Pydantic v2, structlog.

## Kritische Invarianten

Diese Regeln dürfen NIEMALS verletzt werden:

1. **Keine Broker-Imports in Strategie-Dateien** (`strategy/`)
   - Strategien sind reine Python-Funktionen: kein I/O, kein HTTP, kein SDK
   - Cross-Symbol-Kontext kommt ausschließlich über `MarketContextService`

2. **Keine Duplikate in Core**
   - ATR, EMA, VWAP → `core/indicators.py`
   - Position Sizing, Kelly, Stops → `core/risk.py`
   - Gap-Filter, Trend-Filter, Zeitfunktionen → `core/filters.py`
   - Nur dort. Nie copy-pasten.

3. **Gleiche Strategie-Klasse für Backtest und Live**
   - `ORBStrategy` wird in `backtest/engine.py` UND `live/runner.py` identisch verwendet
   - Kein separater Backtest-Code in Strategie-Dateien

4. **Writer/Reader-Trennung im MarketContextService**
   - Writer-Methoden (`set_now`, `update_account`, `set_spy_df`, ...) → nur Runner/Engine
   - Strategien dürfen nur über `snapshot()`, `.account`, `.spy_df`, `.vix`, `.open_symbols` lesen
   - `push_bar()` wird intern von `BaseStrategy.on_bar()` aufgerufen

5. **Async/Sync-Grenze**
   - Alle Broker-Calls sind `async`
   - Sync-SDKs (alpaca-py, ib_insync) werden via `loop.run_in_executor(None, ...)` gewrapped
   - Niemals `asyncio.run()` innerhalb eines laufenden Event-Loops aufrufen

## Verzeichnis-Verantwortlichkeiten

```
core/models.py          → Datenklassen (Bar, Signal, OrderRequest, Position, Trade)
core/indicators.py      → Reine Indikatoren-Funktionen (kein State)
core/risk.py            → Position Sizing, Kelly, Stops, EV-Berechnung
core/filters.py         → Marktzeiten, Gap, Trend, MIT-Independence, VIX-Regime
core/context.py         → MarketContextService (DI-Container, Singleton)
core/trade_manager.py   → ManagedTrade, TradeManager (Exits/Trailing/EOD)
core/config.py          → AppConfig (Pydantic v2), load_config(), load_env()
core/logging.py         → setup_logging(), get_logger() via structlog

strategy/base.py        → BaseStrategy ABC mit on_bar() + DI-Property
strategy/registry.py    → @register-Decorator, StrategyRegistry
strategy/orb.py         → ORBStrategy (@register("orb"))
strategy/obb.py         → OBBStrategy (@register("obb"))

execution/port.py       → BrokerPort ABC + execute_signal() Default-Impl
execution/paper_adapter.py  → In-Memory, kein Netzwerk, für Tests/Backtest
execution/alpaca_adapter.py → alpaca-py sync→async via run_in_executor
execution/ibkr_adapter.py   → ib_insync sync→async, Bracket via Parent/Child

data/providers/base.py         → DataProvider ABC
data/providers/alpaca_provider.py → Alpaca Historical + Polling-Stream
data/providers/yfinance_provider.py → yfinance für Backtest

backtest/engine.py   → BarByBarEngine (chronologische Bar-Iteration)
backtest/report.py   → build_tearsheet(), format_tearsheet()
backtest/slippage.py → SlippageModel, CommissionModel

live/runner.py    → LiveRunner (asyncio Event-Loop, Hauptschleife)
live/scheduler.py → TradingScheduler (APScheduler CronTrigger Mon-Fri ET)
live/state.py     → PersistentState (aiosqlite SQLite)
live/notifier.py  → TelegramNotifier (httpx, graceful degradation)
live/scanner.py   → PremarketScanner (Alpaca Snapshot-API)

configs/base.yaml → Shared Defaults (wird gemerged)
main.py           → CLI: live | paper | backtest
```

## Signal-Flow (Kurzfassung)

```
DataProvider.get_bars_bulk()
  → BarByBarEngine / LiveRunner
    → context.set_now/set_spy_df/update_account
    → strategy.on_bar(bar)
      → context.push_bar(bar)
      → _generate_signals(bar)  ← reine Logik
    → [Signal]
    → broker.execute_signal(signal, equity, risk_pct)
      → position_size() / fixed_fraction_size()
      → broker.submit_order(OrderRequest)
    → trade_manager.register(ManagedTrade)
    → context.set_open_symbols / reserve_group
```

## Zwei Sizing-Paradigmen

- **ORB**: R-basiert über `position_size(equity, risk_pct, entry, stop)`
  Gesteuert durch `qty_factor` aus MIT-Overlay (0.25–1.0)
- **OBB**: Fixed-Fraction über `fixed_fraction_size(equity, price, fraction)`
  Signal enthält `qty_hint` im metadata; `execute_signal` prüft das zuerst

## Strategie registrieren

```python
# Neue Strategie in strategy/my_strat.py:
from strategy.registry import register
from strategy.base import BaseStrategy

@register("my_strat")
class MyStrat(BaseStrategy):
    @property
    def name(self): return "my_strat"
    def _generate_signals(self, bar): ...

# In strategy/__init__.py importieren:
from strategy import my_strat  # noqa: F401
```

## Tests

```bash
pytest              # Alle Tests (kein Netzwerk nötig)
pytest tests/unit/  # Nur Unit-Tests
pytest -v -k "orb"  # Nur ORB-Tests
```

Fixtures in `tests/conftest.py`:
- `context` → frischer `MarketContextService` (setzt + resettet Singleton)
- `paper` → `PaperAdapter` mit 100k Cash, kein Slippage
- `ohlcv_5m` → 100 deterministische 5-Min-Bars (seed=42)
- `ohlcv_daily` → 200 Daily-Bars (seed=99)
- `spy_df` → 50-Bar SPY mit Aufwärtstrend
- `make_bar()` → einzelner Bar mit Default-Werten
- `make_ohlcv()` → parametrisierbarer OHLCV-DataFrame

## Häufige Erweiterungspunkte

| Aufgabe | Datei | Was tun |
|---|---|---|
| Neue Strategie | `strategy/my_strat.py` | `@register("name")` + `BaseStrategy` |
| Neuer Broker | `execution/my_broker.py` | `BrokerPort` erben, `submit_order` etc. implementieren |
| Neue Datenquelle | `data/providers/my_provider.py` | `DataProvider` erben |
| Neuer Filter | `core/filters.py` | Pure Funktion hinzufügen |
| Neuer Indikator | `core/indicators.py` | Pure Funktion hinzufügen |

## Pydantic v2 Konventionen

```python
# Richtig (v2):
class MyModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    field: str = Field(default="x")

# NICHT (v1-Stil):
class Config:
    extra = "allow"
```

## Logging

```python
from core.logging import get_logger
log = get_logger(__name__)

# Strukturiert (key=value Pairs):
log.info("event.name", symbol="AAPL", price=150.0)
log.warning("event.warn", error=str(e))
```

Niemals `print()` für Diagnose-Output. Immer `log.*`.

## Bekannte Gotchas

- **`asyncio.get_event_loop()`** in Tests kann deprecation-Warnings erzeugen
  → `asyncio.get_event_loop().run_until_complete(...)` nur in nicht-async Kontexten
- **yfinance MultiIndex**: `df.columns` kann ein `MultiIndex` sein → `_flatten_columns()` in `yfinance_provider.py`
- **IBKR Client-ID**: Bei Error 326 (Doppel-ID) wirft `IBKRAdapter.__init__` sofort RuntimeError
- **Alpaca Feed**: `iex`-Feed hat möglicherweise 15-Min-Delay → für Live-Trading `sip` nutzen
- **PaperAdapter Lock**: `asyncio.Lock()` verhindert Race Conditions, aber der Lock muss aus
  dem gleichen Event-Loop stammen → bei Test-Isolation `PaperAdapter()` pro Test neu erstellen

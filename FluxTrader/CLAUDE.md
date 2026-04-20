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
core/models.py          → Datenklassen (Bar, FeatureVector, BaseSignal, Signal, PairSignal, ...)
core/indicators.py      → Reine Indikatoren-Funktionen + KalmanSpreadEstimator
core/risk.py            → Position Sizing, Kelly, Stops, EV-Berechnung
core/filters.py         → Marktzeiten, Gap, Trend, MIT-Independence, VIX-Regime
core/ml_filter.py       → MLFilter (optionaler Konfidenz-Filter, Null-Object-Pattern)
core/context.py         → MarketContextService (DI-Container, Singleton)
core/trade_manager.py   → ManagedTrade, TradeManager (Exits/Trailing/EOD)
core/config.py          → AppConfig (Pydantic v2), load_config(), load_env()
core/logging.py         → setup_logging(), get_logger() via structlog

strategy/base.py        → BaseStrategy ABC + PairStrategy ABC
strategy/registry.py    → @register-Decorator, StrategyRegistry (Single + Pair)
strategy/orb.py         → ORBStrategy (@register("orb"))
strategy/obb.py         → OBBStrategy (@register("obb"))
strategy/botti.py       → BottiStrategy (@register("botti"))
strategy/botti_pair.py  → BottiPairStrategy (@register("botti_pair"))

execution/port.py       → BrokerPort ABC + execute_signal() + execute_pair_signal()
execution/paper_adapter.py  → In-Memory, kein Netzwerk, für Tests/Backtest
execution/alpaca_adapter.py → alpaca-py sync→async via run_in_executor
execution/ibkr_adapter.py   → ib_insync sync→async, Bracket via Parent/Child

data/providers/base.py         → DataProvider ABC
data/providers/alpaca_provider.py → Alpaca Historical + Polling-Stream
data/providers/yfinance_provider.py → yfinance für Backtest

backtest/engine.py   → BarByBarEngine (chronologische Bar-Iteration)
backtest/report.py   → build_tearsheet(), format_tearsheet()
backtest/slippage.py → SlippageModel, CommissionModel

live/runner.py      → LiveRunner (asyncio Event-Loop, Einzelsymbol-Strategien)
live/pair_runner.py → PairEngine (asyncio Task, Pair-Strategien)
live/scheduler.py   → TradingScheduler (APScheduler CronTrigger Mon-Fri ET)
live/state.py       → PersistentState (aiosqlite SQLite)
live/notifier.py    → TelegramNotifier (httpx, graceful degradation)
live/scanner.py     → PremarketScanner (Alpaca Snapshot-API)

tools/train_ml.py   → CLI: ML-Model-Training auf historischen Trades
tools/models/       → Gespeicherte model.pkl + scaler.pkl

configs/base.yaml              → Shared Defaults (wird gemerged)
configs/botti.yaml            → Botti Live/Paper (IBKR, Daily + MTF-Filter aktiv)
configs/botti_backtest_mtf.yaml  → Botti Backtest MIT MTF-Filter (yfinance, broker=paper)
configs/botti_backtest_nomtf.yaml → Botti Backtest OHNE MTF-Filter (Baseline)
configs/botti_pair.yaml       → Botti Pair-Trading (SPY/QQQ Kalman Z-Score)
main.py                       → CLI: live | paper | backtest
```

## Signal-Flow (Kurzfassung)

### Einzelsymbol (ORB, OBB, Botti)
```
DataProvider.get_bars_bulk()
  → BarByBarEngine / LiveRunner
    → context.set_now/set_spy_df/update_account
    → strategy.on_bar(bar)
      → context.push_bar(bar)
      → _generate_signals(bar)  ← reine Logik
        [Botti: _classify_signal() → BUY/BUY_MR/SELL]
        [Botti: BUY + use_multi_timeframe → _daily_mtf_proxy(df, cfg)]
        [Botti: BUY_MR → MTF-Filter übersprungen (v6-Konvention)]
    → [Signal] (mit FeatureVector)
    → ml_filter.passes(signal)   ← optionaler ML-Konfidenz-Filter
    → broker.execute_signal(signal, equity, risk_pct)
      → position_size() / fixed_fraction_size()
      → broker.submit_order(OrderRequest)
    → trade_manager.register(ManagedTrade)
    → context.set_open_symbols / reserve_group
```

### Pair-Trading (botti_pair)
```
PairEngine._fetch_bars(sym_a, sym_b)
  → strategy._generate_pair_signal(bar_a, bar_b, snapshot)
  → PairSignal (mit FeatureVector, z_score)
  → ml_filter.passes(signal)
  → broker.execute_pair_signal(signal, equity)
    → submit_order(long_leg) + submit_order(short_leg)
    → bei Failure: cancel(long_leg) → kein Leg-Mismatch
  → trade_manager.register(long_leg + short_leg)
```

## Signal-Hierarchie

```
BaseSignal (strategy, symbol, features: FeatureVector, timestamp)
  ├── Signal      (direction, strength, stop_price, ...) → Einzelsymbol
  └── PairSignal  (long_symbol, short_symbol, z_score, action, qty_pct) → Pair
```

`FeatureVector` ist einheitlich für ML-Filter: sma_diff, adx, atr_pct, rsi,
macd_hist, z_score (Pair: Spread Z-Score, Einzelsymbol: 0.0), volume_ratio.

## Drei Sizing-Paradigmen

- **ORB**: R-basiert über `position_size(equity, risk_pct, entry, stop)`
  Gesteuert durch `qty_factor` aus MIT-Overlay (0.25–1.0)
- **OBB**: Fixed-Fraction über `fixed_fraction_size(equity, price, fraction)`
  Signal enthält `qty_hint` im metadata; `execute_signal` prüft das zuerst
- **Botti**: R-basiert über `position_size()`, VIX-Faktor skaliert Größe bei hohem VIX
  `botti_trend`-Signal enthält `vix_factor` in metadata
- **Pair**: ATR-basiert über `execute_pair_signal(signal, equity)`
  Nutzt `qty_pct` und `atr_pct` aus PairSignal/FeatureVector

## Strategie registrieren

```python
# Neue Einzelsymbol-Strategie:
from strategy.registry import register
from strategy.base import BaseStrategy

@register("my_strat")
class MyStrat(BaseStrategy):
    @property
    def name(self): return "my_strat"
    def _generate_signals(self, bar): ...

# Neue Pair-Strategie:
from strategy.base import PairStrategy

@register("my_pair")
class MyPair(PairStrategy):
    @property
    def name(self): return "my_pair"
    @property
    def symbol_a(self): return "SPY"
    @property
    def symbol_b(self): return "QQQ"
    def _generate_pair_signal(self, bar_a, bar_b, snapshot): ...

# In strategy/__init__.py importieren:
from strategy import my_strat  # noqa: F401
```

## Tests

```bash
pytest              # Alle Tests (kein Netzwerk nötig)
pytest tests/unit/  # Nur Unit-Tests
pytest -v -k "orb"  # Nur ORB-Tests
pytest -v -k "pair or ml_filter"  # Pair-Trading + ML-Filter
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
| Neue Einzelsymbol-Strategie | `strategy/my_strat.py` | `@register("name")` + `BaseStrategy` |
| Neue Pair-Strategie | `strategy/my_pair.py` | `@register("name")` + `PairStrategy` |
| Neuer Broker | `execution/my_broker.py` | `BrokerPort` erben, `submit_order` etc. implementieren |
| Neue Datenquelle | `data/providers/my_provider.py` | `DataProvider` erben |
| Neuer Filter | `core/filters.py` | Pure Funktion hinzufügen |
| Neuer Indikator | `core/indicators.py` | Pure Funktion hinzufügen |
| ML-Modell trainieren | `tools/train_ml.py` | `python tools/train_ml.py --history DB --output tools/models/` |

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
- **Botti Daily-Backtest via IBKR**: IBKR-Gateway muss laufen; für reine Backtests
  `botti_backtest_mtf.yaml` / `botti_backtest_nomtf.yaml` mit `provider: yfinance` nutzen
- **Botti RSI im MTF-Proxy**: Monoton steigende Bars (kein Down-Move) → `RSI = NaN`
  → `_daily_mtf_proxy` gibt `False` zurück. In Tests Sinus-überlagerte Bars nutzen, nicht linear steigende.
- **Botti MTF-Filter gilt nur für `botti_trend`**, nicht für `botti_mr` (Mean Reversion).
  Das ist bewusste v6-Konvention: MR-Entries brauchen keinen Intraday-Bestätigungs-Punkt.
- **Windows-Terminal & Tearsheet**: `PYTHONIOENCODING=utf-8` setzen, sonst `UnicodeEncodeError`
  bei Unicode-Pfeilen im Tearsheet-Output.
